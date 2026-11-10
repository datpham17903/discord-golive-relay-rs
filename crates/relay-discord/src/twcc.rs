//! Transport-wide congestion control feedback (RTCP RTPFB, PT 205, FMT 15). Port of
//! `adapter/twcc.py`.
//!
//! draft-holmer-rmcat-transport-wide-cc-extensions-01. The SFU stamps every packet it sends us with a
//! 16-bit transport-wide sequence number (RTP header extension [`TWCC_EXT_ID`]); reporting their arrival
//! times keeps it from throttling the sender's encoder. [`TwccFeedback::record`] runs on the UDP
//! dispatcher thread, [`TwccFeedback::build`] on the 50 ms feedback tick ([`TwccSender`]).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use relay_voice::receive::RtpExtension;

use crate::ingress::round_half_even;

pub const MAX_PACKETS_PER_FEEDBACK: usize = 8000;
/// Unreported arrivals kept; the oldest are dropped beyond this.
pub const MAX_PENDING: usize = 20000;
/// RTP header extension id of the transport-wide sequence number.
pub const TWCC_EXT_ID: u8 = 5;
pub const TWCC_INTERVAL_S: f64 = 0.05;
/// Receive delta resolution: 250 us.
const UNIT_S: f64 = 0.00025;
/// Reference time resolution: 64 ms.
const REF_UNIT_S: f64 = 0.064;

#[derive(Default)]
struct State {
    /// Unwrapped transport seq -> arrival time (s).
    arrivals: BTreeMap<i64, f64>,
    /// Highest unwrapped seq seen.
    last: Option<i64>,
    /// First seq the next feedback reports.
    next_base: Option<i64>,
    /// Arrival time the reference time counts from.
    origin: Option<f64>,
    fb_count: u32,
}

#[derive(Default)]
pub struct TwccFeedback {
    state: Mutex<State>,
}

/// The transport-wide sequence number carried by an RTP extension list, if any (the first
/// [`TWCC_EXT_ID`] entry with at least two bytes).
pub fn transport_seq(extensions: &[RtpExtension]) -> Option<u16> {
    extensions
        .iter()
        .find(|e| e.id == TWCC_EXT_ID && e.data.len() >= 2)
        .map(|e| u16::from_be_bytes([e.data[0], e.data[1]]))
}

impl State {
    fn unwrap_seq(&mut self, seq: u16) -> i64 {
        let seq = i64::from(seq);
        let Some(last) = self.last else {
            self.last = Some(seq);
            return seq;
        };
        let mut d = (seq - (last & 0xFFFF)) & 0xFFFF;
        if d >= 0x8000 {
            d -= 0x10000;
        }
        let v = last + d;
        if v > last {
            self.last = Some(v);
        }
        v
    }
}

impl TwccFeedback {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, seq16: u16, arrival_s: f64) {
        let mut st = self.state.lock();
        let seq = st.unwrap_seq(seq16);
        if st.origin.is_none() {
            st.origin = Some(arrival_s);
        }
        st.arrivals.insert(seq, arrival_s);
        while st.arrivals.len() > MAX_PENDING {
            st.arrivals.pop_first();
        }
    }

    /// Number of arrivals not reported yet.
    pub fn pending(&self) -> usize {
        self.state.lock().arrivals.len()
    }

    /// Reports everything recorded since the last call as `(rtcp_header, rtcp_body)`; `None` when
    /// nothing new.
    pub fn build(&self, sender_ssrc: u32, media_ssrc: u32) -> Option<(Vec<u8>, Vec<u8>)> {
        let (base, seqs, got, origin, fb_count) = {
            let mut st = self.state.lock();
            let origin = st.origin?;
            let (&lowest, &highest) = (
                st.arrivals.first_key_value()?.0,
                st.arrivals.last_key_value()?.0,
            );
            let base = st.next_base.unwrap_or(lowest);
            if highest < base {
                st.arrivals.clear();
                return None;
            }
            let end = highest.min(base + MAX_PACKETS_PER_FEEDBACK as i64 - 1);
            let mut got: BTreeMap<i64, f64> = BTreeMap::new();
            for s in base..=end {
                if let Some(at) = st.arrivals.remove(&s) {
                    got.insert(s, at);
                }
            }
            // late arrivals of already-reported numbers
            st.arrivals = st.arrivals.split_off(&base);
            st.next_base = Some(end + 1);
            if got.is_empty() {
                return None;
            }
            let fb_count = st.fb_count;
            st.fb_count += 1;
            (base, base..=end, got, origin, fb_count)
        };

        // The reference time is that of the lowest sequence number that arrived, like `min(got)`.
        let first_arrival = *got.values().next().expect("got is not empty");
        let reference = ((first_arrival - origin) / REF_UNIT_S).floor() as i64;
        let mut prev = origin + reference as f64 * REF_UNIT_S;
        let mut symbols: Vec<u8> = Vec::new();
        let mut deltas: Vec<u8> = Vec::new();
        for s in seqs {
            let Some(&at) = got.get(&s) else {
                symbols.push(0);
                continue;
            };
            let d = round_half_even((at - prev) / UNIT_S);
            prev = at;
            if (0..=255).contains(&d) {
                symbols.push(1);
                deltas.push(d as u8);
            } else {
                symbols.push(2);
                deltas.extend_from_slice(&(d.clamp(-32768, 32767) as i16).to_be_bytes());
            }
        }
        let mut chunks = Vec::with_capacity(symbols.len().div_ceil(7) * 2);
        for part in symbols.chunks(7) {
            let mut v: u16 = 0xC000; // two-bit status vector chunk
            for (j, &sym) in part.iter().enumerate() {
                v |= u16::from(sym) << (12 - 2 * j);
            }
            chunks.extend_from_slice(&v.to_be_bytes());
        }
        let mut body = Vec::with_capacity(12 + chunks.len() + deltas.len() + 4);
        body.extend_from_slice(&media_ssrc.to_be_bytes());
        body.extend_from_slice(&((base & 0xFFFF) as u16).to_be_bytes());
        body.extend_from_slice(&(symbols.len() as u16).to_be_bytes());
        let ref_fb = (((reference & 0xFF_FFFF) as u32) << 8) | (fb_count & 0xFF);
        body.extend_from_slice(&ref_fb.to_be_bytes());
        body.extend_from_slice(&chunks);
        body.extend_from_slice(&deltas);
        let pad = (4 - (8 + body.len()) % 4) % 4;
        if pad > 0 {
            body.extend(std::iter::repeat_n(0u8, pad - 1));
            body.push(pad as u8);
        }
        let mut header = Vec::with_capacity(8);
        header.push(0x80 | if pad > 0 { 0x20 } else { 0 } | 15);
        header.push(205);
        header.extend_from_slice(&(((8 + body.len()) / 4 - 1) as u16).to_be_bytes());
        header.extend_from_slice(&sender_ssrc.to_be_bytes());
        Some((header, body))
    }
}

/// The feedback tick of one adapter: counts what it sent and reports a failure once per adapter, not
/// once per 50 ms tick (`_twcc_sent`, `_twcc_failed`).
#[derive(Default)]
pub struct TwccSender {
    sent: AtomicU64,
    failed: AtomicBool,
}

impl TwccSender {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    /// A new watch starts counting again (`watch` resets `_twcc_sent`).
    pub fn reset_sent(&self) {
        self.sent.store(0, Ordering::Relaxed);
    }

    /// One tick: nothing happens until the video SSRC is known. Returns the error text of the first
    /// failure only (`what` names the step), `None` otherwise.
    pub fn tick(
        &self,
        twcc: &TwccFeedback,
        own_ssrc: Result<u32, String>,
        media_ssrc: Option<u32>,
        send: impl FnOnce(&[u8], &[u8]) -> Result<(), String>,
    ) -> Option<(&'static str, String)> {
        let media_ssrc = media_ssrc?;
        let fail = |what: &'static str, error: String| {
            (!self.failed.swap(true, Ordering::Relaxed)).then_some((what, error))
        };
        let own_ssrc = match own_ssrc {
            Ok(ssrc) => ssrc,
            Err(error) => return fail("send", error),
        };
        let (header, body) = twcc.build(own_ssrc, media_ssrc)?;
        match send(&header, &body) {
            Ok(()) => {
                self.sent.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(error) => fail("send", error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Parsed {
        sender: u32,
        media: u32,
        base: u16,
        count: u16,
        reference: u32,
        fb: u32,
        syms: Vec<u8>,
        deltas: Vec<i32>,
    }

    fn parse(fb: &(Vec<u8>, Vec<u8>)) -> Parsed {
        let (header, body) = fb;
        let b0 = header[0];
        assert_eq!(b0 >> 6, 2);
        assert_eq!(b0 & 0x1F, 15);
        assert_eq!(header[1], 205);
        let length = u16::from_be_bytes([header[2], header[3]]) as usize;
        assert_eq!(header.len() + body.len(), (length + 1) * 4);
        let sender = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let mut body = body.as_slice();
        let padded = b0 & 0x20 != 0;
        if padded {
            let pad = usize::from(*body.last().unwrap());
            assert!((1..=3).contains(&pad));
            assert!(
                body[body.len() - pad..body.len() - 1]
                    .iter()
                    .all(|b| *b == 0)
            );
            body = &body[..body.len() - pad];
        }
        let media = u32::from_be_bytes(body[0..4].try_into().unwrap());
        let base = u16::from_be_bytes(body[4..6].try_into().unwrap());
        let count = u16::from_be_bytes(body[6..8].try_into().unwrap());
        let ref_fb = u32::from_be_bytes(body[8..12].try_into().unwrap());
        let mut syms = Vec::new();
        let mut pos = 12;
        while syms.len() < usize::from(count) {
            let chunk = u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap());
            pos += 2;
            assert_eq!(chunk >> 14, 3);
            syms.extend((0..7).map(|j| ((chunk >> (12 - 2 * j)) & 3) as u8));
        }
        assert!(syms[usize::from(count)..].iter().all(|s| *s == 0));
        syms.truncate(usize::from(count));
        let mut deltas = Vec::new();
        for &s in &syms {
            match s {
                1 => {
                    deltas.push(i32::from(body[pos]));
                    pos += 1;
                }
                2 => {
                    deltas.push(i32::from(i16::from_be_bytes(
                        body[pos..pos + 2].try_into().unwrap(),
                    )));
                    pos += 2;
                }
                _ => {}
            }
        }
        assert_eq!(pos, body.len());
        Parsed {
            sender,
            media,
            base,
            count,
            reference: ref_fb >> 8,
            fb: ref_fb & 0xFF,
            syms,
            deltas,
        }
    }

    #[test]
    fn nothing_recorded_builds_nothing() {
        assert!(TwccFeedback::new().build(1, 2).is_none());
    }

    #[test]
    fn basic_report() {
        let t = TwccFeedback::new();
        for i in 0..5u16 {
            t.record(100 + i, 10.0 + f64::from(i) * 0.001);
        }
        let p = parse(&t.build(7, 9).unwrap());
        assert_eq!((p.sender, p.media, p.base, p.count), (7, 9, 100, 5));
        assert_eq!(p.syms, vec![1; 5]);
        assert_eq!(p.deltas, vec![0, 4, 4, 4, 4]);
        assert_eq!((p.reference, p.fb), (0, 0));
        assert!(t.build(7, 9).is_none(), "already reported");
    }

    #[test]
    fn missing_packets_and_feedback_count() {
        let t = TwccFeedback::new();
        t.record(10, 1.0);
        t.record(13, 1.002);
        let p = parse(&t.build(1, 2).unwrap());
        assert_eq!((p.base, p.count), (10, 4));
        assert_eq!(p.syms, vec![1, 0, 0, 1]);
        assert_eq!(p.deltas, vec![0, 8]);
        t.record(14, 1.003);
        let p = parse(&t.build(1, 2).unwrap());
        assert_eq!((p.base, p.count, p.fb), (14, 1, 1));
    }

    #[test]
    fn wrap_at_16_bits() {
        let t = TwccFeedback::new();
        for (s, at) in [(0xFFFE, 1.0), (0xFFFF, 1.001), (0, 1.002), (1, 1.003)] {
            t.record(s, at);
        }
        let p = parse(&t.build(1, 2).unwrap());
        assert_eq!((p.base, p.count), (0xFFFE, 4));
        assert_eq!(p.syms, vec![1; 4]);
    }

    #[test]
    fn large_and_negative_deltas_use_two_bytes() {
        let t = TwccFeedback::new();
        t.record(1, 1.0);
        t.record(2, 1.5); // +500 ms = 2000 units
        t.record(3, 1.7); // +200 ms = 800 units
        t.record(4, 1.65); // reordered: -50 ms = -200 units
        let p = parse(&t.build(1, 2).unwrap());
        assert_eq!(p.syms, vec![1, 2, 2, 2]);
        assert_eq!(p.deltas[1..], [2000, 800, -200]);
        let mut at = 1.0 + f64::from(p.reference) * 0.064;
        let mut out = Vec::new();
        for d in &p.deltas {
            at += f64::from(*d) * 0.00025;
            out.push(at);
        }
        for (got, want) in out.iter().zip([1.0, 1.5, 1.7, 1.65]) {
            assert!((got - want).abs() < 0.0006, "{got} vs {want}");
        }
    }

    #[test]
    fn padding_and_length() {
        for n in 1..12u16 {
            let t = TwccFeedback::new();
            for i in 0..n {
                t.record(i, 5.0 + f64::from(i) * 0.001);
            }
            let fb = t.build(1, 2).unwrap();
            assert_eq!((fb.0.len() + fb.1.len()) % 4, 0);
            assert_eq!(parse(&fb).count, n);
        }
    }

    #[test]
    fn reference_time_units() {
        let t = TwccFeedback::new();
        t.record(1, 100.0);
        t.record(2, 100.0 + 0.064 * 3.0 + 0.01);
        t.build(1, 2);
        t.record(3, 100.0 + 0.064 * 3.0 + 0.02);
        let p = parse(&t.build(1, 2).unwrap());
        assert_eq!(p.reference, 3);
    }

    #[test]
    fn caps_single_feedback_and_bounds_memory() {
        let t = TwccFeedback::new();
        for i in 0..(MAX_PACKETS_PER_FEEDBACK + 10) {
            t.record(i as u16, 1.0 + i as f64 * 0.0001);
        }
        let p = parse(&t.build(1, 2).unwrap());
        assert_eq!(usize::from(p.count), MAX_PACKETS_PER_FEEDBACK);
        let p = parse(&t.build(1, 2).unwrap());
        assert_eq!(usize::from(p.base), MAX_PACKETS_PER_FEEDBACK & 0xFFFF);
        assert_eq!(p.count, 10);
        let big = TwccFeedback::new();
        for i in 0..30000usize {
            big.record((i & 0xFFFF) as u16, 1.0);
        }
        assert!(big.pending() <= MAX_PENDING);
    }

    #[test]
    fn late_arrival_of_reported_number_is_dropped() {
        let t = TwccFeedback::new();
        t.record(5, 1.0);
        t.build(1, 2);
        t.record(3, 1.1);
        assert!(t.build(1, 2).is_none());
        assert_eq!(t.pending(), 0);
    }

    #[test]
    fn the_extension_carries_the_transport_sequence_number() {
        let ext = |id, data: &[u8]| RtpExtension {
            id,
            data: data.to_vec(),
        };
        assert_eq!(transport_seq(&[]), None);
        assert_eq!(
            transport_seq(&[ext(1, &[9, 9]), ext(5, &[0x01, 0x02, 0x03])]),
            Some(0x0102)
        );
        assert_eq!(
            transport_seq(&[ext(5, &[1]), ext(5, &[0, 7])]),
            Some(7),
            "too short entries are skipped"
        );
    }

    #[test]
    fn the_tick_waits_for_the_video_ssrc_and_reports_a_failure_once() {
        let t = TwccFeedback::new();
        let sender = TwccSender::new();
        let sent = std::cell::RefCell::new(Vec::new());
        t.record(1, 1.0);
        assert!(
            sender
                .tick(&t, Ok(11), None, |h, b| {
                    sent.borrow_mut().push([h, b].concat());
                    Ok(())
                })
                .is_none()
        );
        assert!(sent.borrow().is_empty(), "video SSRC unknown yet");

        assert!(
            sender
                .tick(&t, Ok(11), Some(22), |h, b| {
                    sent.borrow_mut().push([h, b].concat());
                    Ok(())
                })
                .is_none()
        );
        assert_eq!(sent.borrow().len(), 1);
        assert_eq!(sender.sent(), 1);
        let p = parse(&(
            sent.borrow()[0][..8].to_vec(),
            sent.borrow()[0][8..].to_vec(),
        ));
        assert_eq!((p.sender, p.media), (11, 22));

        let mut failures = Vec::new();
        for i in 0..3u16 {
            t.record(2 + i, 1.1 + f64::from(i));
            failures.extend(sender.tick(&t, Ok(11), Some(22), |_, _| Err("boom".into())));
        }
        assert_eq!(failures, vec![("send", "boom".to_owned())], "logged once");
        assert_eq!(sender.sent(), 1);
        sender.reset_sent();
        assert_eq!(sender.sent(), 0);
    }
}
