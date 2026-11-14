//! `relayctl dev relay`: live probe for gate V-RELAY, all in one process.
//!
//! The relay account (`--credential`) logs in, joins the allowed voice channel, watches the target's
//! Go Live exactly like `dev watch`, feeds what it receives through the passthrough worker and the
//! publisher (as the service will) and publishes it as its own Go Live in the same channel. A second,
//! viewer account (`--viewer-credential`) joins the channel, waits for the relay's stream and watches
//! it. The exit code says whether the viewer saw frames at a usable rate. Never run against anything
//! but a live test setup: this connects to Discord.
//!
//! Output: JSON log lines on stderr (`role` = `relay` | `viewer`, every token redacted) and one
//! summary object on stdout.

use std::future::Future;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use parking_lot::Mutex;
use relay_core::clock::{monotonic, wall_clock};
use relay_core::config::{CodecMode, RelayConfig, load_config};
use relay_core::ingress::{
    GapTracker, IntervalGate, OVERFLOW_MIN_INTERVAL_S, PLI_MIN_INTERVAL_S, PliLimiter, rtcp_to_mono,
};
use relay_core::publisher::{Publisher, PublisherSource};
use relay_core::supervisor::codec::{CodecAction, CodecController, CodecEvent, OutputId};
use relay_core::worker::messages::{
    Configure, Egress, ForceIdr, Ingress, OutputProfile, ResetPipeline, WorkerEvent,
    WorkerEventKind,
};
use relay_core::worker::process::WorkerHandle;
use relay_discord::codecs::{output_settings, parse_names};
use relay_discord::ingress::{ingress_audio, ingress_video};
use relay_voice::ClientEvent;
use relay_voice::StreamClient;
use relay_voice::VoiceClient;
use relay_voice::publish::{SendStats, VideoParams};
use relay_voice::receive::{MediaType, Packet};
use relay_voice::voice_ws::video_codec_list;
use serde_json::{Value, json};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use super::RelayArgs;
use super::forward::{RelayReport, combined_summary, exit_code, passthrough_mismatch, viewer_json};
use super::session::{
    PacketTap, Probe, STATS_INTERVAL_S, TEARDOWN_STEP, dave_view, log_dave_change, watch_loop,
};
use super::stats::{Progress, WatchAccount, summary_json};
use crate::tokens::load_token;

const GENERATION: u64 = 1;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const CODEC_TICK_S: f64 = 0.1;
const MAX_EGRESS_POLL: usize = 16;
/// `CONNECT_TIMEOUT_S` of the Python adapter: creating the output Go Live.
const OUTPUT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long the relay waits for the target to go live.
const TARGET_WAIT: Duration = Duration::from_secs(300);
/// How long the viewer waits for the relay's own Go Live to appear.
const VIEWER_WAIT: Duration = Duration::from_secs(90);
/// `publisher.py` default `max_late_s`.
const MAX_LATE_S: f64 = 0.2;

#[derive(Default)]
struct Shared {
    canvas: Option<(u32, u32)>,
    /// Size of the source while it is passed through (the worker's `source_codec` event).
    src_dims: Option<(u32, u32)>,
    source_codec: Option<String>,
    negotiated: Option<String>,
    mismatch_logged: Option<(String, String)>,
    send_stats: SendStats,
}

/// The relay account: watch session plus everything between it and the output Go Live.
struct Relay {
    probe: Arc<Probe>,
    profile: OutputProfile,
    first_media_timeout: Duration,
    publisher: Arc<Publisher>,
    worker: Arc<Mutex<WorkerHandle>>,
    epoch: Arc<AtomicU64>,
    ctl: Mutex<CodecController>,
    shared: Mutex<Shared>,
    watch: Mutex<Option<StreamClient>>,
    out: Mutex<Option<StreamClient>>,
    pli: Mutex<PliLimiter>,
    gaps: Mutex<GapTracker>,
    last_overflow: Mutex<f64>,
    pli_from_viewers: AtomicU64,
    /// Set once the output is being torn down: no more packets go to the worker.
    closed: AtomicBool,
    drive: Mutex<Option<JoinHandle<()>>>,
}

fn upper(name: &str) -> String {
    name.to_ascii_uppercase()
}

impl Relay {
    /// Starts the worker thread (profile from the config, adaptive codec mode) and the publisher.
    fn start(probe: Arc<Probe>, config: &RelayConfig) -> Result<Arc<Self>, String> {
        let mut profile: OutputProfile = config.output_profile();
        profile.codec_mode = CodecMode::Adaptive;
        let profile_view = profile.clone();
        let worker = WorkerHandle::start(profile.clone()).map_err(|e| format!("worker: {e}"))?;
        let worker = Arc::new(Mutex::new(worker));
        let epoch = Arc::new(AtomicU64::new(1));
        let configured = worker.lock().send(
            Configure {
                generation: GENERATION,
                epoch: 1,
                profile,
            }
            .into(),
        );
        if !configured {
            return Err("worker did not accept its configuration".into());
        }

        let idr_gate = Mutex::new(IntervalGate::new(
            config.runtime.feedback_idr_min_interval_ms as f64 / 1000.0,
        ));
        let need_idr = {
            let (worker, epoch) = (Arc::clone(&worker), Arc::clone(&epoch));
            Box::new(move || {
                if idr_gate.lock().allow(monotonic()) {
                    worker.lock().send(
                        ForceIdr {
                            generation: GENERATION,
                            epoch: epoch.load(Ordering::Relaxed),
                        }
                        .into(),
                    );
                }
            })
        };
        let publisher = Arc::new(Publisher::new(
            Box::new(|_, _, _| true),
            Box::new(monotonic),
            MAX_LATE_S,
            need_idr,
            "H264",
        ));
        Ok(Arc::new(Self {
            probe,
            profile: profile_view,
            first_media_timeout: Duration::from_secs_f64(
                config.runtime.first_media_timeout_seconds,
            ),
            publisher,
            worker,
            epoch,
            ctl: Mutex::new(CodecController::new()),
            shared: Mutex::new(Shared::default()),
            watch: Mutex::new(None),
            out: Mutex::new(None),
            pli: Mutex::new(PliLimiter::new(PLI_MIN_INTERVAL_S)),
            gaps: Mutex::new(GapTracker::default()),
            last_overflow: Mutex::new(f64::NEG_INFINITY),
            pli_from_viewers: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            drive: Mutex::new(None),
        }))
    }

    fn log(&self, event: &str, fields: Value) {
        self.probe.log(event, fields);
    }

    // ------------------------------------------------------------- ingress

    /// UDP dispatcher thread: forward one packet of the target to the worker.
    fn ingress(&self, packet: &Packet) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        let mono_now = monotonic();
        let received = mono_now - packet.received_at.elapsed().as_secs_f64();
        let rtcp = rtcp_to_mono(packet.rtcp_time, wall_clock(), mono_now);
        let epoch = self.epoch.load(Ordering::Relaxed);
        let msg: Ingress = match packet.media_type {
            MediaType::Video => {
                let Some(codec) = packet.codec.video() else {
                    return;
                };
                let gap = {
                    let mut gaps = self.gaps.lock();
                    if gaps.payload_type_switched(packet.ssrc, codec, packet.payload_type)
                        && let Some(receiver) = self
                            .watch
                            .lock()
                            .as_ref()
                            .and_then(|stream| stream.receiver().ok())
                    {
                        receiver.forget_other_payload_types(packet.ssrc, packet.payload_type);
                    }
                    gaps.gap(
                        packet.ssrc,
                        codec,
                        packet.sequence,
                        packet.rtp_packets.iter().map(|p| p.sequence),
                    )
                };
                ingress_video(packet, (GENERATION, epoch), received, rtcp, gap).into()
            }
            MediaType::Audio => ingress_audio(packet, (GENERATION, epoch), received, rtcp).into(),
        };
        if !self.worker.lock().send(msg) {
            self.ingress_overflow(epoch);
        }
    }

    /// The bounded ingress is full: a new epoch drops the buffered backlog.
    fn ingress_overflow(&self, epoch: u64) {
        let now = monotonic();
        {
            let mut last = self.last_overflow.lock();
            if epoch != self.epoch.load(Ordering::Relaxed) || now - *last < OVERFLOW_MIN_INTERVAL_S
            {
                return;
            }
            *last = now;
        }
        let next = epoch + 1;
        self.worker.lock().send(
            ResetPipeline {
                generation: GENERATION,
                epoch: next,
            }
            .into(),
        );
        self.epoch.store(next, Ordering::Relaxed);
        self.publisher.set_source(GENERATION, next);
        self.log("ingress_overflow", json!({"epoch": next}));
    }

    // -------------------------------------------------------------- keyframes

    /// `service._request_pli`: a PLI to the target, rate limited, one trailing PLI deferred.
    fn request_pli(&self, from: &str) {
        if self.pli.lock().request(monotonic()) {
            self.send_pli(from);
        }
    }

    fn send_pli(&self, from: &str) {
        let Some(stream) = self.watch.lock().clone() else {
            return;
        };
        let Some(ssrc) = self.probe.account.lock().video_ssrc() else {
            return;
        };
        let ok = stream.send_pli(ssrc).is_ok();
        self.log("pli_request", json!({"ssrc": ssrc, "ok": ok, "from": from}));
    }

    /// A viewer of our Go Live asked for a keyframe (UDP dispatcher thread).
    fn viewer_pli(&self) {
        let n = self.pli_from_viewers.fetch_add(1, Ordering::Relaxed) + 1;
        self.log("viewer_pli", json!({"count": n}));
        self.request_pli("viewer");
    }

    // ---------------------------------------------------------------- worker

    fn pump_worker(&self) {
        let (egress, events) = {
            let worker = self.worker.lock();
            (worker.poll_egress(MAX_EGRESS_POLL), worker.poll_events())
        };
        for item in egress {
            match item {
                Egress::Video(au) => self.publisher.offer_video(au),
                Egress::Audio(au) => self.publisher.offer_audio(au),
            }
        }
        for event in events {
            self.on_worker_event(event);
        }
    }

    fn on_worker_event(&self, event: WorkerEvent) {
        if event.generation != GENERATION {
            return;
        }
        let data = event.data;
        let size = |data: &serde_json::Map<String, Value>| {
            Some((
                u32::try_from(data.get("width")?.as_u64()?).ok()?,
                u32::try_from(data.get("height")?.as_u64()?).ok()?,
            ))
        };
        match event.kind {
            WorkerEventKind::FirstFrame => {
                self.shared.lock().canvas = size(&data);
                self.log("worker_first_frame", Value::Object(data));
                self.probe.notify.notify_waiters();
            }
            WorkerEventKind::SourceCodec => {
                let codec = data
                    .get("codec")
                    .and_then(Value::as_str)
                    .map(upper)
                    .unwrap_or_default();
                let eligible = data
                    .get("eligible")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                {
                    let mut shared = self.shared.lock();
                    shared.src_dims = if eligible { size(&data) } else { None };
                    shared.source_codec = Some(codec.clone());
                }
                self.log("source_codec", Value::Object(data));
                self.codec_event(CodecEvent::SourceCodec { codec, eligible });
            }
            WorkerEventKind::NeedKeyframe => self.request_pli("worker"),
            WorkerEventKind::SourceIneligible => self.probe.error(format!(
                "source not eligible for passthrough: {}",
                Value::Object(data)
            )),
            WorkerEventKind::Fatal => self
                .probe
                .error(format!("worker fatal: {}", Value::Object(data))),
            WorkerEventKind::Stats => self.log("worker_stats", Value::Object(data)),
            WorkerEventKind::DecodeError | WorkerEventKind::Overflow => {
                self.log(
                    &format!("worker_{}", event.kind.as_str()),
                    Value::Object(data),
                );
            }
            WorkerEventKind::FreshFrame | WorkerEventKind::KeyWait | WorkerEventKind::Keyframe => {}
        }
    }

    // ------------------------------------------------------- adaptive codec

    /// Feeds the controller and runs what it asks; each action's outcome is fed back.
    fn codec_event(&self, event: CodecEvent) {
        let mut queue = vec![event];
        while let Some(event) = queue.pop() {
            let actions = {
                let mut ctl = self.ctl.lock();
                let before = ctl.effective().map(str::to_owned);
                let actions = match ctl.handle(event, monotonic()) {
                    Ok(actions) => actions,
                    Err(e) => {
                        drop(ctl);
                        self.probe.error(format!("codec controller: {e}"));
                        return;
                    }
                };
                let after = ctl.effective().map(str::to_owned);
                if before != after {
                    self.log("codec_change", json!({"old": before, "new": after}));
                }
                actions
            };
            for action in actions {
                queue.extend(self.run_action(action));
            }
        }
    }

    fn run_action(&self, action: CodecAction) -> Vec<CodecEvent> {
        let out = self.out.lock().clone();
        let result = match &action {
            CodecAction::UpdateWatch { codecs } => {
                let stream = self.watch.lock().clone();
                match parse_names(codecs) {
                    Ok(names) => stream.map(|s| s.session_update(&video_codec_list(&names, false))),
                    Err(_) => None,
                }
            }
            CodecAction::UpdateOutput { codecs, .. } => match parse_names(codecs) {
                Ok(names) => out.map(|s| s.session_update(&video_codec_list(&names, true))),
                Err(_) => None,
            },
            CodecAction::SwapOutput { codec, .. } => {
                return self.swap_output(out.as_ref(), codec);
            }
        };
        let ok = matches!(result, Some(Ok(())));
        self.log(
            "codec_action",
            json!({"action": format!("{action:?}"), "ok": ok}),
        );
        if ok {
            Vec::new()
        } else {
            vec![CodecEvent::ActionFailed(action)]
        }
    }

    /// `service._codec_swap`: rebind the output to the codec negotiated now, in place.
    fn swap_output(&self, out: Option<&StreamClient>, wanted: &str) -> Vec<CodecEvent> {
        let output_id = OutputId::Ingest;
        let failed = || {
            vec![CodecEvent::OutputBound {
                output_id,
                codec: None,
            }]
        };
        let Some(out) = out else {
            return failed();
        };
        let previous = self.publisher.codec();
        let publisher = Arc::clone(&self.publisher);
        let swapped = out.swap_output_source(move |codec| {
            publisher.set_codec(codec.as_str());
            Box::new(PublisherSource(publisher))
        });
        match swapped {
            Ok(Some(bound)) => {
                let bound = bound.as_str().to_owned();
                self.log("codec_swap", json!({"codec": wanted, "bound": bound}));
                let mut events = vec![CodecEvent::OutputBound {
                    output_id,
                    codec: Some(bound.clone()),
                }];
                if bound != wanted {
                    // the server renegotiated while we swapped
                    events.push(CodecEvent::OutputCodec {
                        output_id,
                        codec: bound,
                    });
                }
                events
            }
            Ok(None) | Err(_) => {
                self.log(
                    "codec_swap",
                    json!({"codec": wanted, "bound": null, "error": swapped.err().map(|e| e.to_string())}),
                );
                if self.publisher.codec() != previous {
                    self.publisher.set_codec(&previous);
                }
                failed()
            }
        }
    }

    /// Op 14 of the output stream: the server renegotiated its codec.
    fn on_session_update(&self, stream_key: Option<&str>, video_codec: Option<&str>) {
        let (Some(key), Some(codec)) = (stream_key, video_codec) else {
            return;
        };
        let is_output = self.out.lock().as_ref().is_some_and(|s| s.key() == key);
        if !is_output {
            return;
        }
        let codec = upper(codec);
        self.shared.lock().negotiated = Some(codec.clone());
        self.codec_event(CodecEvent::OutputCodec {
            output_id: OutputId::Ingest,
            codec,
        });
    }

    fn check_codec_match(&self) {
        let negotiated = self
            .out
            .lock()
            .as_ref()
            .and_then(|s| s.negotiated_video_codec().ok().flatten())
            .map(|c| c.as_str().to_owned());
        let mut shared = self.shared.lock();
        if negotiated.is_some() {
            shared.negotiated.clone_from(&negotiated);
        }
        let pair = shared.negotiated.clone().zip(shared.source_codec.clone());
        match pair {
            Some((n, s)) if n != s => {
                if shared.mismatch_logged.as_ref() != Some(&(n.clone(), s.clone())) {
                    shared.mismatch_logged = Some((n.clone(), s.clone()));
                    drop(shared);
                    self.log(
                        "codec_mismatch",
                        json!({"output_codec": n, "source_codec": s}),
                    );
                }
            }
            _ => shared.mismatch_logged = None,
        }
    }

    // ---------------------------------------------------------------- output

    /// Waits for the worker to lock the canvas, then creates and starts the output Go Live
    /// (`service._create_output`, adaptive path).
    async fn setup_output(
        self: &Arc<Self>,
        voice: &VoiceClient,
        user_agent: &str,
    ) -> Result<(), String> {
        let got_frame = self
            .probe
            .wait_until(self.first_media_timeout, || {
                self.shared.lock().canvas.is_some()
            })
            .await;
        if !got_frame {
            return Err(format!(
                "no first frame from the worker within {} s",
                self.first_media_timeout.as_secs()
            ));
        }
        let (dims, source_codec) = {
            let shared = self.shared.lock();
            (
                shared.src_dims.or(shared.canvas),
                shared.source_codec.clone(),
            )
        };
        let (width, height) = dims.ok_or("canvas not locked")?;
        let fps = u32::try_from(self.profile.fps).unwrap_or(30);
        let bitrate =
            u32::try_from(self.profile.video_bitrate_kbps.saturating_mul(1000)).unwrap_or(0);

        let settings = output_settings(true, fps, false, user_agent);
        let out = voice
            .create_stream(settings, OUTPUT_TIMEOUT)
            .await
            .map_err(|e| format!("create output stream: {e}"))?;
        *self.out.lock() = Some(out.clone());
        // an op 14 renegotiation can land during the create: play() needs the codec negotiated now
        let codec = out
            .negotiated_video_codec()
            .map_err(|e| format!("output codec: {e}"))?
            .ok_or("the server negotiated no video codec for the output")?
            .as_str()
            .to_owned();
        self.shared.lock().negotiated = Some(codec.clone());
        self.log(
            "output_created",
            json!({"key": out.key(), "codec": codec, "source_codec": source_codec,
                   "width": width, "height": height, "fps": fps}),
        );
        self.check_codec_match();

        let me = Arc::downgrade(self);
        out.on_keyframe_request(move || {
            if let Some(relay) = me.upgrade() {
                relay.viewer_pli();
            }
        });
        // the publisher drops other codecs until the controller swaps it
        self.publisher.set_codec(&codec);
        self.publisher
            .set_source(GENERATION, self.epoch.load(Ordering::Relaxed));
        out.play(
            PublisherSource(Arc::clone(&self.publisher)),
            VideoParams {
                width,
                height,
                fps,
                bitrate,
            },
        )
        .map_err(|e| format!("play: {e}"))?;
        self.log("output_playing", json!({"codec": codec}));
        // the publisher waits for a keyframe that arrives after the source was set
        self.request_pli("output_start");
        self.codec_event(CodecEvent::OutputAdded {
            output_id: OutputId::Ingest,
            codec: codec.clone(),
            offer_top: "AV1".to_owned(),
            bound: Some(codec),
        });
        Ok(())
    }

    fn install_event_hook(self: &Arc<Self>) {
        let me = Arc::downgrade(self);
        self.probe.set_event_hook(move |event| {
            if let ClientEvent::SessionUpdate { stream_key, info } = event
                && let Some(relay) = me.upgrade()
            {
                relay.on_session_update(stream_key.as_deref(), info.video_codec.as_deref());
            }
        });
    }

    fn packet_tap(self: &Arc<Self>) -> PacketTap {
        let me = Arc::downgrade(self);
        Arc::new(move |packet: &Packet| {
            if let Some(relay) = me.upgrade() {
                relay.ingress(packet);
            }
        })
    }

    // ------------------------------------------------------------ background

    /// Worker egress to the publisher every 10 ms, the controller's tick every 100 ms, stats every 5 s.
    async fn drive(self: Arc<Self>) {
        let mut tick = tokio::time::interval(PUMP_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut next_codec_tick = monotonic();
        let mut next_stats = monotonic() + STATS_INTERVAL_S;
        let mut last_dave = None;
        loop {
            tick.tick().await;
            self.pump_worker();
            let now = monotonic();
            if self.pli.lock().poll(now) {
                self.send_pli("deferred");
            }
            if now >= next_codec_tick {
                next_codec_tick = now + CODEC_TICK_S;
                self.codec_event(CodecEvent::Tick);
                self.check_codec_match();
                let conn = self
                    .out
                    .lock()
                    .as_ref()
                    .and_then(|stream| stream.connection().ok());
                if let Some(conn) = conn {
                    log_dave_change(
                        &self.probe,
                        "output",
                        &mut last_dave,
                        dave_view(&conn, self.probe.target_id()),
                    );
                }
            }
            if now >= next_stats {
                next_stats += STATS_INTERVAL_S;
                self.log_output_stats();
            }
        }
    }

    fn log_output_stats(&self) {
        let Some(out) = self.out.lock().clone() else {
            return;
        };
        let stats = out.send_stats();
        self.shared.lock().send_stats = stats;
        let snapshot = self.publisher.snapshot();
        let window = self.publisher.take_window();
        self.log(
            "output_stats",
            json!({
                "playing": out.is_playing(),
                "video_frames": stats.video_frames,
                "video_packets": stats.video_packets,
                "video_octets": stats.video_octets,
                "audio_packets": stats.audio_packets,
                "dave_encrypt_errors": stats.dave_encrypt_errors,
                "send_errors": stats.send_errors,
                "nack_packets_received": stats.nack_packets_received,
                "rtx_packets_resent": stats.rtx_packets_resent,
                "pli_received": stats.pli_received,
                "pli_from_viewers": self.pli_from_viewers.load(Ordering::Relaxed),
                "publisher": {
                    "codec": self.publisher.codec(),
                    "frames": snapshot.frames,
                    "bytes": snapshot.bytes,
                    "freezes": snapshot.freezes,
                    "frozen_s": snapshot.frozen_s,
                    "window_sent": window.sent,
                    "stalls": window.stalls,
                    "max_send_gap_ms": window.max_send_gap_ms,
                    "audio_played": window.audio_played,
                    "audio_held_reads": window.audio_held_reads,
                    "audio_late_dropped": window.audio_late_dropped,
                    "audio_evicted": window.audio_evicted,
                },
            }),
        );
    }

    // -------------------------------------------------------------- teardown

    /// Spec §7, first step: stop the background work and end the output Go Live (op 19).
    async fn end_output(&self) {
        self.closed.store(true, Ordering::Relaxed);
        if let Some(drive) = self.drive.lock().take() {
            drive.abort();
        }
        let out = self.out.lock().take();
        if let Some(out) = out {
            let stats = out.send_stats();
            let negotiated = out
                .negotiated_video_codec()
                .ok()
                .flatten()
                .map(|c| c.as_str().to_owned());
            {
                let mut shared = self.shared.lock();
                shared.send_stats = stats;
                if negotiated.is_some() {
                    shared.negotiated = negotiated;
                }
            }
            let done = tokio::time::timeout(TEARDOWN_STEP * 2, out.disconnect()).await;
            self.log(
                "teardown_step",
                json!({"step": "output_end", "ok": done.is_ok(),
                       "video_frames": stats.video_frames, "audio_packets": stats.audio_packets}),
            );
        }
        self.publisher.close();
    }

    async fn stop_worker(&self) {
        let worker = Arc::clone(&self.worker);
        let stopped = tokio::task::spawn_blocking(move || worker.lock().stop())
            .await
            .is_ok();
        self.log(
            "teardown_step",
            json!({"step": "worker_stop", "ok": stopped}),
        );
    }

    fn report(&self) -> RelayReport {
        let shared = self.shared.lock();
        let mut errors = self.probe.progress.lock().errors.clone();
        if let Some(mismatch) =
            passthrough_mismatch(shared.negotiated.as_deref(), shared.source_codec.as_deref())
        {
            errors.push(mismatch);
        }
        RelayReport {
            watch_frames: self.probe.account.lock().frames(),
            published_video_frames: shared.send_stats.video_frames,
            published_audio_frames: shared.send_stats.audio_packets,
            negotiated_codec: shared.negotiated.clone(),
            source_codec: shared.source_codec.clone(),
            pli_from_viewers: self.pli_from_viewers.load(Ordering::Relaxed),
            errors,
        }
    }
}

/// The relay account: join, watch the target, forward it through the worker, publish the result.
async fn relay_flow(relay: Arc<Relay>, viewer_id: oneshot::Sender<u64>) -> Result<(), String> {
    let probe = Arc::clone(&relay.probe);
    let mut conn = probe.connect().await?;
    let _ = viewer_id.send(conn.user_id);
    relay.install_event_hook();

    let stream = probe
        .watch_target(&conn, TARGET_WAIT, Some(relay.packet_tap()))
        .await?;
    *relay.watch.lock() = Some(stream.clone());
    *relay.drive.lock() = Some(tokio::spawn(Arc::clone(&relay).drive()));

    // The watch loop keeps requesting video and keyframes while the output is being set up.
    let user_agent = conn.rest.headers_context().user_agent().to_owned();
    let looping = watch_loop(&probe, &conn.voice, &stream, &mut conn.voice_events, None);
    tokio::pin!(looping);
    let setup = relay.setup_output(&conn.voice, &user_agent);
    tokio::pin!(setup);
    tokio::select! {
        () = &mut looping => return Ok(()),
        done = &mut setup => done?,
    }
    looping.await;
    Ok(())
}

/// The viewer account: join, wait for the relay's Go Live, watch it for `seconds`.
async fn viewer_flow(
    probe: Arc<Probe>,
    relay_id: oneshot::Receiver<u64>,
    seconds: u64,
) -> Result<(), String> {
    let relay_id = relay_id
        .await
        .map_err(|_| "the relay account never logged in".to_owned())?;
    probe.set_target_id(relay_id);
    let mut conn = probe.connect().await?;
    let stream = probe.watch_target(&conn, VIEWER_WAIT, None).await?;
    watch_loop(
        &probe,
        &conn.voice,
        &stream,
        &mut conn.voice_events,
        Some(seconds),
    )
    .await;
    Ok(())
}

/// Runs `flow` until it ends or `stop` fires, then fires `stop` for the other side.
async fn until_stopped(
    stop: &watch::Sender<bool>,
    flow: impl Future<Output = Result<(), String>>,
) -> Result<(), String> {
    let mut stopped = stop.subscribe();
    let result = tokio::select! {
        result = flow => result,
        _ = stopped.wait_for(|s| *s) => Ok(()),
    };
    stop.send_replace(true);
    result
}

fn viewer_summary(probe: &Probe) -> Value {
    let account = probe.account.lock();
    let seen = probe.observed.lock();
    viewer_json(&summary_json(
        &probe.progress.lock(),
        &account,
        seen.dave_protocol_version,
        seen.transport_failures + seen.dave_failures,
    ))
}

/// A summary for a run that failed before both sessions existed.
fn finish_early(errors: Vec<String>) -> u8 {
    let relay = RelayReport {
        errors,
        ..RelayReport::default()
    };
    let viewer = viewer_json(&summary_json(
        &Progress::default(),
        &WatchAccount::new(),
        None,
        0,
    ));
    println!("{}", combined_summary(&relay, viewer));
    1
}

pub async fn run(args: RelayArgs) -> u8 {
    let config = match load_config(&args.config) {
        Ok(config) => config,
        Err(e) => return finish_early(e.errors),
    };
    if args.credential == args.viewer_credential {
        return finish_early(vec![
            "--credential and --viewer-credential must name different accounts".into(),
        ]);
    }
    let relay_token = match load_token(&args.credential) {
        Ok(token) => token,
        Err(e) => return finish_early(vec![e.to_string()]),
    };
    let viewer_token = match load_token(&args.viewer_credential) {
        Ok(token) => token,
        Err(e) => return finish_early(vec![e.to_string()]),
    };
    if relay_token == viewer_token {
        return finish_early(vec![
            "the two credentials hold the same token (one account cannot watch itself)".into(),
        ]);
    }

    let writer: Arc<StdMutex<dyn Write + Send>> = Arc::new(StdMutex::new(std::io::stderr()));
    let discord = &config.discord;
    let relay_probe = Arc::new(Probe::new(
        Arc::clone(&writer),
        Some("relay"),
        relay_token.clone(),
        std::slice::from_ref(&viewer_token),
        discord.guild_id,
        discord.target_user_id,
        discord.allowed_voice_channel_id,
    ));
    let viewer_probe = Arc::new(Probe::new(
        writer,
        Some("viewer"),
        viewer_token,
        &[relay_token],
        discord.guild_id,
        0,
        discord.allowed_voice_channel_id,
    ));
    let relay = match Relay::start(Arc::clone(&relay_probe), &config) {
        Ok(relay) => relay,
        Err(e) => return finish_early(vec![e]),
    };

    let (stop, _) = watch::channel(false);
    let (viewer_id_tx, viewer_id_rx) = oneshot::channel();
    let flows = async {
        tokio::join!(
            async {
                let result =
                    until_stopped(&stop, relay_flow(Arc::clone(&relay), viewer_id_tx)).await;
                if let Err(message) = &result {
                    relay_probe.error(message);
                }
            },
            async {
                let result = until_stopped(
                    &stop,
                    viewer_flow(Arc::clone(&viewer_probe), viewer_id_rx, args.seconds),
                )
                .await;
                if let Err(message) = &result {
                    viewer_probe.error(message);
                }
            },
        );
    };
    tokio::select! {
        () = flows => {}
        _ = tokio::signal::ctrl_c() => {
            relay_probe.log("stop_requested", json!({"reason": "sigint"}));
        }
    }

    // viewer first, then the relay in spec §7 order: end output, stop worker, stop watching, leave
    viewer_probe.teardown().await;
    relay.end_output().await;
    relay.stop_worker().await;
    relay_probe.teardown().await;

    let summary = combined_summary(&relay.report(), viewer_summary(&viewer_probe));
    println!("{}", relay_probe.redact_secrets(&summary.to_string()));
    exit_code(&summary["viewer"])
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn relay() -> (Arc<Relay>, Arc<StdMutex<Vec<u8>>>) {
        let config = load_config(
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config.example.yaml"),
        )
        .unwrap();
        let buffer = Arc::new(StdMutex::new(Vec::new()));
        let writer: Arc<StdMutex<dyn Write + Send>> = buffer.clone();
        let probe = Arc::new(Probe::new(
            writer,
            Some("relay"),
            "relay-secret".into(),
            &[],
            1,
            2,
            3,
        ));
        (Relay::start(probe, &config).unwrap(), buffer)
    }

    fn logged(buffer: &StdMutex<Vec<u8>>) -> String {
        String::from_utf8(buffer.lock().unwrap().clone()).unwrap()
    }

    fn event(kind: WorkerEventKind, data: Value) -> WorkerEvent {
        let Value::Object(data) = data else {
            panic!("not an object");
        };
        WorkerEvent {
            generation: GENERATION,
            epoch: 1,
            kind,
            data,
        }
    }

    #[test]
    fn the_worker_runs_in_adaptive_mode_with_the_configured_quality() {
        let (relay, _) = relay();
        assert_eq!(relay.profile.codec_mode, CodecMode::Adaptive);
        assert_eq!(relay.profile.max_width, 2560);
        assert_eq!(relay.profile.fps, 60);
    }

    #[test]
    fn the_source_codec_event_feeds_the_report_and_the_controller() {
        let (relay, buffer) = relay();
        relay.on_worker_event(event(
            WorkerEventKind::SourceCodec,
            json!({"codec": "h264", "width": 1280, "height": 720, "eligible": true}),
        ));
        {
            let shared = relay.shared.lock();
            assert_eq!(shared.source_codec.as_deref(), Some("H264"));
            assert_eq!(shared.src_dims, Some((1280, 720)));
        }
        assert_eq!(relay.ctl.lock().effective(), Some("H264"));
        assert!(logged(&buffer).contains(r#""event":"codec_change""#));
    }

    #[test]
    fn an_ineligible_source_has_no_dimensions_and_the_controller_falls_back() {
        let (relay, _) = relay();
        relay.on_worker_event(event(
            WorkerEventKind::SourceCodec,
            json!({"codec": "AV1", "width": 7680, "height": 4320, "eligible": false}),
        ));
        assert_eq!(relay.shared.lock().src_dims, None);
        assert_eq!(relay.ctl.lock().effective(), Some("H264"));
    }

    #[test]
    fn the_first_frame_locks_the_canvas() {
        let (relay, _) = relay();
        relay.on_worker_event(event(
            WorkerEventKind::FirstFrame,
            json!({"width": 1920, "height": 1080, "codec": "H264"}),
        ));
        assert_eq!(relay.shared.lock().canvas, Some((1920, 1080)));
    }

    #[test]
    fn events_of_another_generation_are_ignored() {
        let (relay, _) = relay();
        let mut stale = event(
            WorkerEventKind::FirstFrame,
            json!({"width": 1920, "height": 1080}),
        );
        stale.generation = GENERATION + 1;
        relay.on_worker_event(stale);
        assert_eq!(relay.shared.lock().canvas, None);
    }

    #[test]
    fn ineligible_source_and_fatal_worker_events_are_errors() {
        let (relay, _) = relay();
        relay.on_worker_event(event(
            WorkerEventKind::SourceIneligible,
            json!({"codec": "H264", "width": 9000, "height": 9000}),
        ));
        relay.on_worker_event(event(
            WorkerEventKind::Fatal,
            json!({"reason": "worker_crash"}),
        ));
        let errors = relay.probe.progress.lock().errors.clone();
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors[0].contains("not eligible"), "{errors:?}");
        assert!(errors[1].contains("worker_crash"), "{errors:?}");
    }

    #[test]
    fn a_differing_output_and_source_codec_is_reported_as_an_error() {
        let (relay, buffer) = relay();
        {
            let mut shared = relay.shared.lock();
            shared.negotiated = Some("H264".into());
            shared.source_codec = Some("AV1".into());
            shared.send_stats.video_frames = 12;
        }
        relay.check_codec_match();
        relay.check_codec_match();
        let log = logged(&buffer);
        assert_eq!(
            log.matches("codec_mismatch").count(),
            1,
            "logged once: {log}"
        );
        let report = relay.report();
        assert_eq!(report.negotiated_codec.as_deref(), Some("H264"));
        assert_eq!(report.source_codec.as_deref(), Some("AV1"));
        assert_eq!(report.published_video_frames, 12);
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].contains("passthrough impossible"));

        relay.shared.lock().source_codec = Some("H264".into());
        relay.check_codec_match();
        assert!(relay.report().errors.is_empty());
    }

    #[test]
    fn actions_without_an_output_fail_and_are_fed_back_to_the_controller() {
        let (relay, _) = relay();
        let update = CodecAction::UpdateOutput {
            output_id: OutputId::Ingest,
            codecs: &["H264", "VP8", "VP9"],
        };
        assert_eq!(
            relay.run_action(update.clone()),
            [CodecEvent::ActionFailed(update)]
        );
        let swap = CodecAction::SwapOutput {
            output_id: OutputId::Ingest,
            codec: "H264".into(),
        };
        assert_eq!(
            relay.run_action(swap),
            [CodecEvent::OutputBound {
                output_id: OutputId::Ingest,
                codec: None
            }]
        );
        let watch = CodecAction::UpdateWatch {
            codecs: &["H264", "VP8", "VP9"],
        };
        assert!(matches!(
            relay.run_action(watch).as_slice(),
            [CodecEvent::ActionFailed(_)]
        ));
    }

    #[test]
    fn an_ingress_overflow_starts_a_new_epoch_at_most_once_a_second() {
        let (relay, _) = relay();
        assert_eq!(relay.epoch.load(Ordering::Relaxed), 1);
        relay.ingress_overflow(1);
        assert_eq!(relay.epoch.load(Ordering::Relaxed), 2);
        relay.ingress_overflow(2);
        assert_eq!(relay.epoch.load(Ordering::Relaxed), 2, "rate limited");
        relay.ingress_overflow(1);
        assert_eq!(relay.epoch.load(Ordering::Relaxed), 2, "stale epoch");
    }

    #[test]
    fn viewer_plis_are_counted_even_when_the_pli_to_the_target_is_rate_limited() {
        let (relay, buffer) = relay();
        for _ in 0..5 {
            relay.viewer_pli();
        }
        assert_eq!(relay.report().pli_from_viewers, 5);
        assert_eq!(logged(&buffer).matches("viewer_pli").count(), 5);
    }

    #[test]
    fn a_session_update_for_another_stream_is_not_the_output() {
        let (relay, _) = relay();
        relay.on_session_update(Some("guild:1:3:99"), Some("vp9"));
        relay.on_session_update(None, Some("vp9"));
        assert_eq!(relay.shared.lock().negotiated, None);
    }

    #[test]
    fn the_report_never_carries_a_token() {
        let (relay, _) = relay();
        relay.probe.error("failed with relay-secret");
        let json = relay.report().to_json().to_string();
        assert!(!json.contains("relay-secret"), "{json}");
    }

    #[tokio::test]
    async fn until_stopped_ends_the_flow_when_the_other_side_stops() {
        let (stop, _) = watch::channel(false);
        let flow = until_stopped(&stop, async {
            std::future::pending::<()>().await;
            Ok(())
        });
        let stopper = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            stop.send_replace(true);
        };
        let (result, ()) = tokio::join!(flow, stopper);
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn a_finished_flow_stops_the_other_side_and_keeps_its_error() {
        let (stop, rx) = watch::channel(false);
        let result = until_stopped(&stop, async { Err("login: boom".to_owned()) }).await;
        assert_eq!(result, Err("login: boom".to_owned()));
        assert!(*rx.borrow());
    }

    #[tokio::test]
    async fn the_viewer_flow_fails_cleanly_when_the_relay_never_logs_in() {
        let writer: Arc<StdMutex<dyn Write + Send>> = Arc::new(StdMutex::new(Vec::new()));
        let probe = Arc::new(Probe::new(writer, Some("viewer"), "v".into(), &[], 1, 0, 3));
        let (tx, rx) = oneshot::channel::<u64>();
        drop(tx);
        let result = viewer_flow(probe, rx, 1).await;
        assert_eq!(result, Err("the relay account never logged in".to_owned()));
    }
}
