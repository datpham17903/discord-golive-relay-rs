// Vendored from discord-native-voice (MIT): https://github.com/dolfies/discord-native-voice
// Commit:        18bbd204bfe46a781bee37cc8c10d574bdf435dd
// Original path: src/python/net.rs
// Copyright (c) 2026-present Dolfies. MIT licence text: see NOTICE.
// Changes:       pyo3 layer removed: `PyNativeUDPTransport` is now `UdpTransport`,
//                listeners are Rust closures (a panicking listener is counted in
//                `callback_errors`), stats come back as `UdpStats`. The raw
//                `send(2)`/`setsockopt(2)` FFI (Windows and Unix variants) is
//                replaced by `socket2` and `std::net::UdpSocket`, so the crate
//                forbids unsafe code on every platform. Dropping the transport
//                closes it.

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use socket2::{Domain, Protocol, Socket, Type};

use super::udp::{self, PingSender, UDPPing};

const SEND_WOULD_BLOCK_TIMEOUT: Duration = Duration::from_millis(50);
const SEND_WOULD_BLOCK_SLEEP: Duration = Duration::from_millis(1);
const UDP_IDLE_SLEEP: Duration = Duration::from_millis(1);
const UDP_PACKET_BUFFER_SIZE: usize = 2048;
const UDP_RECV_BATCH_SIZE: usize = 128;
const UDP_DEFAULT_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const UDP_DISPATCH_QUEUE_CAPACITY: usize = 256;
const DISPATCHER_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
const UDP_LISTENER_RTP: u8 = 1;
const UDP_LISTENER_RTCP: u8 = 1 << 1;

/// Which packet classes a listener wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenerKinds {
    pub rtp: bool,
    pub rtcp: bool,
}

impl ListenerKinds {
    fn flags(self) -> u8 {
        let mut flags = 0;
        if self.rtp {
            flags |= UDP_LISTENER_RTP;
        }
        if self.rtcp {
            flags |= UDP_LISTENER_RTCP;
        }
        flags
    }
}

type PacketCallback = Arc<dyn Fn(&[u8]) + Send + Sync>;
type BatchCallback = Arc<dyn Fn(&[Vec<u8>]) + Send + Sync>;
/// A batch with the arrival time of each datagram, taken when the reader thread got it from the
/// socket (before any queueing or processing).
type TimedBatchCallback = Arc<dyn Fn(&[Vec<u8>], &[Instant]) + Send + Sync>;

#[derive(Clone)]
enum ListenerCallback {
    Single(PacketCallback),
    Batch(BatchCallback),
    TimedBatch(TimedBatchCallback),
}

fn update_atomic_max(value: &AtomicUsize, candidate: usize) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

fn is_transient(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
        || err.kind() == io::ErrorKind::Interrupted
        || err.kind() == io::ErrorKind::TimedOut
}

fn retry_would_block(mut send: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    let started = Instant::now();
    loop {
        match send() {
            Ok(()) => return Ok(()),
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::Interrupted =>
            {
                if started.elapsed() >= SEND_WOULD_BLOCK_TIMEOUT {
                    return Err(err);
                }
                std::thread::sleep(SEND_WOULD_BLOCK_SLEEP);
            }
            Err(err) => return Err(err),
        }
    }
}

fn send_datagram(socket: &UdpSocket, packet: &[u8]) -> io::Result<()> {
    retry_would_block(|| {
        let sent = socket.send(packet)?;
        if sent != packet.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "Socket sent a partial datagram",
            ));
        }
        Ok(())
    })
}

/// Counters of one transport. Field names match the Python `stats()` keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UdpStats {
    pub packets_received: usize,
    pub rtp_packets_received: usize,
    pub rtcp_packets_received: usize,
    pub control_packets_received: usize,
    pub invalid_packets_received: usize,
    pub packets_sent: usize,
    pub octets_sent: usize,
    pub callbacks_called: usize,
    pub callback_errors: usize,
    pub dispatch_batches_queued: usize,
    pub dispatch_packets_queued: usize,
    pub dispatch_batches_dispatched: usize,
    pub dispatch_packets_dispatched: usize,
    pub dispatch_queue_pending: usize,
    pub dispatch_queue_max_pending: usize,
    pub dispatch_batches_dropped: usize,
}

#[derive(Default)]
struct UdpTransportStats {
    packets_received: AtomicUsize,
    rtp_packets_received: AtomicUsize,
    rtcp_packets_received: AtomicUsize,
    control_packets_received: AtomicUsize,
    invalid_packets_received: AtomicUsize,
    packets_sent: AtomicUsize,
    octets_sent: AtomicUsize,
    callbacks_called: AtomicUsize,
    callback_errors: AtomicUsize,
    dispatch_batches_queued: AtomicUsize,
    dispatch_packets_queued: AtomicUsize,
    dispatch_batches_dispatched: AtomicUsize,
    dispatch_packets_dispatched: AtomicUsize,
    dispatch_queue_pending: AtomicUsize,
    dispatch_queue_max_pending: AtomicUsize,
    dispatch_batches_dropped: AtomicUsize,
}

struct UdpListener {
    id: u64,
    flags: u8,
    callback: ListenerCallback,
}

struct UdpTransportInner {
    socket: Arc<UdpSocket>,
    closed: AtomicBool,
    reader_running: AtomicBool,
    next_listener_id: AtomicU64,
    listeners: Mutex<Vec<UdpListener>>,
    reader: Mutex<Option<JoinHandle<()>>>,
    dispatcher: Mutex<Option<JoinHandle<()>>>,
    dispatch_tx: Mutex<Option<SyncSender<UdpDispatchBatch>>>,
    ping: Mutex<Option<UDPPing>>,
    stats: UdpTransportStats,
}

struct UdpDispatchBatch {
    rtp_packets: Vec<Vec<u8>>,
    rtp_arrivals: Vec<Instant>,
    rtcp_packets: Vec<Vec<u8>>,
    rtcp_arrivals: Vec<Instant>,
}

impl UdpTransportInner {
    fn ensure_open(&self) -> io::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "UDP transport is closed",
            ));
        }
        Ok(())
    }

    fn send_packet(&self, packet: &[u8]) -> io::Result<()> {
        self.ensure_open()?;
        send_datagram(&self.socket, packet)?;
        self.stats.packets_sent.fetch_add(1, Ordering::Relaxed);
        self.stats
            .octets_sent
            .fetch_add(packet.len(), Ordering::Relaxed);
        Ok(())
    }

    fn add_listener(&self, callback: ListenerCallback, flags: u8) -> u64 {
        let id = self.next_listener_id.fetch_add(1, Ordering::Relaxed);
        self.listeners
            .lock()
            .expect("UDP transport listeners poisoned")
            .push(UdpListener {
                id,
                flags,
                callback,
            });
        id
    }

    fn remove_listener(&self, id: u64) {
        self.listeners
            .lock()
            .expect("UDP transport listeners poisoned")
            .retain(|listener| listener.id != id);
    }

    fn start_reader(self: &Arc<Self>) {
        if self.reader_running.swap(true, Ordering::SeqCst) {
            return;
        }

        let (dispatch_tx, dispatch_rx) = mpsc::sync_channel(UDP_DISPATCH_QUEUE_CAPACITY);
        *self
            .dispatch_tx
            .lock()
            .expect("UDP dispatcher sender poisoned") = Some(dispatch_tx);

        let dispatcher_inner = Arc::clone(self);
        let dispatcher_handle = thread::Builder::new()
            .name("native-voice-udp-dispatch".to_owned())
            .spawn(move || run_udp_dispatcher(dispatcher_inner, dispatch_rx));
        match dispatcher_handle {
            Ok(handle) => {
                *self
                    .dispatcher
                    .lock()
                    .expect("UDP dispatcher handle poisoned") = Some(handle);
            }
            Err(_) => {
                self.reader_running.store(false, Ordering::SeqCst);
                self.dispatch_tx
                    .lock()
                    .expect("UDP dispatcher sender poisoned")
                    .take();
                return;
            }
        }

        let inner = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("native-voice-udp-transport".to_owned())
            .spawn(move || run_udp_reader(inner));
        match handle {
            Ok(handle) => {
                *self.reader.lock().expect("UDP reader handle poisoned") = Some(handle);
            }
            Err(_) => {
                self.reader_running.store(false, Ordering::SeqCst);
                self.dispatch_tx
                    .lock()
                    .expect("UDP dispatcher sender poisoned")
                    .take();
                if let Some(handle) = self
                    .dispatcher
                    .lock()
                    .expect("UDP dispatcher handle poisoned")
                    .take()
                    && handle.thread().id() != thread::current().id()
                {
                    let _ = handle.join();
                }
            }
        }
    }

    fn stop_reader_threads(&self) {
        self.reader_running.store(false, Ordering::SeqCst);
        self.dispatch_tx
            .lock()
            .expect("UDP dispatcher sender poisoned")
            .take();

        let handle = self
            .reader
            .lock()
            .expect("UDP reader handle poisoned")
            .take();
        if let Some(handle) = handle
            && handle.thread().id() != thread::current().id()
        {
            let _ = handle.join();
        }

        let handle = self
            .dispatcher
            .lock()
            .expect("UDP dispatcher handle poisoned")
            .take();
        if let Some(handle) = handle
            && handle.thread().id() != thread::current().id()
        {
            let deadline = Instant::now() + DISPATCHER_JOIN_TIMEOUT;
            while !handle.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            if handle.is_finished() {
                let _ = handle.join();
            } else {
                eprintln!("UDP dispatcher did not stop within the join timeout; detaching it");
            }
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(mut ping) = self.ping.lock().expect("UDP ping poisoned").take() {
            ping.stop();
        }
        self.stop_reader_threads();
    }

    fn start_ping(
        self: &Arc<Self>,
        initial_delay: Duration,
        interval: Duration,
        timeout: Duration,
    ) {
        self.start_reader();
        let mut ping = self.ping.lock().expect("UDP ping poisoned");
        if ping.is_some() {
            return;
        }
        let socket = Arc::clone(&self.socket);
        let send_packet: PingSender = Box::new(move |packet| send_datagram(&socket, packet));
        *ping = Some(UDPPing::start(
            initial_delay,
            interval,
            timeout,
            send_packet,
        ));
    }

    fn stop_ping(&self) {
        if let Some(mut ping) = self.ping.lock().expect("UDP ping poisoned").take() {
            ping.stop();
        }
    }
}

impl Drop for UdpTransportInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(mut ping) = self.ping.lock().expect("UDP ping poisoned").take() {
            ping.stop();
        }
        self.stop_reader_threads();
    }
}

#[derive(Clone, Copy)]
enum UdpPacketKind {
    Rtp,
    Rtcp,
}

fn classify_udp_packet(data: &[u8]) -> Option<UdpPacketKind> {
    if udp::is_rtcp_packet(data) {
        return Some(UdpPacketKind::Rtcp);
    }
    if udp::is_rtp_packet(data) {
        return Some(UdpPacketKind::Rtp);
    }
    None
}

fn run_udp_reader(inner: Arc<UdpTransportInner>) {
    let mut buffer = [0u8; UDP_PACKET_BUFFER_SIZE];
    let mut packets = Vec::with_capacity(UDP_RECV_BATCH_SIZE);
    while inner.reader_running.load(Ordering::SeqCst) {
        packets.clear();
        match inner.socket.recv(&mut buffer) {
            Ok(length) => packets.push((buffer[..length].to_vec(), Instant::now())),
            Err(err) if is_transient(&err) => {
                thread::sleep(UDP_IDLE_SLEEP);
                continue;
            }
            Err(_) => {
                if !inner.closed.load(Ordering::SeqCst) {
                    inner
                        .stats
                        .invalid_packets_received
                        .fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
        }

        while packets.len() < UDP_RECV_BATCH_SIZE {
            match inner.socket.recv(&mut buffer) {
                Ok(length) => packets.push((buffer[..length].to_vec(), Instant::now())),
                Err(err) if is_transient(&err) => break,
                Err(_) => {
                    if !inner.closed.load(Ordering::SeqCst) {
                        inner
                            .stats
                            .invalid_packets_received
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    break;
                }
            }
        }

        let batch = std::mem::replace(&mut packets, Vec::with_capacity(UDP_RECV_BATCH_SIZE));
        handle_udp_packets(&inner, batch);
    }
}

fn handle_udp_packets(inner: &Arc<UdpTransportInner>, packets: Vec<(Vec<u8>, Instant)>) {
    if inner.closed.load(Ordering::SeqCst) || !inner.reader_running.load(Ordering::SeqCst) {
        return;
    }

    let mut rtp_packets: Vec<Vec<u8>> = Vec::new();
    let mut rtp_arrivals: Vec<Instant> = Vec::new();
    let mut rtcp_packets: Vec<Vec<u8>> = Vec::new();
    let mut rtcp_arrivals: Vec<Instant> = Vec::new();
    for (packet, arrived) in packets {
        inner.stats.packets_received.fetch_add(1, Ordering::Relaxed);

        if let Some(ping) = inner.ping.lock().expect("UDP ping poisoned").as_ref()
            && ping.handle_packet(packet.as_slice())
        {
            inner
                .stats
                .control_packets_received
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }

        if udp::is_native_control_packet(packet.as_slice()) {
            inner
                .stats
                .control_packets_received
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }

        match classify_udp_packet(packet.as_slice()) {
            Some(UdpPacketKind::Rtp) => {
                inner
                    .stats
                    .rtp_packets_received
                    .fetch_add(1, Ordering::Relaxed);
                rtp_packets.push(packet);
                rtp_arrivals.push(arrived);
            }
            Some(UdpPacketKind::Rtcp) => {
                inner
                    .stats
                    .rtcp_packets_received
                    .fetch_add(1, Ordering::Relaxed);
                rtcp_packets.push(packet);
                rtcp_arrivals.push(arrived);
            }
            None => {
                inner
                    .stats
                    .invalid_packets_received
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    queue_udp_dispatch(
        inner,
        UdpDispatchBatch {
            rtp_packets,
            rtp_arrivals,
            rtcp_packets,
            rtcp_arrivals,
        },
    );
}

fn queue_udp_dispatch(inner: &Arc<UdpTransportInner>, batch: UdpDispatchBatch) {
    let packet_count = batch.rtp_packets.len() + batch.rtcp_packets.len();
    if packet_count == 0 {
        return;
    }

    let sender = inner
        .dispatch_tx
        .lock()
        .expect("UDP dispatcher sender poisoned")
        .as_ref()
        .cloned();
    let Some(sender) = sender else {
        return;
    };

    let pending = inner
        .stats
        .dispatch_queue_pending
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    update_atomic_max(&inner.stats.dispatch_queue_max_pending, pending);

    match sender.try_send(batch) {
        Ok(()) => {
            inner
                .stats
                .dispatch_batches_queued
                .fetch_add(1, Ordering::Relaxed);
            inner
                .stats
                .dispatch_packets_queued
                .fetch_add(packet_count, Ordering::Relaxed);
        }
        Err(err) => {
            inner
                .stats
                .dispatch_queue_pending
                .fetch_sub(1, Ordering::Relaxed);
            if matches!(err, TrySendError::Full(_)) {
                inner
                    .stats
                    .dispatch_batches_dropped
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn run_udp_dispatcher(inner: Arc<UdpTransportInner>, receiver: Receiver<UdpDispatchBatch>) {
    while let Ok(batch) = receiver.recv() {
        inner
            .stats
            .dispatch_queue_pending
            .fetch_sub(1, Ordering::Relaxed);
        if inner.closed.load(Ordering::SeqCst) {
            break;
        }

        let packet_count = batch.rtp_packets.len() + batch.rtcp_packets.len();
        dispatch_udp_packets(
            &inner,
            UDP_LISTENER_RTP,
            batch.rtp_packets,
            &batch.rtp_arrivals,
        );
        dispatch_udp_packets(
            &inner,
            UDP_LISTENER_RTCP,
            batch.rtcp_packets,
            &batch.rtcp_arrivals,
        );
        inner
            .stats
            .dispatch_batches_dispatched
            .fetch_add(1, Ordering::Relaxed);
        inner
            .stats
            .dispatch_packets_dispatched
            .fetch_add(packet_count, Ordering::Relaxed);
    }
}

fn dispatch_udp_packets(
    inner: &Arc<UdpTransportInner>,
    listener_flag: u8,
    packets: Vec<Vec<u8>>,
    arrivals: &[Instant],
) {
    if packets.is_empty() {
        return;
    }

    let callbacks = inner
        .listeners
        .lock()
        .expect("UDP transport listeners poisoned")
        .iter()
        .filter(|listener| listener.flags & listener_flag != 0)
        .map(|listener| listener.callback.clone())
        .collect::<Vec<_>>();

    for callback in callbacks {
        match callback {
            ListenerCallback::Batch(callback) => {
                call_listener(inner, || callback(&packets));
            }
            ListenerCallback::TimedBatch(callback) => {
                call_listener(inner, || callback(&packets, arrivals));
            }
            ListenerCallback::Single(callback) => {
                for packet in packets.iter() {
                    call_listener(inner, || callback(packet));
                }
            }
        }
    }
}

fn call_listener(inner: &UdpTransportInner, call: impl FnOnce()) {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(()) => {
            inner.stats.callbacks_called.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            inner.stats.callback_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn bind_udp_socket(buffer_size: usize, tos: u32) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_nonblocking(true)?;
    // Buffer size and TOS are best-effort tuning, as in the original.
    let _ = socket.set_recv_buffer_size(buffer_size);
    let _ = socket.set_send_buffer_size(buffer_size);
    let _ = socket.set_tos_v4(tos);
    let any: SocketAddr = ([0, 0, 0, 0], 0).into();
    socket.bind(&any.into())?;
    Ok(socket.into())
}

/// Non-blocking IPv4 UDP socket with a reader thread that classifies incoming
/// datagrams (RTP / RTCP / native control) and hands them to listeners on a
/// separate dispatcher thread.
pub struct UdpTransport {
    inner: Arc<UdpTransportInner>,
}

impl UdpTransport {
    pub fn new(buffer_size: Option<usize>, tos: Option<u32>) -> io::Result<Self> {
        let socket = bind_udp_socket(
            buffer_size
                .unwrap_or(UDP_DEFAULT_BUFFER_SIZE)
                .min(i32::MAX as usize),
            tos.unwrap_or(0),
        )?;
        Ok(Self {
            inner: Arc::new(UdpTransportInner {
                socket: Arc::new(socket),
                closed: AtomicBool::new(false),
                reader_running: AtomicBool::new(false),
                next_listener_id: AtomicU64::new(1),
                listeners: Mutex::new(Vec::new()),
                reader: Mutex::new(None),
                dispatcher: Mutex::new(None),
                dispatch_tx: Mutex::new(None),
                ping: Mutex::new(None),
                stats: UdpTransportStats::default(),
            }),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    pub fn connect(&self, address: &str, port: u16) -> io::Result<()> {
        self.inner.ensure_open()?;
        self.inner.socket.connect((address, port))
    }

    /// Sends the 74-byte IP discovery request and waits for the matching reply;
    /// returns the external `(ip, port)` the server saw.
    pub fn discover_ip(&self, ssrc: u32, timeout: Duration) -> io::Result<(String, u16)> {
        self.inner.ensure_open()?;
        udp::discover_ip_socket(&self.inner.socket, ssrc, timeout)
    }

    /// Calls `callback` once per matching datagram. Starts the reader threads.
    pub fn add_listener(
        &self,
        callback: impl Fn(&[u8]) + Send + Sync + 'static,
        kinds: ListenerKinds,
    ) -> u64 {
        self.inner.start_reader();
        self.inner
            .add_listener(ListenerCallback::Single(Arc::new(callback)), kinds.flags())
    }

    /// Calls `callback` once per received batch of matching datagrams.
    pub fn add_batch_listener(
        &self,
        callback: impl Fn(&[Vec<u8>]) + Send + Sync + 'static,
        kinds: ListenerKinds,
    ) -> u64 {
        self.inner.start_reader();
        self.inner
            .add_listener(ListenerCallback::Batch(Arc::new(callback)), kinds.flags())
    }

    /// Like [`Self::add_batch_listener`], with each datagram's socket arrival time (the reader
    /// thread's clock when it read it), so callers can time packets independent of dispatch delay.
    pub fn add_timed_batch_listener(
        &self,
        callback: impl Fn(&[Vec<u8>], &[Instant]) + Send + Sync + 'static,
        kinds: ListenerKinds,
    ) -> u64 {
        self.inner.start_reader();
        self.inner.add_listener(
            ListenerCallback::TimedBatch(Arc::new(callback)),
            kinds.flags(),
        )
    }

    pub fn remove_listener(&self, id: u64) {
        self.inner.remove_listener(id);
    }

    pub fn send_packet(&self, packet: &[u8]) -> io::Result<()> {
        self.inner.send_packet(packet)
    }

    /// Returns `(packets sent, octets sent)`.
    pub fn send_packets<P: AsRef<[u8]>>(&self, packets: &[P]) -> io::Result<(usize, usize)> {
        let mut sent = 0usize;
        let mut octets = 0usize;
        for packet in packets {
            let packet = packet.as_ref();
            self.inner.send_packet(packet)?;
            sent += 1;
            octets += packet.len();
        }
        Ok((sent, octets))
    }

    pub fn start_ping(&self, initial_delay: Duration, interval: Duration, timeout: Duration) {
        self.inner.start_ping(initial_delay, interval, timeout);
    }

    pub fn stop_ping(&self) {
        self.inner.stop_ping();
    }

    pub fn ping_rtt_ms(&self) -> Option<f64> {
        self.inner
            .ping
            .lock()
            .expect("UDP ping poisoned")
            .as_ref()
            .and_then(|ping| ping.rtt_ms())
    }

    pub fn ping_timeouts(&self) -> usize {
        self.inner
            .ping
            .lock()
            .expect("UDP ping poisoned")
            .as_ref()
            .map_or(0, |ping| ping.timeouts())
    }

    pub fn ping_send_errors(&self) -> usize {
        self.inner
            .ping
            .lock()
            .expect("UDP ping poisoned")
            .as_ref()
            .map_or(0, |ping| ping.send_errors())
    }

    pub fn stats(&self) -> UdpStats {
        let stats = &self.inner.stats;
        let load = |value: &AtomicUsize| value.load(Ordering::Relaxed);
        UdpStats {
            packets_received: load(&stats.packets_received),
            rtp_packets_received: load(&stats.rtp_packets_received),
            rtcp_packets_received: load(&stats.rtcp_packets_received),
            control_packets_received: load(&stats.control_packets_received),
            invalid_packets_received: load(&stats.invalid_packets_received),
            packets_sent: load(&stats.packets_sent),
            octets_sent: load(&stats.octets_sent),
            callbacks_called: load(&stats.callbacks_called),
            callback_errors: load(&stats.callback_errors),
            dispatch_batches_queued: load(&stats.dispatch_batches_queued),
            dispatch_packets_queued: load(&stats.dispatch_packets_queued),
            dispatch_batches_dispatched: load(&stats.dispatch_batches_dispatched),
            dispatch_packets_dispatched: load(&stats.dispatch_packets_dispatched),
            dispatch_queue_pending: load(&stats.dispatch_queue_pending),
            dispatch_queue_max_pending: load(&stats.dispatch_queue_max_pending),
            dispatch_batches_dropped: load(&stats.dispatch_batches_dropped),
        }
    }

    /// Stops the ping and reader threads; later sends fail with `NotConnected`.
    pub fn close(&self) {
        self.inner.close();
    }
}

impl Drop for UdpTransport {
    fn drop(&mut self) {
        self.inner.close();
    }
}
