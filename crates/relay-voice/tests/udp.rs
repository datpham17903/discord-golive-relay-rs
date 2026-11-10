//! UDP transport against a local fake voice server on 127.0.0.1: IP discovery, send/receive,
//! packet classification, listeners, ping and shutdown.

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use relay_voice::transport::net::{ListenerKinds, UdpTransport};
use relay_voice::transport::udp::{
    is_native_control_packet, is_rtcp_packet, is_rtp_packet, native_ping_request,
    native_ping_response_sequence, parse_ip_discovery_response,
};

const SSRC: u32 = 0x0102_0304;
const RTP: ListenerKinds = ListenerKinds {
    rtp: true,
    rtcp: false,
};
const RTCP: ListenerKinds = ListenerKinds {
    rtp: false,
    rtcp: true,
};

fn fake_server() -> (UdpSocket, SocketAddr) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    (socket, addr)
}

fn connected_transport(server: SocketAddr) -> UdpTransport {
    let transport = UdpTransport::new(None, None).unwrap();
    transport.connect("127.0.0.1", server.port()).unwrap();
    transport
}

fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached in time");
        thread::sleep(Duration::from_millis(2));
    }
}

fn rtp_packet(payload_type: u8, sequence: u16) -> Vec<u8> {
    let mut packet = vec![0x80, payload_type];
    packet.extend_from_slice(&sequence.to_be_bytes());
    packet.extend_from_slice(&[0; 8]);
    packet.extend_from_slice(&[0xEE; 8]);
    packet
}

fn rtcp_packet() -> Vec<u8> {
    vec![0x81, 205, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2]
}

fn discovery_response(ssrc: u32, ip: &str, port: u16) -> Vec<u8> {
    let mut packet = vec![0u8; 74];
    packet[..2].copy_from_slice(&2u16.to_be_bytes());
    packet[2..4].copy_from_slice(&70u16.to_be_bytes());
    packet[4..8].copy_from_slice(&ssrc.to_be_bytes());
    packet[8..8 + ip.len()].copy_from_slice(ip.as_bytes());
    packet[72..].copy_from_slice(&port.to_be_bytes());
    packet
}

#[test]
fn test_ip_discovery_sends_the_74_byte_request_and_parses_the_response() {
    let (server, addr) = fake_server();
    let handle = thread::spawn(move || {
        let mut buffer = [0u8; 2048];
        let (length, client) = server.recv_from(&mut buffer).unwrap();
        let request = buffer[..length].to_vec();
        // noise first: wrong SSRC, wrong length, wrong type -- all must be ignored
        server
            .send_to(&discovery_response(SSRC + 1, "9.9.9.9", 1), client)
            .unwrap();
        server.send_to(&[0u8; 20], client).unwrap();
        let mut wrong_type = discovery_response(SSRC, "9.9.9.9", 1);
        wrong_type[1] = 1;
        server.send_to(&wrong_type, client).unwrap();
        server
            .send_to(&discovery_response(SSRC, "203.0.113.7", 50_123), client)
            .unwrap();
        request
    });

    let transport = connected_transport(addr);
    let found = transport.discover_ip(SSRC, Duration::from_secs(5)).unwrap();
    assert_eq!(found, ("203.0.113.7".to_owned(), 50_123));

    let request = handle.join().unwrap();
    assert_eq!(request.len(), 74);
    assert_eq!(request[..2], [0x00, 0x01], "type: request");
    assert_eq!(request[2..4], [0x00, 70], "payload length");
    assert_eq!(request[4..8], SSRC.to_be_bytes());
    assert!(
        request[8..].iter().all(|byte| *byte == 0),
        "address and port zeroed"
    );
}

#[test]
fn test_ip_discovery_times_out_without_a_response() {
    let (_server, addr) = fake_server();
    let transport = connected_transport(addr);
    let started = Instant::now();
    let err = transport
        .discover_ip(SSRC, Duration::from_millis(150))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() >= Duration::from_millis(150));
}

#[test]
fn test_parse_ip_discovery_response_edge_cases() {
    // full-length address without a NUL terminator
    let mut packet = discovery_response(SSRC, "", 4000);
    packet[8..72].fill(b'1');
    let (ip, port) = parse_ip_discovery_response(&packet, SSRC).unwrap().unwrap();
    assert_eq!((ip.len(), port), (64, 4000));

    // not UTF-8 / not ASCII address
    let mut packet = discovery_response(SSRC, "", 1);
    packet[8] = 0xFF;
    assert_eq!(
        parse_ip_discovery_response(&packet, SSRC)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );

    // wrong payload length field
    let mut packet = discovery_response(SSRC, "1.2.3.4", 1);
    packet[3] = 69;
    assert_eq!(parse_ip_discovery_response(&packet, SSRC).unwrap(), None);
    // not 74 bytes
    assert_eq!(parse_ip_discovery_response(&[0u8; 73], SSRC).unwrap(), None);
}

#[test]
fn test_packet_classification() {
    assert!(is_rtp_packet(&rtp_packet(120, 1)));
    assert!(
        is_rtp_packet(&rtp_packet(0xF8, 1)),
        "payload type 120 with the marker bit"
    );
    assert!(!is_rtcp_packet(&rtp_packet(120, 1)));
    assert!(is_rtcp_packet(&rtcp_packet()));
    assert!(!is_rtp_packet(&rtcp_packet()));
    for packet_type in [200u8, 201, 205, 206] {
        let mut packet = rtcp_packet();
        packet[1] = packet_type;
        assert!(is_rtcp_packet(&packet), "RTCP type {packet_type}");
    }
    // version bits wrong, or too short
    assert!(!is_rtp_packet(&[0x40; 16]) && !is_rtcp_packet(&[0x40; 16]));
    assert!(!is_rtp_packet(&[0x80; 7]) && !is_rtcp_packet(&[0x80; 7]));
}

#[test]
fn test_native_ping_packets() {
    let request = native_ping_request(0x0000_0102);
    assert_eq!(request[..4], 0xCAFE_1337u32.to_le_bytes());
    assert_eq!(request[4..], 0x0000_0102u32.to_le_bytes());
    assert_eq!(
        native_ping_response_sequence(&request),
        None,
        "a request is not a response"
    );

    let mut response = request;
    response[..4].copy_from_slice(&0xF00D_1337u32.to_le_bytes());
    assert_eq!(native_ping_response_sequence(&response), Some(0x102));
    assert!(is_native_control_packet(&response));
    assert!(is_native_control_packet(&discovery_response(
        SSRC, "1.2.3.4", 5
    )));
    assert!(!is_native_control_packet(&rtp_packet(120, 1)));
}

#[test]
fn test_send_packet_reaches_the_server_and_updates_stats() {
    let (server, addr) = fake_server();
    let transport = connected_transport(addr);

    transport.send_packet(b"hello").unwrap();
    let (sent, octets) = transport.send_packets(&[&b"ab"[..], &b"cde"[..]]).unwrap();
    assert_eq!((sent, octets), (2, 5));

    let mut buffer = [0u8; 64];
    let mut seen = Vec::new();
    for _ in 0..3 {
        let (length, client) = server.recv_from(&mut buffer).unwrap();
        assert_eq!(client.port(), transport.local_addr().unwrap().port());
        seen.push(buffer[..length].to_vec());
    }
    assert_eq!(seen, [b"hello".to_vec(), b"ab".to_vec(), b"cde".to_vec()]);

    let stats = transport.stats();
    assert_eq!((stats.packets_sent, stats.octets_sent), (3, 10));
}

#[test]
fn test_send_before_connect_fails() {
    let transport = UdpTransport::new(None, None).unwrap();
    assert!(transport.send_packet(b"x").is_err());
}

#[test]
fn test_listeners_receive_only_their_packet_kind() {
    let (server, addr) = fake_server();
    let transport = connected_transport(addr);
    // learn the client address: the server needs it to send back
    transport.send_packet(b"hi").unwrap();
    let mut buffer = [0u8; 16];
    let (_, client) = server.recv_from(&mut buffer).unwrap();

    let rtp_seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let rtcp_seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let batches = Arc::new(Mutex::new(Vec::<usize>::new()));
    let rtp_id = {
        let seen = Arc::clone(&rtp_seen);
        transport.add_listener(
            move |packet| seen.lock().unwrap().push(packet.to_vec()),
            RTP,
        )
    };
    {
        let seen = Arc::clone(&rtcp_seen);
        transport.add_listener(
            move |packet| seen.lock().unwrap().push(packet.to_vec()),
            RTCP,
        );
    }
    {
        let batches = Arc::clone(&batches);
        transport.add_batch_listener(
            move |packets| batches.lock().unwrap().push(packets.len()),
            ListenerKinds {
                rtp: true,
                rtcp: true,
            },
        );
    }

    let ping_response = {
        let mut packet = native_ping_request(9);
        packet[..4].copy_from_slice(&0xF00D_1337u32.to_le_bytes());
        packet
    };
    server.send_to(&rtp_packet(120, 1), client).unwrap();
    server.send_to(&rtcp_packet(), client).unwrap();
    server.send_to(&ping_response, client).unwrap(); // control: never reaches listeners
    server.send_to(&[0x01, 0x02, 0x03], client).unwrap(); // junk: counted invalid
    server.send_to(&rtp_packet(120, 2), client).unwrap();

    wait_for(|| rtp_seen.lock().unwrap().len() == 2 && rtcp_seen.lock().unwrap().len() == 1);
    assert_eq!(rtp_seen.lock().unwrap()[0], rtp_packet(120, 1));
    assert_eq!(rtp_seen.lock().unwrap()[1], rtp_packet(120, 2));
    assert_eq!(rtcp_seen.lock().unwrap()[0], rtcp_packet());
    wait_for(|| batches.lock().unwrap().iter().sum::<usize>() == 3);

    let stats = transport.stats();
    assert_eq!(stats.packets_received, 5);
    assert_eq!(stats.rtp_packets_received, 2);
    assert_eq!(stats.rtcp_packets_received, 1);
    assert_eq!(stats.control_packets_received, 1);
    assert_eq!(stats.invalid_packets_received, 1);
    assert_eq!(stats.callback_errors, 0);

    // removed listeners stop receiving
    transport.remove_listener(rtp_id);
    server.send_to(&rtp_packet(120, 3), client).unwrap();
    wait_for(|| transport.stats().rtp_packets_received == 3);
    thread::sleep(Duration::from_millis(50));
    assert_eq!(rtp_seen.lock().unwrap().len(), 2);
}

#[test]
fn test_a_panicking_listener_is_counted_and_does_not_stop_dispatch() {
    let (server, addr) = fake_server();
    let transport = connected_transport(addr);
    transport.send_packet(b"hi").unwrap();
    let mut buffer = [0u8; 16];
    let (_, client) = server.recv_from(&mut buffer).unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    transport.add_listener(|_| panic!("listener failure"), RTP);
    {
        let calls = Arc::clone(&calls);
        transport.add_listener(
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            },
            RTP,
        );
    }
    server.send_to(&rtp_packet(120, 1), client).unwrap();
    server.send_to(&rtp_packet(120, 2), client).unwrap();
    wait_for(|| calls.load(Ordering::SeqCst) == 2);
    let stats = transport.stats();
    assert_eq!(stats.callback_errors, 2);
}

#[test]
fn test_ping_measures_round_trip_and_counts_timeouts() {
    let (server, addr) = fake_server();
    let transport = connected_transport(addr);
    // The timeout is only evaluated when the next request goes out, so it must not exceed the
    // interval for a lost request to be counted (as in the original).
    transport.start_ping(
        Duration::from_millis(10),
        Duration::from_millis(60),
        Duration::from_millis(50),
    );

    // answer the first request with a response carrying the same sequence
    let mut buffer = [0u8; 64];
    let (length, client) = server.recv_from(&mut buffer).unwrap();
    assert_eq!(length, 8);
    assert_eq!(buffer[..4], 0xCAFE_1337u32.to_le_bytes());
    let sequence = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
    assert_eq!(sequence, 1);
    let mut response = buffer[..8].to_vec();
    response[..4].copy_from_slice(&0xF00D_1337u32.to_le_bytes());
    server.send_to(&response, client).unwrap();
    wait_for(|| transport.ping_rtt_ms().is_some());
    assert!(transport.ping_rtt_ms().unwrap() >= 0.0);
    assert_eq!(transport.ping_send_errors(), 0);

    // stop answering: pending requests time out
    wait_for(|| transport.ping_timeouts() >= 1);
    transport.stop_ping();
    assert_eq!(
        transport.ping_rtt_ms(),
        None,
        "stopped ping reports nothing"
    );
}

#[test]
fn test_close_stops_threads_and_rejects_further_use() {
    let (_server, addr) = fake_server();
    let transport = connected_transport(addr);
    transport.add_listener(|_| {}, RTP);
    transport.start_ping(
        Duration::from_millis(5),
        Duration::from_millis(10),
        Duration::from_millis(50),
    );
    transport.close();
    assert_eq!(
        transport.send_packet(b"x").unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        transport.connect("127.0.0.1", 9).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        transport
            .discover_ip(SSRC, Duration::from_millis(10))
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    // dropping after close (and dropping without close, below) must not hang
    drop(transport);
    let transport = connected_transport(addr);
    transport.add_batch_listener(|_| {}, RTP);
    drop(transport);
}
