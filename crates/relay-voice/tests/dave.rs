//! DAVE loopback: two `Dave` sessions and a fake voice gateway (MLS external sender) exchange the
//! op 25-30 binary messages, execute the transition and then encrypt/decrypt each other's frames.

mod common;

use common::mls::{FakeExternalSender, split_commit_welcome};
use relay_media::codec::VoiceCodec;
use relay_voice::dave::{Dave, DaveMedia, DaveOut};
use relay_voice::voice_ws::op;

const CHANNEL: u64 = 1_234_567_890;
const USER_A: u64 = 1_234_123_412_341_234;
const USER_B: u64 = 5_678_567_856_785_678;
const USER_C: u64 = 9_012_901_290_129_012;

fn key_package(outs: &[DaveOut]) -> Vec<u8> {
    match outs {
        [DaveOut::KeyPackage(kp)] => kp.clone(),
        other => panic!("expected a key package, got {other:?}"),
    }
}

/// `seq` is not part of what `on_binary` takes: the websocket strips `seq` and the opcode.
fn with_transition(transition_id: u16, data: &[u8]) -> Vec<u8> {
    let mut out = transition_id.to_be_bytes().to_vec();
    out.extend_from_slice(data);
    out
}

/// Both users joined channel `CHANNEL` through the fake gateway; transition `transition_id` executed.
fn establish(transition_id: u16) -> (Dave, Dave) {
    let (_, a, b) = establish_with_gateway(transition_id);
    (a, b)
}

/// [`establish`], also returning the fake gateway so more members can join later.
fn establish_with_gateway(transition_id: u16) -> (FakeExternalSender, Dave, Dave) {
    let sender = FakeExternalSender::new(CHANNEL);
    let a = Dave::new(USER_A, CHANNEL);
    let b = Dave::new(USER_B, CHANNEL);

    // SESSION_DESCRIPTION with dave_protocol_version 1: each side answers with its key package.
    let _kp_a = key_package(&a.on_session_description(1).unwrap());
    let kp_b = key_package(&b.on_session_description(1).unwrap());
    assert!(a.has_session() && b.has_session());
    assert!(!a.can_encrypt() && !b.can_encrypt());

    for d in [&a, &b] {
        assert!(
            d.on_binary(op::MLS_EXTERNAL_SENDER, &sender.marshalled())
                .unwrap()
                .is_empty()
        );
    }

    // The gateway proposes adding B to A's group; A answers with commit + welcome (op 28).
    let mut proposals = vec![0u8]; // append
    proposals.extend_from_slice(&sender.propose_add(0, &kp_b));
    let outs = a.on_binary(op::MLS_PROPOSALS, &proposals).unwrap();
    let [DaveOut::CommitWelcome(payload)] = outs.as_slice() else {
        panic!("expected commit+welcome, got {outs:?}");
    };
    let (commit, welcome) = split_commit_welcome(payload);
    assert!(!welcome.is_empty());

    // The gateway announces the commit to A and sends the welcome to B.
    let ready = |outs: Vec<DaveOut>| {
        if transition_id == 0 {
            assert!(outs.is_empty());
        } else {
            assert_eq!(outs, vec![DaveOut::TransitionReady(transition_id)]);
        }
    };
    ready(
        a.on_binary(
            op::MLS_ANNOUNCE_COMMIT_TRANSITION,
            &with_transition(transition_id, &commit),
        )
        .unwrap(),
    );
    ready(
        b.on_binary(op::MLS_WELCOME, &with_transition(transition_id, &welcome))
            .unwrap(),
    );

    if transition_id != 0 {
        // davey 0.2: keys only switch on execute_transition.
        assert!(!a.can_encrypt() && !b.can_encrypt());
        a.on_execute_transition(transition_id).unwrap();
        b.on_execute_transition(transition_id).unwrap();
    }
    (sender, a, b)
}

#[test]
fn two_sessions_encrypt_and_decrypt_after_transition() {
    let (a, b) = establish(7);
    assert!(a.can_encrypt() && b.can_encrypt());
    assert!(a.can_decrypt(USER_B) && b.can_decrypt(USER_A));
    assert_eq!(a.protocol_version(), 1);

    let opus = [0x78, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    let sealed = a.encrypt_opus(&opus).unwrap();
    assert_ne!(sealed.as_ref(), &opus[..]);
    assert_eq!(
        b.decrypt(USER_A, DaveMedia::Audio, &sealed).unwrap(),
        opus.to_vec()
    );

    // A video frame in the layout the H.264 payloader sends (start code, SPS-like and IDR NALs).
    let frame: Vec<u8> = [
        &[0, 0, 0, 1, 0x67, 0x42, 0x00, 0x1f, 0xe9][..],
        &[0, 0, 0, 1, 0x68, 0xce, 0x06, 0xe2][..],
        &[
            0, 0, 0, 1, 0x65, 0x88, 0x84, 0x00, 0x10, 0xff, 0xfe, 0x01, 0x02, 0x03,
        ][..],
    ]
    .concat();
    let sealed = b.encrypt_video(VoiceCodec::H264, &frame).unwrap();
    assert_ne!(sealed.as_ref(), &frame[..]);
    assert_eq!(a.decrypt(USER_B, DaveMedia::Video, &sealed).unwrap(), frame);
    assert_eq!(a.decrypt_failures(), 0);
    assert_eq!(b.decrypt_failures(), 0);
}

/// Go Live late join: viewer C joins the running group of sender A (with B, epoch 1). C decrypts
/// A's frames as soon as A executes the transition (op 22). Frames A sealed before that use the
/// epoch-1 key, which C never gets: they fail as retryable forever, so a receiver that keeps
/// retrying them counts a failure per retry until they expire.
#[test]
fn late_joining_viewer_decrypts_once_the_sender_executes_the_transition() {
    let (gateway, a, b) = establish_with_gateway(7);
    let c = Dave::new(USER_C, CHANNEL);
    let kp_c = key_package(&c.on_session_description(1).unwrap());
    assert!(
        c.on_binary(op::MLS_EXTERNAL_SENDER, &gateway.marshalled())
            .unwrap()
            .is_empty()
    );

    // Every member gets the add proposal; the gateway announces A's commit, B's loses.
    let mut proposals = vec![0u8]; // append
    proposals.extend_from_slice(&gateway.propose_add(1, &kp_c));
    let outs = a.on_binary(op::MLS_PROPOSALS, &proposals).unwrap();
    let [DaveOut::CommitWelcome(payload)] = outs.as_slice() else {
        panic!("expected commit+welcome, got {outs:?}");
    };
    assert!(matches!(
        b.on_binary(op::MLS_PROPOSALS, &proposals)
            .unwrap()
            .as_slice(),
        [DaveOut::CommitWelcome(_)]
    ));
    let (commit, welcome) = split_commit_welcome(payload);
    let announce = with_transition(8, &commit);
    for d in [&a, &b] {
        assert_eq!(
            d.on_binary(op::MLS_ANNOUNCE_COMMIT_TRANSITION, &announce)
                .unwrap(),
            vec![DaveOut::TransitionReady(8)]
        );
    }
    assert_eq!(
        c.on_binary(op::MLS_WELCOME, &with_transition(8, &welcome))
            .unwrap(),
        vec![DaveOut::TransitionReady(8)]
    );

    let frame = [0x41u8, 9, 8, 7, 6, 5, 4, 3, 2, 1];
    // Before op 22 A still seals with the epoch-1 key: B decrypts, C cannot.
    let old = a
        .encrypt_video(VoiceCodec::VP8, &frame)
        .unwrap()
        .into_owned();
    assert_eq!(b.decrypt(USER_A, DaveMedia::Video, &old).unwrap(), frame);
    let err = c.decrypt(USER_A, DaveMedia::Video, &old).unwrap_err();
    assert!(err.is_retryable(), "{err}");

    // op 22 on the sender is enough: C installed A's epoch-2 key when it prepared the welcome.
    a.on_execute_transition(8).unwrap();
    let new = a
        .encrypt_video(VoiceCodec::VP8, &frame)
        .unwrap()
        .into_owned();
    assert_eq!(c.decrypt(USER_A, DaveMedia::Video, &new).unwrap(), frame);
    assert_eq!(b.decrypt(USER_A, DaveMedia::Video, &new).unwrap(), frame);
    for d in [&b, &c] {
        d.on_execute_transition(8).unwrap();
    }
    let sealed = c.encrypt_opus(&[0x78, 1, 2, 3, 4, 5]).unwrap().into_owned();
    assert_eq!(
        a.decrypt(USER_C, DaveMedia::Audio, &sealed).unwrap(),
        vec![0x78, 1, 2, 3, 4, 5]
    );

    // The pre-transition frame never becomes decryptable for C.
    assert!(c.decrypt(USER_A, DaveMedia::Video, &old).is_err());
    assert_eq!(c.decrypt_failures(), 2);
    assert_eq!(b.decrypt_failures(), 0);
}

#[test]
fn transition_zero_applies_immediately() {
    let (a, b) = establish(0);
    assert!(a.can_encrypt() && b.can_encrypt());
    let frame = [0x41u8, 9, 8, 7, 6, 5, 4, 3, 2, 1];
    let sealed = a.encrypt_video(VoiceCodec::VP8, &frame).unwrap();
    assert_eq!(
        b.decrypt(USER_A, DaveMedia::Video, &sealed).unwrap(),
        frame.to_vec()
    );
}

#[test]
fn encrypt_is_a_no_op_without_a_session() {
    let d = Dave::new(USER_A, CHANNEL);
    assert!(!d.can_encrypt());
    let packet = [1u8, 2, 3, 4];
    assert_eq!(d.encrypt_opus(&packet).unwrap().as_ref(), &packet[..]);
    assert_eq!(
        d.encrypt_video(VoiceCodec::H264, &packet).unwrap().as_ref(),
        &packet[..]
    );
    let err = d.decrypt(USER_B, DaveMedia::Audio, &packet).unwrap_err();
    assert!(!err.is_retryable());
}

#[test]
fn decrypt_failures_are_counted_and_unknown_users_are_retryable() {
    let (a, _b) = establish(3);
    // Nothing from this user has ever been seen: the key may still arrive.
    let err = a.decrypt(42, DaveMedia::Video, &[1, 2, 3, 4]).unwrap_err();
    assert!(err.is_retryable(), "{err}");
    assert_eq!(a.decrypt_failures(), 1);

    // A frame that is not DAVE-framed while passthrough is off is a hard failure.
    let err = a
        .decrypt(USER_B, DaveMedia::Video, &[1, 2, 3, 4])
        .unwrap_err();
    assert!(!err.is_retryable(), "{err}");
    assert_eq!(a.decrypt_failures(), 2);
}

#[test]
fn downgrade_enables_passthrough_then_stops_encrypting() {
    let (a, b) = establish(1);

    // op 21 PREPARE_TRANSITION to version 0: answer ready, accept plaintext from the peer.
    assert_eq!(
        b.on_prepare_transition(9, 0).unwrap(),
        vec![DaveOut::TransitionReady(9)]
    );
    assert!(
        b.can_encrypt(),
        "still encrypting until the transition executes"
    );
    b.on_execute_transition(9).unwrap();
    assert_eq!(b.protocol_version(), 0);
    assert!(!b.can_encrypt());

    let plain = [0xAAu8, 0xBB, 0xCC, 0xDD, 0xEE];
    assert_eq!(b.encrypt_opus(&plain).unwrap().as_ref(), &plain[..]);
    assert!(b.can_decrypt(USER_A), "passthrough from the downgrade");
    assert_eq!(
        b.decrypt(USER_A, DaveMedia::Video, &plain).unwrap(),
        plain.to_vec()
    );
    drop(a);
}

#[test]
fn invalid_commit_reports_op31_and_sends_a_fresh_key_package() {
    let (a, _b) = establish(2);
    let outs = a
        .on_binary(
            op::MLS_ANNOUNCE_COMMIT_TRANSITION,
            &with_transition(11, b"garbage"),
        )
        .unwrap();
    match outs.as_slice() {
        [DaveOut::InvalidCommitWelcome(11), DaveOut::KeyPackage(kp)] => assert!(!kp.is_empty()),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn prepare_epoch_one_reinitializes_and_other_epochs_are_ignored() {
    let (a, _b) = establish(5);
    assert!(a.on_prepare_epoch(2, 1).unwrap().is_empty());
    let outs = a.on_prepare_epoch(1, 1).unwrap();
    assert!(matches!(outs.as_slice(), [DaveOut::KeyPackage(_)]));
    // davey 0.2 keeps the keys through a reinit, so media keeps flowing while the new group forms.
    assert!(a.can_encrypt());
}

#[test]
fn binary_ops_are_ignored_without_a_session() {
    let d = Dave::new(USER_A, CHANNEL);
    assert!(
        d.on_binary(op::MLS_EXTERNAL_SENDER, b"x")
            .unwrap()
            .is_empty()
    );
    assert!(d.on_prepare_transition(1, 1).unwrap().is_empty());
    assert!(d.on_prepare_epoch(1, 1).unwrap().is_empty());
    // Version 0 in SESSION_DESCRIPTION: no session is created.
    assert!(d.on_session_description(0).unwrap().is_empty());
    assert!(!d.has_session());
}

/// Why the Python stack pinned davey 0.1.7: 0.1.x installed keys inside `process_commit` /
/// `process_welcome`; 0.2.0 only does so after `prepare_transition`. A caller that never prepares
/// (as `discord.py-self` / `discord-native-voice` do) is left with no decryptors and no encryptor.
#[test]
fn davey_020_installs_keys_only_after_prepare_transition() {
    use davey::{DaveSession, MediaType, ProposalsOperationType};
    use std::num::NonZeroU16;

    let sender = FakeExternalSender::new(CHANNEL);
    let version = NonZeroU16::new(1).unwrap();
    let mut a = DaveSession::new(version, USER_A, CHANNEL, None).unwrap();
    let mut b = DaveSession::new(version, USER_B, CHANNEL, None).unwrap();
    a.set_external_sender(&sender.marshalled()).unwrap();
    b.set_external_sender(&sender.marshalled()).unwrap();
    let kp_b = b.create_key_package().unwrap();
    let cw = a
        .process_proposals(
            ProposalsOperationType::APPEND,
            &sender.propose_add(0, &kp_b),
            None,
        )
        .unwrap()
        .unwrap();
    assert!(a.process_commit(&cw.commit).unwrap());
    b.process_welcome(&cw.welcome.unwrap(), None).unwrap();

    assert!(!a.is_ready() && !b.is_ready());
    assert!(!a.can_encrypt() && !b.can_encrypt());
    assert!(matches!(
        b.decrypt(USER_A, MediaType::AUDIO, &[0u8; 32]),
        Err(davey::errors::DecryptError::NoDecryptorForUser)
    ));

    a.prepare_transition(0, 1).unwrap();
    b.prepare_transition(0, 1).unwrap();
    assert!(a.can_encrypt() && b.can_encrypt());
    let sealed = a.encrypt_opus(&[1, 2, 3, 4]).unwrap();
    assert_eq!(
        b.decrypt(USER_A, MediaType::AUDIO, &sealed).unwrap(),
        vec![1, 2, 3, 4]
    );
}

/// A DAVE frame held back for a key that never comes is retried at every batch; its decrypt failure
/// counts once, and its expiry is counted on its own.
#[test]
fn held_back_video_frame_counts_one_decrypt_failure() {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use common::CollectSink;
    use relay_voice::receive::{ReceiveConfig, Receiver, SsrcMap, VideoState};
    use relay_voice::rtp::RtpPacket;
    use relay_voice::transport::TransportEncryptionMode;
    use relay_voice::transport::packets::SharedTransportCrypto;
    use relay_voice::voice_ws::video_codec_list;

    const STRANGER: u64 = 42;
    const SSRC: u32 = 5000;
    let crypto = || {
        SharedTransportCrypto::new(TransportEncryptionMode::AeadAes256GcmRtpSize, &[7; 32]).unwrap()
    };
    let (a, _b) = establish(3);
    let dave = Arc::new(a);
    let mut cfg = ReceiveConfig::new(video_codec_list(&[VoiceCodec::H264], false), 1);
    cfg.debug_stats = true;
    let ssrcs = Arc::new(SsrcMap::new());
    ssrcs.update_video_state(
        STRANGER,
        VideoState {
            audio_ssrc: None,
            video_ssrc: Some(SSRC),
            rtx_ssrc: None,
            streams: Vec::new(),
        },
    );
    let rx = Receiver::new(
        cfg,
        crypto(),
        crypto(),
        Arc::new(CollectSink::default()),
        Arc::clone(&dave),
        ssrcs,
    );

    // an IDR NAL with a well-formed DAVE trailer, from a user whose key never arrives
    let mut nal = vec![0x65, 0x88, 0x84, 1];
    nal.extend_from_slice(&[0xAA; 8]); // tag
    nal.push(0x05); // nonce
    nal.push(12); // supplemental size
    nal.extend_from_slice(&[0xFA, 0xFA]);
    let header = RtpPacket::new_header(105, 1, 1000, SSRC, true, false);
    let packet = crypto().encrypt_rtp(&header, &nal).unwrap();
    assert!(rx.process_batch(&[packet], Instant::now()).is_empty());
    assert_eq!(dave.decrypt_failures(), 1);

    for _ in 0..20 {
        assert!(rx.process_batch(&[], Instant::now()).is_empty());
    }
    assert_eq!(dave.decrypt_failures(), 1, "retries are not new failures");
    assert!(!rx.stats().contains_key("dave_pending_expired"));

    let later = Instant::now() + Duration::from_secs(11);
    assert!(rx.process_batch(&[], later).is_empty());
    assert_eq!(rx.stats()["dave_pending_expired"], 1);
    assert!(rx.process_batch(&[], later).is_empty());
    assert_eq!(rx.stats()["dave_pending_expired"], 1);
    assert_eq!(dave.decrypt_failures(), 1);
}
