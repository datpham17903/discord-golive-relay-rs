"""Generate relay-voice cross-check fixtures from the compiled discord-native-voice module.

Run once (outputs are committed):
    $PY/.venv/Scripts/python.exe -I tools/gen_voice_fixtures.py

For every codec it sends the T4 keyframe + delta AU (1280x720) through the native
`VideoSendPipeline` (AES-256-GCM transport crypto, fixed key) over a loopback UDP
socket and records the encrypted wire packets, what the native `TransportCrypto`
decrypts them to, what the native depacketizer rebuilds, and the RTX/NACK
retransmissions. It also records deterministic transport-crypto vectors for both
modes, RTCP decryption, and `strip_h264_filler_nalus` cases.

The Rust tests (crates/relay-voice/tests/native_crosscheck.rs) replay these files; CI
does not run this script. Needs the reference venv (the compiled module lives there).
"""

from __future__ import annotations

import hashlib
import json
import socket
from pathlib import Path

from discord.ext.native_voice import _native_voice as nv

ROOT = Path(__file__).resolve().parent.parent
CODEC_FIX = ROOT / "crates" / "relay-media" / "tests" / "fixtures" / "codecs"
OUT = ROOT / "crates" / "relay-voice" / "tests" / "fixtures" / "rtp"

KEY = bytes(range(32))
MODES = ("aead_aes256_gcm_rtpsize", "aead_xchacha20_poly1305_rtpsize")
MEDIA_SSRC = 0x11223344
RTX_SSRC = 0x55667788
RID = "100"
STREAM_TYPE = "screen"
FRAME_MS = 1000 / 30
# (file prefix, native name, payload type) -- payload types from the voice codec table.
CODECS = [
    ("h264", "H264", 105),
    ("h265", "H265", 103),
    ("av1", "AV1", 101),
    ("vp8", "VP8", 107),
    ("vp9", "VP9", 109),
]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def recv_exact(rx: socket.socket, count: int) -> list[bytes]:
    packets: list[bytes] = []
    while len(packets) < count:
        packets.append(rx.recv(4096))
    return packets


def decrypted_json(crypto, packets: list[bytes]):
    failed, decoded = crypto.decrypt_rtp_packets(packets)
    assert failed == 0, failed
    rows = []
    for index, pt, marker, padded, seq, ts, ssrc, extended, ext_payload, exts, media in decoded:
        rows.append(
            {
                "index": index,
                "payload_type": pt,
                "marker": marker,
                "padded": padded,
                "sequence": seq,
                "timestamp": ts,
                "ssrc": ssrc,
                "extended": extended,
                "media_sha256": sha256(media),
                "media_len": len(media),
            }
        )
        if index == 0:
            # Extension blocks only differ in transport sequence and abs-send-time; keep the first
            # packet's full list and the transport sequence of every packet.
            rows[-1]["extensions"] = [[ext_id, data.hex()] for ext_id, data in exts]
        rows[-1]["transport_sequence"] = int.from_bytes(dict(exts)[5], "big")
    return rows, decoded


def codec_fixture(prefix: str, native: str, payload_type: int) -> tuple[dict, bytes]:
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
    rx.bind(("127.0.0.1", 0))
    rx.settimeout(2.0)
    tx = nv.NativeUDPTransport(None, None)
    tx.connect("127.0.0.1", rx.getsockname()[1])
    crypto = nv.TransportCrypto(MODES[0], KEY)
    pipe = nv.VideoSendPipeline()
    pipe.configure_stream(native, MEDIA_SSRC, payload_type, payload_type + 1, RTX_SSRC, RID)
    depacketizer = getattr(nv, native + "Depacketizer")()

    wire: list[bytes] = []
    frames = []
    first_media_seq = None
    for kind in ("key", "delta"):
        au_file = f"{prefix}_1280x720_{kind}.bin"
        au = (CODEC_FIX / au_file).read_bytes()
        sent, octets, transport_seq, rtp_ts, last_seq = pipe.send_video_frame(
            crypto, tx.fileno(), MEDIA_SSRC, au, None, FRAME_MS, STREAM_TYPE, 0, 0.0
        )
        packets = recv_exact(rx, sent)
        rows, decoded = decrypted_json(crypto, packets)
        rebuilt = None
        for row, item in zip(rows, decoded):
            out = depacketizer.push_packet(item[10], item[2], item[4], item[5])
            if out is not None:
                assert row is rows[-1]
                rebuilt = out
        assert rebuilt is not None
        if first_media_seq is None:
            first_media_seq = rows[0]["sequence"]
        frames.append(
            {
                "au_file": au_file,
                "packet_count": sent,
                "octets": octets,
                "transport_sequence": transport_seq,
                "rtp_timestamp": rtp_ts,
                "last_sequence": last_seq,
                "packets": rows,
                "depacketized_len": len(rebuilt),
                "depacketized_sha256": sha256(rebuilt),
                "depacketized_equals_input": rebuilt == au,
            }
        )
        wire.extend(packets)

    # RTX: single packet for the 2nd media packet, then a NACK for seq0 + BLP bits {0, 2}
    # (resends seq0, seq0+1, seq0+3).
    ok, rtx_ssrc, rtx_seq, rtx_transport = pipe.send_video_rtx_packet(
        crypto, tx.fileno(), MEDIA_SSRC, first_media_seq + 1, STREAM_TYPE
    )
    assert ok
    rtx_single = recv_exact(rx, 1)
    rtx_single_rows, _ = decrypted_json(crypto, rtx_single)

    nack_payload = (
        MEDIA_SSRC.to_bytes(4, "big")
        + first_media_seq.to_bytes(2, "big")
        + (0b101).to_bytes(2, "big")
    )
    resent, nack_media, nack_rtx_ssrc, nack_last_rtx_seq, nack_transport = pipe.send_video_rtx_for_nack(
        crypto, tx.fileno(), nack_payload, STREAM_TYPE, 0, 0.0
    )
    nack_packets = recv_exact(rx, resent)
    nack_rows, _ = decrypted_json(crypto, nack_packets)
    wire.extend(rtx_single)
    wire.extend(nack_packets)

    entry = {
        "codec": native,
        "payload_type": payload_type,
        "rtx_payload_type": payload_type + 1,
        "media_ssrc": MEDIA_SSRC,
        "rtx_ssrc": RTX_SSRC,
        "rid": RID,
        "media_stream_type": STREAM_TYPE,
        "frame_time_ms": FRAME_MS,
        "wire_file": f"{prefix}.bin",
        "frames": frames,
        "rtx_single": {
            "original_sequence": first_media_seq + 1,
            "rtx_sequence": rtx_seq,
            "transport_sequence": rtx_transport,
            "packets": rtx_single_rows,
        },
        "nack": {
            "payload_hex": nack_payload.hex(),
            "resent": resent,
            "last_rtx_sequence": nack_last_rtx_seq,
            "transport_sequence": nack_transport,
            "original_sequences": [first_media_seq, first_media_seq + 1, first_media_seq + 3],
            "packets": nack_rows,
        },
    }
    tx.close()
    rx.close()
    blob = b"".join(len(p).to_bytes(2, "big") + p for p in wire)
    return entry, blob


def crypto_vectors() -> dict:
    out = {}
    header = bytes([0x80, 0x78, 0x12, 0x34, 0, 0, 0x03, 0xE8, 0xDE, 0xAD, 0xBE, 0xEF])
    for mode in MODES:
        crypto = nv.TransportCrypto(mode, KEY)
        vectors = []
        for payload in (b"", b"opus-frame", bytes(range(200))):
            wire = crypto.encrypt_rtp(header, payload)
            vectors.append(
                {"header_hex": header.hex(), "payload_hex": payload.hex(), "wire_hex": wire.hex()}
            )
        out[mode] = vectors
    return out


def rtcp_vectors() -> dict:
    """NACK feedback + a trailing receiver report in one compound packet."""
    out = {}
    for mode in MODES:
        crypto = nv.TransportCrypto(mode, KEY)
        sender = 0x01020304
        media = MEDIA_SSRC
        nack_body = media.to_bytes(4, "big") + (1000).to_bytes(2, "big") + (0b11).to_bytes(2, "big")
        second = bytes([0x80, 201, 0, 1]) + (0x0A0B0C0D).to_bytes(4, "big")
        total_words = (8 + len(nack_body)) // 4 - 1
        header = bytes([0x81, 205]) + total_words.to_bytes(2, "big") + sender.to_bytes(4, "big")
        wire = crypto.encrypt_rtcp(header, nack_body + second)
        packets = crypto.decrypt_rtcp_packets(wire, 8)
        out[mode] = {
            "header_hex": header.hex(),
            "plaintext_hex": (nack_body + second).hex(),
            "wire_hex": wire.hex(),
            "packets": [
                {"packet_type": t, "count": c, "sender_ssrc": s, "payload_hex": p.hex()}
                for (t, c, s, p) in packets
            ],
        }
    return out


def strip_filler_vectors() -> list[dict]:
    sc4 = b"\x00\x00\x00\x01"
    sc3 = b"\x00\x00\x01"
    sps = b"\x67\x42\xc0\x1f\xaa"
    filler = b"\x0c\xff\xff\xff"
    idr = b"\x65\x88\x84\x00\x10"
    cases = {
        "filler_between": sc4 + sps + sc4 + filler + sc4 + idr,
        "filler_three_byte_start": sc4 + sps + sc3 + filler + sc3 + idr,
        "no_filler": sc4 + sps + sc4 + idr,
        "no_start_code": b"\x65\x88\x84\x00\x10",
        "only_filler": sc4 + filler,
        "empty": b"",
    }
    return [
        {"name": name, "input_hex": data.hex(), "output_hex": nv.strip_h264_filler_nalus(data).hex()}
        for name, data in cases.items()
    ]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source": "discord.ext.native_voice._native_voice (compiled module in the reference venv)",
        "mode": MODES[0],
        "key_hex": KEY.hex(),
        "codecs": {},
        "crypto": crypto_vectors(),
        "rtcp": rtcp_vectors(),
        "strip_h264_filler": strip_filler_vectors(),
    }
    for prefix, native, payload_type in CODECS:
        entry, blob = codec_fixture(prefix, native, payload_type)
        (OUT / entry["wire_file"]).write_bytes(blob)
        manifest["codecs"][prefix] = entry
        print(prefix, sum(f["packet_count"] for f in entry["frames"]), "media packets,", len(blob), "bytes")
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n", encoding="utf8")


if __name__ == "__main__":
    main()
