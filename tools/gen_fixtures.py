"""Generate binary test fixtures from the Python reference implementation.

Run once (outputs are committed):
    $PY/.venv/Scripts/python.exe tools/gen_fixtures.py [m1] [opus_table] [t4]   (default: t4)

Sections are appended per task (M1 h264/opus, T4 per-codec AUs + Opus packets). Requires PyAV from the reference venv.
"""

from __future__ import annotations

import json
import math
import random
import struct
import sys
from fractions import Fraction
from pathlib import Path

PY_SRC = Path(r"D:\Project\Discord-Relay-Bot\src")
sys.path.insert(0, str(PY_SRC))

import av  # noqa: E402

from golive_relay.media.h264 import (  # noqa: E402
    NAL_SPS,
    _BitWriter,
    _escape,
    _parse_sps_full,
    _unescape,
    ensure_low_delay_vui,
    is_idr,
    nal_units,
)

ROOT = Path(__file__).resolve().parent.parent
MEDIA_FIX = ROOT / "crates" / "relay-media" / "tests" / "fixtures"
# T4: codec/opus fixtures shared by relay-media, relay-core and relay-voice tests
# (relay-core reads them from here instead of keeping a second copy).
CODEC_FIX = MEDIA_FIX / "codecs"


def h264_encode(n: int = 40, keyint: int = 10, w: int = 64, h: int = 64) -> list[bytes]:
    # Same encoder settings as $PY/tests/media/test_h264.py::_encode.
    enc = av.CodecContext.create("libx264", "w")
    enc.width, enc.height, enc.pix_fmt = w, h, "yuv420p"
    enc.time_base = Fraction(1, 30)
    enc.framerate = Fraction(30, 1)
    enc.options = {"tune": "zerolatency", "bf": "0", "scenecut": "0",
                   "x264-params": f"keyint={keyint}:min-keyint={keyint}"}
    enc.open()
    out = []
    for i in range(n):
        f = av.VideoFrame(w, h, "yuv420p")
        f.pts = i
        out += [bytes(p) for p in enc.encode(f)]
    out += [bytes(p) for p in enc.encode(None)]
    return out


def h264_decode(au: bytes) -> list[bytes]:
    dec = av.CodecContext.create("h264", "r")
    frames = dec.decode(av.Packet(au)) + dec.decode(None)
    return [b"".join(bytes(p) for p in f.planes) for f in frames]


def pack(aus: list[bytes]) -> bytes:
    """u32 big-endian length prefix + bytes, repeated."""
    return b"".join(struct.pack(">I", len(a)) + a for a in aus)


def sps_of(au: bytes) -> bytes:
    return next(n for t, n in nal_units(au) if t == NAL_SPS)


def replace_sps(au: bytes, rbsp: bytes) -> bytes:
    old = sps_of(au)
    i = au.find(old)
    return au[:i] + old[:1] + _escape(rbsp) + au[i + len(old):]


def strip_vui(au: bytes, drop_vui: bool) -> bytes:
    sps = sps_of(au)
    info = _parse_sps_full(sps)
    rbsp = _unescape(sps[1:])
    w = _BitWriter()
    if drop_vui:
        w.copy(rbsp, info.vui_flag_pos)
        w.u(1, 0)
    else:
        w.copy(rbsp, info.restriction_flag_pos)
        w.u(1, 0)
    return replace_sps(au, w.finish())


def gen_m1() -> None:
    MEDIA_FIX.mkdir(parents=True, exist_ok=True)
    stream = h264_encode()
    (MEDIA_FIX / "h264_64x64_keyint10.bin").write_bytes(pack(stream))
    idr_320x240 = next(a for a in h264_encode(n=3, w=320, h=240) if is_idr(a))
    idr_320x180 = next(a for a in h264_encode(n=3, w=320, h=180) if is_idr(a))
    (MEDIA_FIX / "idr_320x240.bin").write_bytes(idr_320x240)
    (MEDIA_FIX / "idr_320x180.bin").write_bytes(idr_320x180)
    for name, drop_vui in (("no_vui", True), ("vui_no_restriction", False)):
        stripped = strip_vui(idr_320x240, drop_vui)
        out = ensure_low_delay_vui(stripped)
        # Python test asserted the rewritten AU decodes identically to the original.
        assert h264_decode(out) == h264_decode(idr_320x240) and len(h264_decode(out)) == 1
        (MEDIA_FIX / f"idr_320x240_{name}.bin").write_bytes(stripped)
        (MEDIA_FIX / f"idr_320x240_{name}.expected.bin").write_bytes(out)


def gen_opus_table() -> None:
    """Every TOC byte (code 3 also with several frame-count bytes) -> Python opus_packet_info result."""
    from golive_relay.media.opus import opus_packet_info

    payloads = [bytes([toc]) for toc in range(256)]
    payloads += [bytes([toc, b]) for toc in range(256) if toc & 3 == 3 for b in (0x00, 0x01, 0x03, 0x3F, 0x40, 0xC2)]
    lines = []
    for p in payloads:
        info = opus_packet_info(p)
        lines.append(p.hex() + ("\t-" if info is None else "\t%r\t%s\t%s\t%d" % (float(info[0]), info[1], info[2], info[3])))
    (MEDIA_FIX / "opus_toc_table.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# T4: per-codec access units + Opus packets.
#
# File formats (all recorded in codecs/manifest.json):
#   h264, h265 : one Annex-B access unit (start-code 00 00 00 01 NALs; the keyframe AU carries
#                VPS/SPS/PPS (SPS/PPS for h264) in-band).
#   av1        : one temporal unit in the low-overhead OBU format (every OBU has obu_has_size_field=1,
#                RTP/Section 5 style). Each TU starts with a temporal-delimiter OBU (12 00); the key TU
#                then has the sequence-header OBU (0a ..) followed by the frame OBU.
#   vp8, vp9   : one raw frame (no IVF/container framing).
#   opus       : one raw Opus packet (TOC byte first), no framing.
# ---------------------------------------------------------------------------
_RAMP = bytes(range(256)) * 16
VIDEO_SIZES = ((1920, 1080), (1280, 720))
VIDEO_CODECS = {
    # manifest codec name -> (encoder, options). Low-delay settings so packet 0 is the keyframe and
    # packet 1 the first delta, with no reordering.
    "h264": ("libx264", {"tune": "zerolatency", "bf": "0", "scenecut": "0",
                         "x264-params": "keyint=30:min-keyint=30"}),
    "h265": ("libx265", {"x265-params": "repeat-headers=1:bframes=0:keyint=30:min-keyint=30:scenecut=0:log-level=none"}),
    "av1": ("libsvtav1", {"preset": "12", "svtav1-params": "pred-struct=1:keyint=30:scd=0"}),
    "vp8": ("libvpx", {"lag-in-frames": "0", "g": "30"}),
    "vp9": ("libvpx-vp9", {"lag-in-frames": "0", "g": "30", "deadline": "realtime", "cpu-used": "8"}),
}


def _fill_plane(plane, w: int, h: int, row) -> None:
    ls = plane.line_size
    buf = bytearray(ls * h)
    for y in range(h):
        buf[y * ls:y * ls + w] = row(y)
    plane.update(bytes(buf))


def _video_frames(w: int, h: int, n: int):
    """Deterministic moving gradient with per-row jitter (compressible but not trivially flat)."""
    rng = random.Random(1)
    offs = [rng.randrange(256) for _ in range(h)]
    for i in range(n):
        f = av.VideoFrame(w, h, "yuv420p")
        _fill_plane(f.planes[0], w, h, lambda y: _RAMP[(offs[y] + y * 2 + i * 8) % 256:][:w])
        _fill_plane(f.planes[1], w // 2, h // 2, lambda y: bytes([90 + i * 10]) * (w // 2))
        _fill_plane(f.planes[2], w // 2, h // 2, lambda y: bytes([160]) * (w // 2))
        f.pts = i
        yield f


def _encode_video(codec: str, w: int, h: int) -> list[tuple[bytes, bool]]:
    """[(access_unit, is_keyframe)] for exactly 1 keyframe + 1 delta."""
    name, opts = VIDEO_CODECS[codec]
    enc = av.CodecContext.create(name, "w")
    enc.width, enc.height, enc.pix_fmt = w, h, "yuv420p"
    enc.time_base = Fraction(1, 30)
    enc.framerate = Fraction(30, 1)
    enc.options = dict(opts)
    enc.open()
    pk: list = []
    for f in _video_frames(w, h, 3):
        pk += enc.encode(f)
    pk += enc.encode(None)
    out = [(bytes(p), bool(p.is_keyframe)) for p in pk[:2]]
    assert len(pk) >= 2 and [k for _, k in out] == [True, False], (codec, w, h, [k for _, k in out])
    return out


def _opus_encode(layout: str, ch: int, dur_ms: int) -> list[bytes]:
    c = av.CodecContext.create("libopus", "w")
    c.sample_rate, c.layout, c.format = 48000, layout, "s16"
    c.time_base = Fraction(1, 48000)
    c.options = {"application": "audio", "frame_duration": str(dur_ms)}
    c.open()
    n = 48 * dur_ms
    pk: list = []
    for i in range(4):
        f = av.AudioFrame(format="s16", layout=layout, samples=n)
        f.sample_rate, f.pts = 48000, i * n
        fmt = "<" + "h" * ch
        f.planes[0].update(b"".join(
            struct.pack(fmt, *([int(8000 * math.sin(2 * math.pi * 440 * (i * n + k) / 48000))] * ch))
            for k in range(n)))
        pk += c.encode(f)
    pk += c.encode(None)
    return [bytes(p) for p in pk]


def gen_t4() -> None:
    from golive_relay.media.opus import opus_packet_info

    CODEC_FIX.mkdir(parents=True, exist_ok=True)
    entries: list[dict] = []

    for codec in VIDEO_CODECS:
        for w, h in VIDEO_SIZES:
            for (au, key), kind in zip(_encode_video(codec, w, h), ("key", "delta")):
                fn = f"{codec}_{w}x{h}_{kind}.bin"
                (CODEC_FIX / fn).write_bytes(au)
                entries.append({"file": fn, "media": "video", "codec": codec, "width": w, "height": h,
                                "keyframe": key, "bytes": len(au)})

    def add_opus(fn: str, pkt: bytes, source: str) -> None:
        info = opus_packet_info(pkt)
        assert info is not None, fn
        (CODEC_FIX / fn).write_bytes(pkt)
        entries.append({"file": fn, "media": "audio", "codec": "opus", "keyframe": False, "bytes": len(pkt),
                        "source": source, "duration_ms": float(info[0]), "channels": info[1],
                        "mode": info[2], "frames": info[3]})

    # Real libopus packets (the second one of each run: the first carries encoder warm-up).
    for layout, ch in (("mono", 1), ("stereo", 2)):
        for dur in (10, 20, 40, 60):
            add_opus(f"opus_{layout}_{dur}ms.bin", _opus_encode(layout, ch, dur)[1], "libopus")
    # Hand-built TOC-only packets (not decodable) for the paths libopus does not emit.
    add_opus("opus_synth_silk_20ms_mono.bin", bytes([0x08]) + bytes(range(1, 40)), "synthetic")  # config 1, code 0
    add_opus("opus_synth_hybrid_20ms_stereo.bin", bytes([0x7C]) + bytes(range(1, 40)), "synthetic")  # config 15, code 0
    add_opus("opus_synth_code2_celt_2x10ms.bin", bytes([0xF2, 0x05]) + bytes(range(1, 30)), "synthetic")  # 2 frames, VBR
    add_opus("opus_synth_code3_celt_20ms_x3.bin", bytes([0xFB, 0x03]) + bytes(range(1, 60)), "synthetic")  # 3 frames

    manifest = {
        "formats": {
            "h264": "Annex-B access unit (start codes), keyframe has SPS/PPS in-band",
            "h265": "Annex-B access unit (start codes), keyframe has VPS/SPS/PPS in-band",
            "av1": "low-overhead OBU temporal unit (obu_has_size_field=1); starts with temporal delimiter 12 00",
            "vp8": "raw VP8 frame",
            "vp9": "raw VP9 frame",
            "opus": "raw Opus packet (TOC byte first)",
        },
        "files": entries,
    }
    (CODEC_FIX / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


SECTIONS = {"m1": gen_m1, "opus_table": gen_opus_table, "t4": gen_t4}

if __name__ == "__main__":
    # No args: only t4 (x264 output for the M1 fixtures is not byte-stable across runs, so m1 /
    # opus_table are regenerated only when named explicitly).
    for name in sys.argv[1:] or ["t4"]:
        SECTIONS[name]()
