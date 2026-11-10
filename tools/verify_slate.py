"""Cross-check the Rust slate (crates/relay-media/src/slate.rs) against libavcodec's H.264 decoder.

Run (after `UPDATE_SLATE_FIXTURES=1 cargo test -p relay-media --test slate` regenerated the fixtures):
    $PY/.venv/Scripts/python.exe tools/verify_slate.py

Decodes every length-prefixed fixture in crates/relay-media/tests/fixtures/slate/ with a fresh PyAV decoder,
asserts frame count, size and that Y, U and V are uniformly 128 in every frame, and writes
slate_decoded.json next to the fixtures (read by the Rust test `test_fixtures_match_pyav_decode`).
"""

from __future__ import annotations

import hashlib
import json
import re
import struct
import sys
from pathlib import Path

import av

FIX = Path(__file__).resolve().parent.parent / "crates" / "relay-media" / "tests" / "fixtures" / "slate"


def read_aus(path: Path) -> list[bytes]:
    data = path.read_bytes()
    out, i = [], 0
    while i < len(data):
        (n,) = struct.unpack(">I", data[i : i + 4])
        out.append(data[i + 4 : i + 4 + n])
        i += 4 + n
    return out


def decode_variant(aus: list[bytes]) -> dict:
    dec = av.CodecContext.create("h264", "r")
    frames = []
    for au in aus:
        frames += dec.decode(av.Packet(au))
    frames += dec.decode(None)
    if len(frames) != len(aus):
        raise SystemExit(f"decoded {len(frames)} frames from {len(aus)} AUs")
    sizes = {(f.width, f.height) for f in frames}
    if len(sizes) != 1 or {f.format.name for f in frames} != {"yuv420p"}:
        raise SystemExit(f"unexpected frame geometry/format: {sizes}")
    (w, h) = sizes.pop()
    sha = hashlib.sha256()
    lo = {"y": 255, "u": 255, "v": 255}
    hi = {"y": 0, "u": 0, "v": 0}
    for f in frames:
        for name, plane in zip("yuv", f.planes):
            pw, ph = (w, h) if name == "y" else (w // 2, h // 2)
            raw = bytes(plane)
            rows = b"".join(raw[r * plane.line_size : r * plane.line_size + pw] for r in range(ph))
            sha.update(rows)
            lo[name] = min(lo[name], min(rows))
            hi[name] = max(hi[name], max(rows))
    out = {"frames": len(frames), "decoded_width": w, "decoded_height": h, "sha256_yuv": sha.hexdigest()}
    for name in "yuv":
        out[name] = {"min": lo[name], "max": hi[name]}
        if out[name] != {"min": 128, "max": 128}:
            raise SystemExit(f"plane {name} is not uniformly 128: {out[name]}")
    return out


def main() -> None:
    cases: dict[tuple[int, int, int], list[tuple[int, Path]]] = {}
    for p in sorted(FIX.glob("slate_*x*_*fps_v*.bin")):
        m = re.fullmatch(r"slate_(\d+)x(\d+)_(\d+)fps_v(\d+)\.bin", p.name)
        w, h, fps, n = map(int, m.groups())
        cases.setdefault((w, h, fps), []).append((n, p))
    result = []
    for (w, h, fps), files in sorted(cases.items()):
        variants = []
        for _, p in sorted(files):
            d = decode_variant(read_aus(p))
            if (d["decoded_width"], d["decoded_height"]) != (w, h):
                raise SystemExit(f"{p.name}: decoded {d['decoded_width']}x{d['decoded_height']}, expected {w}x{h}")
            variants.append(d)
            print(f"{p.name}: {d['frames']} frames {w}x{h} Y=U=V=128 sha256={d['sha256_yuv'][:16]}")
        result.append({"width": w, "height": h, "fps": fps, "variants": variants})
    (FIX / "slate_decoded.json").write_text(json.dumps({"decoder": f"PyAV {av.__version__} h264", "cases": result}, indent=1) + "\n")
    print("OK", len(result), "cases")


if __name__ == "__main__":
    sys.exit(main())
