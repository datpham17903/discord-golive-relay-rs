#!/usr/bin/env python3
"""Public read-only status page for relayctl.

Reads the relay's status.json (polled) and relay.log (tailed, JSON lines from `relayctl run ... | tee -a
relay.log`) and serves `/` (index.html) and `/api/status`. Exposes no ids, tokens or controls.
Stdlib only.

  python3 dashboard.py --status /var/lib/discord-relay/status.json \
      --log /opt/discord-relay-rs/relay.log --data /var/lib/relay-dashboard --listen 127.0.0.1:8787
"""
import argparse
import json
import os
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

POLL_S = 5.0
STALE_S = 30.0  # status.json older than this: the relay process is not running
SERIES_LEN = 720  # 1 h of 5 s samples
INCIDENTS_KEPT = 200
TRANSITIONS_KEPT_S = 7 * 86400
# Session states whose time counts against availability (idle/disabled: the target is not live).
DOWN = {"hold", "connecting", "backoff", "quarantine", "fatal"}
UP = "relaying"


class Collector:
    def __init__(self, status_path, log_path, data_dir):
        self.status_path = status_path
        self.log_path = log_path
        self.history_path = os.path.join(data_dir, "history.json")
        os.makedirs(data_dir, exist_ok=True)
        self.lock = threading.Lock()
        self.series = deque(maxlen=SERIES_LEN)
        self.prev = None
        self.snapshot = {}
        h = self._load_history()
        self.log_pos = h.get("log_pos", 0)
        self.log_ino = h.get("log_ino")
        # [ts, state] in order; the first entry of a process start is ["restart"] too
        self.transitions = h.get("transitions", [])
        self.incidents = h.get("incidents", [])
        self.open_incident = h.get("open_incident")
        self.state = h.get("state")
        self.last_ts = h.get("last_ts")  # newest event of any kind: when a killed run was last alive

    # ------------------------------------------------------------ persistence

    def _load_history(self):
        try:
            with open(self.history_path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save_history(self):
        tmp = self.history_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"log_pos": self.log_pos, "log_ino": self.log_ino,
                       "transitions": self.transitions, "incidents": self.incidents,
                       "open_incident": self.open_incident, "state": self.state,
                       "last_ts": self.last_ts}, f)
        os.replace(tmp, self.history_path)

    # --------------------------------------------------------------- log tail

    def tail_log(self):
        try:
            st = os.stat(self.log_path)
        except OSError:
            return
        if st.st_ino != self.log_ino or st.st_size < self.log_pos:
            self.log_ino, self.log_pos = st.st_ino, 0  # rotated or truncated
        if st.st_size == self.log_pos:
            return
        with open(self.log_path, "rb") as f:
            f.seek(self.log_pos)
            data = f.read()
        end = data.rfind(b"\n") + 1  # keep a partial last line for the next read
        for raw in data[:end].splitlines():
            self._line(raw.decode("utf-8", "replace"))
        self.log_pos += end
        cutoff = time.time() - TRANSITIONS_KEPT_S
        self.transitions = [t for t in self.transitions if t[0] >= cutoff]
        self.incidents = self.incidents[-INCIDENTS_KEPT:]
        self._save_history()

    def _line(self, line):
        if line.startswith("account ingest ->"):
            # first line of `relayctl run`; the previous run ended at an unknown time
            self._restart(self.last_ts)
            return
        i = line.find('{"ts"')
        if i < 0:
            return
        try:
            ev = json.loads(line[i:])
        except ValueError:
            return
        ts, name = ev.get("ts"), ev.get("event")
        if not isinstance(ts, (int, float)):
            return
        self.last_ts = ts
        if name == "state":
            self._state(ts, ev.get("new"), ev.get("hold_reason"))
        elif name == "gateway_disconnected" and self.open_incident:
            self.open_incident.setdefault("notes", []).append("gateway")

    def _restart(self, ts):
        if self.open_incident and ts:
            self._close_incident(ts, by_restart=True)
        self.state = None
        if ts and not (self.transitions and self.transitions[-1] == [ts, "restart"]):
            self.transitions.append([ts, "restart"])
            self.incidents.append({"start": ts, "end": ts, "kind": "restart"})

    def _state(self, ts, new, hold_reason):
        old = self.state
        self.state = new
        self.transitions.append([ts, new])
        # a hold or reconnect after relaying, or any failed connect (a normal start passes
        # through `connecting` only)
        if new in DOWN and (old == UP or new != "connecting") and not self.open_incident:
            self.open_incident = {"start": ts, "kind": new, "reason": hold_reason}
        elif new in DOWN and self.open_incident and hold_reason and not self.open_incident.get("reason"):
            self.open_incident["reason"] = hold_reason
        elif new not in DOWN and self.open_incident:
            self._close_incident(ts, recovered=new == UP)

    def _close_incident(self, ts, recovered=False, by_restart=False):
        inc = self.open_incident
        inc["end"] = ts
        inc["recovered"] = recovered
        if by_restart:
            inc["by_restart"] = True
        if "notes" in inc:
            inc["notes"] = sorted(set(inc["notes"]))
        self.incidents.append(inc)
        self.open_incident = None

    # ------------------------------------------------------------- status poll

    def poll(self):
        now = time.time()
        try:
            age = now - os.stat(self.status_path).st_mtime
            with open(self.status_path, encoding="utf-8") as f:
                s = json.load(f)
        except (OSError, ValueError):
            s, age = None, None
        online = s is not None and age is not None and age < STALE_S
        snap = {"now": now, "online": online, "status_age_s": age}
        sample = {"t": now, "state": None, "pps": None, "fps": None, "loss": None}
        if online:
            w = (s.get("network") or {}).get("watch") or {}
            g = (s.get("metrics") or {}).get("gauges") or {}
            d = s.get("details") or {}
            cur = {"t": now, "pkts": w.get("video_packets_received"),
                   "rtx": w.get("rtx_packets_received"), "frames": g.get("worker.frames_out")}
            pps = fps = loss = None
            p = self.prev
            if p and all(isinstance(cur[k], (int, float)) and isinstance(p[k], (int, float))
                         for k in ("pkts", "rtx", "frames")):
                dt = cur["t"] - p["t"]
                dp = cur["pkts"] - p["pkts"]
                if dt > 0 and dp >= 0 and cur["frames"] >= p["frames"]:
                    pps = dp / dt
                    fps = (cur["frames"] - p["frames"]) / dt
                    loss = 100.0 * (cur["rtx"] - p["rtx"]) / dp if dp > 0 else 0.0
            self.prev = cur
            accounts = s.get("accounts") or []
            state = s.get("state")
            snap.update({
                "state": state,
                "hold_reason": s.get("hold_reason"),
                "enabled": s.get("desired_enabled"),
                "codec": s.get("codec"),
                "session_uptime_s": d.get("session_uptime_s"),
                "channel_members": d.get("viewers"),
                "relay_viewers": sum(a.get("viewers") or 0 for a in accounts
                                     if a.get("state") == "live"),
                "outputs_live": sum(1 for a in accounts if a.get("state") == "live"),
                "mirrors": len(((s.get("routing") or {}).get("mirrors")) or {}),
                "pps": pps, "fps": fps, "loss_pct": loss,
            })
            sample.update({"state": state, "pps": pps, "fps": fps, "loss": loss})
        else:
            self.prev = None
        with self.lock:
            self.series.append(sample)
            self.snapshot = snap

    # ----------------------------------------------------------------- report

    def availability(self, window_s, now):
        start = now - window_s
        up = down = 0.0
        tr = self.transitions
        for i, (ts, state) in enumerate(tr):
            end = tr[i + 1][0] if i + 1 < len(tr) else now
            a, b = max(ts, start), min(end, now)
            if b <= a:
                continue
            if state == UP:
                up += b - a
            elif state in DOWN:
                down += b - a
        total = up + down
        return {"pct": 100.0 * up / total if total else None, "up_s": up, "down_s": down}

    def report(self):
        now = time.time()
        with self.lock:
            snap = dict(self.snapshot)
            series = list(self.series)
        last = next((t for t in reversed(self.transitions) if t[1] != "restart"), None)
        snap["state_since"] = last[0] if last and last[1] == snap.get("state") else None
        incidents = list(self.incidents[-30:])
        if self.open_incident:
            incidents.append(dict(self.open_incident, ongoing=True))
        snap["incidents"] = list(reversed(incidents))
        snap["availability"] = {"24h": self.availability(86400, now),
                                "7d": self.availability(7 * 86400, now)}
        snap["series"] = series
        return snap

    def run(self):
        while True:
            try:
                self.tail_log()
                self.poll()
            except Exception as e:  # keep serving; the page shows the stale age
                print(f"collector error: {e!r}", flush=True)
            time.sleep(POLL_S)


def make_handler(collector, page_path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/index.html"):
                try:
                    with open(page_path, "rb") as f:
                        body = f.read()
                except OSError:
                    self.send_error(500)
                    return
                self._send(body, "text/html; charset=utf-8", "no-cache")
            elif self.path == "/api/status":
                body = json.dumps(collector.report(), separators=(",", ":")).encode()
                self._send(body, "application/json", "no-store")
            else:
                self.send_error(404)

        def _send(self, body, ctype, cache):
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", cache)
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--status", default="/var/lib/discord-relay/status.json")
    ap.add_argument("--log", default="/opt/discord-relay-rs/relay.log")
    ap.add_argument("--data", default="/var/lib/relay-dashboard")
    ap.add_argument("--listen", default="127.0.0.1:8787")
    a = ap.parse_args()
    collector = Collector(a.status, a.log, a.data)
    collector.tail_log()
    collector.poll()
    threading.Thread(target=collector.run, daemon=True).start()
    host, port = a.listen.rsplit(":", 1)
    page = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    srv = ThreadingHTTPServer((host, int(port)), make_handler(collector, page))
    print(f"dashboard on http://{a.listen}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
