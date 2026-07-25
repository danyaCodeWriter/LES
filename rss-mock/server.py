#!/usr/bin/env python3
"""
Minimal Region State Service (RSS) mock for demos.

Implements GET /v1/recovery-groups/{rg}/active-region?region=… as SSE
(state + heartbeat), matching openapi/rss-api.yaml.

Periodically cycles status: active → switch → stand_in → …
and flips activeRegion between HOME_REGION and PEER_REGION.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8090"))
RECOVERY_GROUP = os.environ.get("RECOVERY_GROUP", "demo-rg")
HOME_REGION = os.environ.get("HOME_REGION", "r1")
PEER_REGION = os.environ.get("PEER_REGION", "r2")
CYCLE_INTERVAL_SEC = float(os.environ.get("CYCLE_INTERVAL_SEC", "8"))
HEARTBEAT_INTERVAL_SEC = float(os.environ.get("HEARTBEAT_INTERVAL_SEC", "2"))

# (status, active_region, regional_writes_blocked)
CYCLE = (
    ("active", HOME_REGION, False),
    ("switch", PEER_REGION, True),
    ("stand_in", PEER_REGION, True),
    ("switch", HOME_REGION, True),
)


class Store:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.revision = 0
        self.epoch = 1
        self.status = "active"
        self.active_region = HOME_REGION
        self.regional_writes_blocked = False
        self.subscribers: list[queue.Queue] = []
        self._apply_cycle_step(0, bump_epoch=False)

    def _apply_cycle_step(self, idx: int, *, bump_epoch: bool) -> None:
        status, active, blocked = CYCLE[idx % len(CYCLE)]
        self.revision += 1
        if bump_epoch and status == "switch":
            self.epoch += 1
        self.status = status
        self.active_region = active
        self.regional_writes_blocked = blocked

    def snapshot_for(self, home_region: str) -> dict:
        with self.lock:
            return {
                "region": home_region,
                "activeRegion": self.active_region,
                "status": self.status,
                "regionalWritesBlocked": self.regional_writes_blocked,
                "epoch": self.epoch,
                "revision": self.revision,
            }

    def heartbeat(self) -> dict:
        with self.lock:
            return {"epoch": self.epoch, "revision": self.revision}

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=64)
        with self.lock:
            self.subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            if q in self.subscribers:
                self.subscribers.remove(q)

    def _broadcast(self, event: str, payload: dict) -> None:
        item = (event, payload)
        dead: list[queue.Queue] = []
        with self.lock:
            subs = list(self.subscribers)
        for q in subs:
            try:
                q.put_nowait(item)
            except queue.Full:
                dead.append(q)
        for q in dead:
            self.unsubscribe(q)

    def advance_cycle(self, idx: int) -> dict:
        with self.lock:
            self._apply_cycle_step(idx, bump_epoch=True)
            snap = {
                "region": HOME_REGION,
                "activeRegion": self.active_region,
                "status": self.status,
                "regionalWritesBlocked": self.regional_writes_blocked,
                "epoch": self.epoch,
                "revision": self.revision,
            }
        self._broadcast("state", snap)
        return snap

    def force(self, status: str, active_region: str | None = None) -> dict:
        blocked = status != "active"
        with self.lock:
            self.revision += 1
            if status == "switch":
                self.epoch += 1
            self.status = status
            self.active_region = active_region or (
                HOME_REGION if status == "active" else PEER_REGION
            )
            self.regional_writes_blocked = blocked
            snap = {
                "region": HOME_REGION,
                "activeRegion": self.active_region,
                "status": self.status,
                "regionalWritesBlocked": self.regional_writes_blocked,
                "epoch": self.epoch,
                "revision": self.revision,
            }
        self._broadcast("state", snap)
        return snap


STORE = Store()


def _sse_frame(event: str, data: dict, event_id: int | str) -> bytes:
    body = json.dumps(data, separators=(",", ":"))
    return f"event: {event}\nid: {event_id}\ndata: {body}\n\n".encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        print(f"[rss-mock] {self.address_string()} {fmt % args}")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = urllib.parse.parse_qs(parsed.query)

        if path in ("/health", "/healthz"):
            self._json(200, {"status": "UP", "revision": STORE.heartbeat()["revision"]})
            return

        if path == "/v1/debug/state":
            home = (qs.get("region") or [HOME_REGION])[0]
            self._json(200, STORE.snapshot_for(home))
            return

        prefix = "/v1/recovery-groups/"
        suffix = "/active-region"
        if path.startswith(prefix) and path.endswith(suffix):
            rg = path[len(prefix) : -len(suffix)].strip("/")
            if not rg:
                self._json(400, {"error": "missing recoveryGroup"})
                return
            if rg != RECOVERY_GROUP:
                self._json(400, {"error": f"unknown recoveryGroup '{rg}' (mock expects '{RECOVERY_GROUP}')"})
                return
            region = (qs.get("region") or [""])[0]
            if not region:
                self._json(400, {"error": "query param region is required"})
                return
            self._stream_sse(region)
            return

        self._json(404, {"error": f"not found: {path}"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = urllib.parse.parse_qs(parsed.query)
        if path == "/v1/debug/force":
            status = (qs.get("status") or ["active"])[0]
            if status not in ("active", "stand_in", "switch"):
                self._json(400, {"error": "status must be active|stand_in|switch"})
                return
            active = (qs.get("activeRegion") or [None])[0]
            snap = STORE.force(status, active)
            # Rewrite region echo for response clarity
            snap = dict(snap)
            snap["region"] = HOME_REGION
            self._json(200, snap)
            return
        self._json(404, {"error": f"not found: {path}"})

    def _json(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _stream_sse(self, home_region: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        q = STORE.subscribe()
        try:
            snap = STORE.snapshot_for(home_region)
            self.wfile.write(_sse_frame("state", snap, snap["revision"]))
            self.wfile.flush()
            while True:
                try:
                    event, payload = q.get(timeout=HEARTBEAT_INTERVAL_SEC)
                except queue.Empty:
                    hb = STORE.heartbeat()
                    self.wfile.write(_sse_frame("heartbeat", hb, hb["revision"]))
                    self.wfile.flush()
                    continue
                if event == "state":
                    # Echo caller's home region in payload
                    data = dict(payload)
                    data["region"] = home_region
                    self.wfile.write(_sse_frame("state", data, data["revision"]))
                else:
                    self.wfile.write(_sse_frame(event, payload, payload.get("revision", 0)))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            STORE.unsubscribe(q)


def cycle_loop() -> None:
    idx = 0
    print(
        f"[rss-mock] cycling every {CYCLE_INTERVAL_SEC}s: "
        f"active({HOME_REGION}) → switch → stand_in({PEER_REGION}) → …"
    )
    while True:
        time.sleep(CYCLE_INTERVAL_SEC)
        idx += 1
        snap = STORE.advance_cycle(idx)
        print(
            f"[rss-mock] state revision={snap['revision']} "
            f"status={snap['status']} activeRegion={snap['activeRegion']}"
        )


def main() -> None:
    threading.Thread(target=cycle_loop, name="rss-cycle", daemon=True).start()
    httpd = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[rss-mock] listening on :{PORT} recoveryGroup={RECOVERY_GROUP} home={HOME_REGION}")
    print(f"[rss-mock] SSE GET /v1/recovery-groups/{RECOVERY_GROUP}/active-region?region={HOME_REGION}")
    print("[rss-mock] debug GET /v1/debug/state  POST /v1/debug/force?status=active|stand_in|switch")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
