"""Deterministic local announcement source; this file is immutable for the task."""
from __future__ import annotations

import argparse
import copy
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import time
from urllib.parse import parse_qs, urlparse

PAGES = json.loads((Path(__file__).parent / "fixtures" / "pages.json").read_text(encoding="utf-8"))
LOCK = threading.Lock()
STATE = {}


def reset_state():
    with LOCK:
        if STATE.get("in_flight", 0):
            return False
        STATE.clear()
        STATE.update(attempts={}, requests=[], in_flight=0, max_parallel=0)
    return True


reset_state()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def send_json(self, code, value, headers=None):
        data = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def do_POST(self):
        if self.path != "/reset":
            return self.send_json(404, {"error": "not found"})
        ok = reset_state()
        return self.send_json(200 if ok else 409, {"reset": ok})

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            return self.send_json(200, {"ok": True, "partitions": sorted(PAGES)})
        if url.path == "/metrics":
            with LOCK:
                snapshot = copy.deepcopy(STATE)
            return self.send_json(200, snapshot)
        if url.path != "/feed":
            return self.send_json(404, {"error": "not found"})
        params = parse_qs(url.query, keep_blank_values=True)
        partition = params.get("partition", [""])[0]
        cursor = params.get("cursor", ["start"])[0] or "start"
        if partition not in PAGES or cursor not in PAGES[partition]:
            return self.send_json(400, {"error": "invalid partition or cursor"})
        key = partition + ":" + cursor
        with LOCK:
            STATE["in_flight"] += 1
            STATE["max_parallel"] = max(STATE["max_parallel"], STATE["in_flight"])
            attempt = STATE["attempts"].get(key, 0) + 1
            STATE["attempts"][key] = attempt
        try:
            # Latency is deliberate so concurrent partition fetches are observable.
            time.sleep(0.08)
            status, headers = 200, {}
            body = dict(PAGES[partition][cursor], partition=partition, cursor=cursor)
            if key == "north:n2" and attempt == 1:
                status, headers, body = 429, {"Retry-After": "1"}, {"error": "rate limited"}
            elif key == "south:s2" and attempt <= 2:
                status, body = 503, {"error": "temporary source outage"}
            with LOCK:
                STATE["requests"].append({"partition": partition, "cursor": cursor,
                    "attempt": attempt, "status": status, "at_monotonic": time.monotonic()})
            self.send_json(status, body, headers)
        finally:
            with LOCK:
                STATE["in_flight"] -= 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.daemon_threads = True
    print(f"Fixture ready: http://127.0.0.1:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
