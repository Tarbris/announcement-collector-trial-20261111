#!/usr/bin/env python3
"""Standard-library integration tests for collector.py."""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parent
COLLECTOR = ROOT / "collector.py"
FIXTURE_SERVER = ROOT / "fixture_server.py"

EXPECTED_ANNOUNCEMENTS = [
    ("north", "N001", 2, "北区道路养护公告（时间更正）", 0),
    ("north", "N002", 2, None, 1),
    ("north", "N003", 1, "北区公共设施检修公告", 0),
    ("south", "S001", 2, "南区管线维护公告（更正）", 0),
    ("south", "S002", 2, None, 1),
    ("south", "S003", 2, "南区场馆开放公告（更正）", 0),
    ("south", "S004", 1, "南区河道巡检公告", 0),
]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _get_json(url: str):
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.loads(response.read().decode("utf-8"))


class FixtureSource:
    def __init__(self) -> None:
        self.port = _free_port()
        self.base_url = "http://127.0.0.1:%d" % self.port
        self.process = subprocess.Popen(
            [sys.executable, str(FIXTURE_SERVER), "--port", str(self.port)],
            cwd=str(ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 8
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("fixture server exited early")
            try:
                _get_json(self.base_url + "/health")
                return
            except (urllib.error.URLError, OSError, json.JSONDecodeError):
                time.sleep(0.1)
        raise RuntimeError("fixture server did not become ready")

    def metrics(self):
        return _get_json(self.base_url + "/metrics")

    def close(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


def _run_collector(*args: object, timeout: int = 60):
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [sys.executable, str(COLLECTOR), *[str(arg) for arg in args]],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=timeout,
    )


def _status(db: Path) -> Dict[str, object]:
    completed = _run_collector("status", "--db", db)
    if completed.returncode != 0:
        raise AssertionError("status failed: " + completed.stderr)
    return json.loads(completed.stdout)


def _db_rows(db: Path):
    conn = sqlite3.connect(str(db))
    try:
        announcements = conn.execute(
            "SELECT partition, id, revision, title, deleted FROM announcements ORDER BY partition, id"
        ).fetchall()
        quarantine = conn.execute(
            "SELECT partition, cursor, record_index, raw_json, reason "
            "FROM quarantine ORDER BY partition, cursor, record_index"
        ).fetchall()
        checkpoints = conn.execute(
            "SELECT partition, next_cursor, done FROM checkpoints ORDER BY partition"
        ).fetchall()
        return announcements, quarantine, checkpoints
    finally:
        conn.close()


class CollectorIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = FixtureSource()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Path(self.temp_dir.name) / "collector.db"

    def tearDown(self) -> None:
        self.source.close()
        self.temp_dir.cleanup()

    def _clean_run(self, db: Path = None) -> None:
        db = db or self.db
        completed = _run_collector(
            "run", "--base-url", self.source.base_url, "--db", db, "--workers", "2"
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_01_normal_collection_state_retries_and_concurrency(self) -> None:
        self._clean_run()
        status = _status(self.db)
        self.assertEqual(
            {key: status[key] for key in ("total", "active", "deleted", "quarantined")},
            {"total": 7, "active": 5, "deleted": 2, "quarantined": 2},
        )
        self.assertEqual(
            status["partitions"],
            {
                "north": {"next_cursor": None, "done": True},
                "south": {"next_cursor": None, "done": True},
            },
        )

        announcements, quarantine, checkpoints = _db_rows(self.db)
        self.assertEqual(announcements, EXPECTED_ANNOUNCEMENTS)
        self.assertEqual(
            [(row[0], row[1], row[2]) for row in quarantine],
            [("north", "n2", 1), ("south", "s3", 2)],
        )
        north_raw = json.loads(quarantine[0][3])
        self.assertEqual(north_raw["id"], "N_BAD")
        self.assertNotIn("title", north_raw)
        self.assertIn("title", quarantine[0][4])
        south_raw = json.loads(quarantine[1][3])
        self.assertEqual(south_raw["id"], "S_BAD")
        self.assertEqual(south_raw["revision"], "oops")
        self.assertIn("revision", quarantine[1][4])
        self.assertEqual(checkpoints, [("north", None, 1), ("south", None, 1)])

        metrics = self.source.metrics()
        self.assertGreaterEqual(metrics["max_parallel"], 2)
        self.assertEqual(metrics["attempts"]["north:n2"], 2)
        self.assertEqual(metrics["attempts"]["south:s2"], 3)
        self.assertEqual(len(metrics["requests"]), 9)

        north_n2 = [
            request
            for request in metrics["requests"]
            if request["partition"] == "north" and request["cursor"] == "n2"
        ]
        self.assertEqual([request["status"] for request in north_n2], [429, 200])
        self.assertGreaterEqual(north_n2[1]["at_monotonic"] - north_n2[0]["at_monotonic"], 1.0)

        south_s2 = [
            request
            for request in metrics["requests"]
            if request["partition"] == "south" and request["cursor"] == "s2"
        ]
        self.assertEqual([request["status"] for request in south_s2], [503, 503, 200])

    def test_02_completed_rerun_does_not_request_feed(self) -> None:
        self._clean_run()
        before = self.source.metrics()
        before_rows = _db_rows(self.db)

        completed = _run_collector(
            "run", "--base-url", self.source.base_url, "--db", self.db, "--workers", "2"
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("无需请求 /feed", completed.stdout)

        after = self.source.metrics()
        self.assertEqual(len(after["requests"]), len(before["requests"]))
        self.assertEqual(after["attempts"], before["attempts"])
        self.assertEqual(_db_rows(self.db), before_rows)
        self.assertEqual(_status(self.db)["total"], 7)

    def test_03_real_process_crash_rolls_back_page_and_resumes(self) -> None:
        crashed = _run_collector(
            "run",
            "--base-url",
            self.source.base_url,
            "--db",
            self.db,
            "--workers",
            "2",
            "--crash-before-commit",
            "north:n2",
        )
        self.assertEqual(crashed.returncode, 86, crashed.stderr)
        self.assertIn("FAULT_INJECTION north:n2 before_commit", crashed.stdout)
        self.assertTrue(self.db.exists())

        conn = sqlite3.connect(str(self.db))
        try:
            checkpoint = conn.execute(
                "SELECT next_cursor, done FROM checkpoints WHERE partition = 'north'"
            ).fetchone()
            north_rows = conn.execute(
                "SELECT id, revision, title, deleted FROM announcements "
                "WHERE partition = 'north' ORDER BY id"
            ).fetchall()
            north_quarantine = conn.execute(
                "SELECT COUNT(*) FROM quarantine WHERE partition = 'north'"
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(checkpoint, ("n2", 0))
        self.assertEqual(
            north_rows,
            [
                ("N001", 1, "北区道路养护公告（初版）", 0),
                ("N002", 1, "北区停水公告", 0),
            ],
        )
        self.assertEqual(north_quarantine, 0)

        self._clean_run()
        announcements, quarantine, checkpoints = _db_rows(self.db)
        self.assertEqual(announcements, EXPECTED_ANNOUNCEMENTS)
        self.assertEqual(len(quarantine), 2)
        self.assertEqual(checkpoints, [("north", None, 1), ("south", None, 1)])
        self.assertEqual(_status(self.db)["quarantined"], 2)

    def test_04_reset_only_clears_the_selected_database(self) -> None:
        second_db = Path(self.temp_dir.name) / "other.db"
        marker = Path(self.temp_dir.name) / "keep.txt"
        marker.write_text("keep", encoding="utf-8")
        self._clean_run(self.db)
        self._clean_run(second_db)
        metrics_before = self.source.metrics()

        reset = _run_collector("reset", "--db", self.db)
        self.assertEqual(reset.returncode, 0, reset.stderr)

        metrics_after = self.source.metrics()
        self.assertEqual(metrics_after["requests"], metrics_before["requests"])
        self.assertEqual(metrics_after["attempts"], metrics_before["attempts"])
        self.assertEqual(
            _status(self.db),
            {"total": 0, "active": 0, "deleted": 0, "quarantined": 0, "partitions": {}},
        )
        self.assertEqual(_db_rows(self.db), ([], [], []))

        second_status = _status(second_db)
        self.assertEqual(second_status["total"], 7)
        self.assertEqual(second_status["quarantined"], 2)
        self.assertEqual(_db_rows(second_db)[0], EXPECTED_ANNOUNCEMENTS)
        self.assertTrue(self.db.exists())
        self.assertTrue(second_db.exists())
        self.assertTrue(marker.exists())


class _Always503Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def _send_json(self, status, value):
        data = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            return self._send_json(200, {"ok": True, "partitions": ["north"]})
        if self.path.startswith("/feed"):
            self.server.feed_times.append(time.monotonic())
            return self._send_json(503, {"error": "always unavailable"})
        if self.path == "/metrics":
            return self._send_json(
                200,
                {"attempts": {"north:start": len(self.server.feed_times)}, "feed_times": self.server.feed_times},
            )
        return self._send_json(404, {"error": "not found"})


class Always503Source:
    def __init__(self) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Always503Handler)
        self.server.feed_times = []
        self.base_url = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class RetryExhaustionTests(unittest.TestCase):
    def test_503_exhaustion_is_clear_and_does_not_commit(self) -> None:
        source = Always503Source()
        temp_dir = tempfile.TemporaryDirectory()
        db = Path(temp_dir.name) / "failed.db"
        try:
            completed = _run_collector(
                "run", "--base-url", source.base_url, "--db", db, "--workers", "2", timeout=30
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("503", completed.stderr)
            self.assertIn("重试耗尽", completed.stderr)
            self.assertNotIn("采集完成", completed.stdout)

            times = list(source.server.feed_times)
            self.assertEqual(len(times), 5)
            expected_gaps = [0.25, 0.5, 1.0, 2.0]
            actual_gaps = [times[index + 1] - times[index] for index in range(4)]
            for actual, expected in zip(actual_gaps, expected_gaps):
                self.assertGreaterEqual(actual, expected - 0.05)

            conn = sqlite3.connect(str(db))
            try:
                checkpoint = conn.execute(
                    "SELECT next_cursor, done FROM checkpoints WHERE partition = 'north'"
                ).fetchone()
                announcements = conn.execute("SELECT COUNT(*) FROM announcements").fetchone()[0]
                quarantine = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(checkpoint, ("start", 0))
            self.assertEqual(announcements, 0)
            self.assertEqual(quarantine, 0)
        finally:
            source.close()
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main(verbosity=2)
