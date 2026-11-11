"""Independent acceptance tests. Run only after the model's single turn ends."""
from __future__ import annotations

import ast
import contextlib
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

WORK = Path(__file__).resolve().parent
REPO = WORK / "trial_repo"
RUNTIME = WORK / "acceptance_runtime"
RESULTS = []
LOCK = threading.Lock()
EXPECTED = [
    ("north", "N001", 2, "北区道路养护公告（时间更正）", 0),
    ("north", "N002", 2, None, 1),
    ("north", "N003", 1, "北区公共设施检修公告", 0),
    ("south", "S001", 2, "南区管线维护公告（更正）", 0),
    ("south", "S002", 2, None, 1),
    ("south", "S003", 2, "南区场馆开放公告（更正）", 0),
    ("south", "S004", 1, "南区河道巡检公告", 0),
]


def check(name, ok, actual=None, expected=None, note="", advisory=False):
    value = {"name": name, "status": ("PASS" if ok else "OBSERVE" if advisory else "FAIL"),
             "actual": actual, "expected": expected, "note": note}
    with LOCK:
        RESULTS.append(value)
        print(value["status"] + " " + name + ": " + json.dumps(actual, ensure_ascii=False), flush=True)
    return ok


def query(db, sql, args=()):
    with sqlite3.connect(str(db), timeout=5) as connection:
        return connection.execute(sql, args).fetchall()


def rows(db):
    return query(db, "SELECT partition,id,revision,title,deleted FROM announcements ORDER BY partition,id")


def api(base, path, method="GET"):
    with urlopen(Request(base + path, method=method), timeout=3) as response:
        return json.load(response)


def run_cli(db, command="run", base=None, extra=(), timeout=35, tag="run"):
    args = [sys.executable, str(REPO / "collector.py"), command, "--db", str(db)]
    if command == "run":
        args += ["--base-url", base, "--workers", "2"]
    args += list(extra)
    started = time.monotonic()
    try:
        result = subprocess.run(args, cwd=REPO, capture_output=True, text=True, encoding="utf-8",
                                errors="replace", timeout=timeout,
                                env=dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8"))
        record = {"args": args, "returncode": result.returncode, "stdout": result.stdout,
                  "stderr": result.stderr, "elapsed": round(time.monotonic() - started, 3)}
    except subprocess.TimeoutExpired as exc:
        def decoded(value):
            return value.decode("utf-8", "replace") if isinstance(value, bytes) else value or ""
        record = {"args": args, "returncode": None, "stdout": decoded(exc.stdout),
                  "stderr": decoded(exc.stderr), "elapsed": round(time.monotonic() - started, 3),
                  "timed_out": True}
    (RUNTIME / (tag + ".json")).write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    return record


@contextlib.contextmanager
def source_process():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    process = subprocess.Popen([sys.executable, str(REPO / "fixture_server.py"), "--port", str(port)],
                               cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    base = "http://127.0.0.1:" + str(port)
    try:
        for _ in range(100):
            try:
                if api(base, "/health")["ok"]:
                    break
            except Exception:
                time.sleep(0.03)
        else:
            raise RuntimeError("fixture source did not start")
        yield base
    finally:
        process.terminate()
        process.communicate(timeout=5)


def assert_checkpoint_complete(db, label):
    actual = query(db, "SELECT partition,next_cursor,done FROM checkpoints ORDER BY partition")
    check(label, actual == [("north", None, 1), ("south", None, 1)], actual,
          [("north", None, 1), ("south", None, 1)])


def normal_and_idempotency(base):
    db = RUNTIME / "normal.db"
    outcome = run_cli(db, base=base, tag="normal")
    check("normal_exit", outcome["returncode"] == 0, outcome)
    actual = rows(db)
    check("exact_final_business_rows", actual == EXPECTED, actual, EXPECTED)
    assert_checkpoint_complete(db, "final_checkpoints")
    qrows = query(db, "SELECT partition,cursor,record_index,raw_json,reason FROM quarantine ORDER BY partition,cursor,record_index")
    source = json.loads((REPO / "fixtures" / "pages.json").read_text(encoding="utf-8"))
    expected_keys = [("north", "n2", 1), ("south", "s3", 2)]
    check("quarantine_exact_keys", [row[:3] for row in qrows] == expected_keys, [row[:3] for row in qrows], expected_keys)
    raw_ok = len(qrows) == 2 and all(json.loads(row[3]) == source[row[0]][row[1]]["records"][row[2]]
                                    and isinstance(row[4], str) and row[4].strip() for row in qrows)
    check("quarantine_preserves_raw_and_reason", raw_ok, qrows)
    metrics = api(base, "/metrics")
    check("normal_exact_http_requests", len(metrics["requests"]) == 9, metrics, "9 requests")
    check("partition_concurrency", metrics["max_parallel"] >= 2, metrics["max_parallel"], ">=2")
    north_n2 = [r for r in metrics["requests"] if r["partition"] == "north" and r["cursor"] == "n2"]
    retry_gap = north_n2[1]["at_monotonic"] - north_n2[0]["at_monotonic"] if len(north_n2) >= 2 else None
    check("retry_after_429_one_second", retry_gap is not None and retry_gap >= 1.0, retry_gap, ">=1.0 seconds")
    check("planned_503_retries", metrics["attempts"].get("south:s2") == 3, metrics["attempts"].get("south:s2"), 3)
    status = run_cli(db, "status", tag="normal_status")
    try:
        status_json = json.loads(status["stdout"])
    except Exception:
        status_json = None
    check("status_is_machine_readable", status["returncode"] == 0 and isinstance(status_json, dict), status)
    if isinstance(status_json, dict):
        count_values = {key: status_json.get(key) for key in ["total", "active", "deleted", "quarantined"]}
        check("status_exact_counts", count_values == {"total": 7, "active": 5, "deleted": 2, "quarantined": 2}, count_values)
        partitions = status_json.get("partitions", {})
        check("status_partition_completion", isinstance(partitions, dict) and set(partitions) == {"north", "south"}
              and all(p.get("done") in (True, 1) and p.get("next_cursor") is None for p in partitions.values()), partitions)
    repeat = run_cli(db, base=base, tag="normal_repeat")
    after = api(base, "/metrics")
    check("completed_rerun_zero_feed_requests", repeat["returncode"] == 0 and after["requests"] == metrics["requests"],
          {"before": len(metrics["requests"]), "after": len(after["requests"]), "exit": repeat["returncode"]})
    check("completed_rerun_no_row_changes", rows(db) == actual and query(db,
          "SELECT partition,cursor,record_index,raw_json,reason FROM quarantine ORDER BY partition,cursor,record_index") == qrows,
          rows(db))
    return db


def crash_and_recover(base):
    api(base, "/reset", "POST")
    db = RUNTIME / "crash.db"
    crashed = run_cli(db, base=base, extra=["--crash-before-commit", "north:n2"], tag="crash")
    check("real_crash_exit_86", crashed["returncode"] == 86, crashed)
    check("crash_marker", "FAULT_INJECTION north:n2 before_commit" in crashed["stdout"] + crashed["stderr"], crashed)
    north = query(db, "SELECT partition,id,revision,title,deleted FROM announcements WHERE partition='north' ORDER BY id")
    expected = [("north", "N001", 1, "北区道路养护公告（初版）", 0), ("north", "N002", 1, "北区停水公告", 0)]
    check("crash_rolls_back_business_page", north == expected, north, expected)
    checkpoint = query(db, "SELECT next_cursor,done FROM checkpoints WHERE partition='north'")
    check("crash_rolls_back_checkpoint", checkpoint == [("n2", 0)], checkpoint, [("n2", 0)])
    quarantined = query(db, "SELECT COUNT(*) FROM quarantine WHERE partition='north'")[0][0]
    check("crash_rolls_back_quarantine", quarantined == 0, quarantined, 0)
    recovered = run_cli(db, base=base, tag="recovery")
    check("recovery_exact_final_rows", recovered["returncode"] == 0 and rows(db) == EXPECTED, rows(db), EXPECTED)
    assert_checkpoint_complete(db, "recovery_checkpoints")
    check("recovery_quarantine_count", query(db, "SELECT COUNT(*) FROM quarantine")[0][0] == 2,
          query(db, "SELECT COUNT(*) FROM quarantine")[0][0], 2)
    attempts = api(base, "/metrics")["attempts"]
    check("recovery_does_not_replay_committed_start", attempts.get("north:start") == 1, attempts, "north:start=1")


def reset_scope(base, db):
    with sqlite3.connect(str(db)) as connection:
        connection.execute("CREATE TABLE external_sentinel(value TEXT)")
        connection.execute("INSERT INTO external_sentinel VALUES ('keep this unrelated table')")
    neighbor = RUNTIME / "neighbor.db"
    with sqlite3.connect(str(neighbor)) as connection:
        connection.execute("CREATE TABLE sentinel(value TEXT)")
        connection.execute("INSERT INTO sentinel VALUES ('keep neighboring database')")
    before_hash = hashlib.sha256(neighbor.read_bytes()).hexdigest()
    before_metrics = api(base, "/metrics")
    reset = run_cli(db, "reset", tag="reset")
    check("reset_exit", reset["returncode"] == 0, reset)
    table_names = {row[0] for row in query(db, "SELECT name FROM sqlite_master WHERE type='table'")}
    sentinel = query(db, "SELECT value FROM external_sentinel") if "external_sentinel" in table_names else []
    check("reset_preserves_unrelated_table", sentinel == [("keep this unrelated table",)], sentinel)
    check("reset_preserves_neighbor_database", neighbor.exists() and hashlib.sha256(neighbor.read_bytes()).hexdigest() == before_hash,
          neighbor.exists())
    check("reset_preserves_source_state", api(base, "/metrics") == before_metrics, api(base, "/metrics"))
    counts = {}
    for table in ["announcements", "quarantine"]:
        counts[table] = query(db, "SELECT COUNT(*) FROM " + table)[0][0] if table in table_names else 0
    checkpoints = query(db, "SELECT next_cursor,done FROM checkpoints") if "checkpoints" in table_names else []
    check("reset_clears_only_collector_state", all(v == 0 for v in counts.values()) and
          all(row == ("start", 0) for row in checkpoints), {"counts": counts, "checkpoints": checkpoints})


@contextlib.contextmanager
def custom_source(mode):
    request_counts = {}
    request_log = []
    local_lock = threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass
        def do_GET(self):
            from urllib.parse import parse_qs, urlparse
            parsed = urlparse(self.path)
            extra_headers = {}
            if parsed.path == "/health":
                payload, code = {"ok": True, "partitions": ["north", "south"]}, 200
            elif parsed.path == "/feed":
                parameters = parse_qs(parsed.query)
                partition = parameters.get("partition", [""])[0]
                cursor = parameters.get("cursor", ["start"])[0]
                key = partition + ":" + cursor
                with local_lock:
                    request_counts[key] = request_counts.get(key, 0) + 1
                    attempt = request_counts[key]
                    request_entry = {"key": key, "time": time.monotonic(), "wall_time": time.time()}
                    request_log.append(request_entry)
                if mode == "permanent503":
                    payload, code = {"error": "permanent test outage"}, 503
                elif mode == "400":
                    payload, code = {"error": "invalid request must not retry"}, 400
                elif mode == "timeout":
                    time.sleep(120)
                    payload, code = {"records": [], "next_cursor": None}, 200
                elif mode == "missing_cursor":
                    payload, code = {"partition": partition, "cursor": cursor,
                                     "records": [{"id": "BOUNDARY", "revision": 1, "op": "upsert", "title": "valid row"}]}, 200
                elif mode == "retry_after_date":
                    if attempt == 1:
                        from email.utils import formatdate
                        deadline = int(time.time() + 3)
                        extra_headers["Retry-After"] = formatdate(deadline, usegmt=True)
                        request_entry["retry_after_wall"] = deadline
                        payload, code = {"error": "date-form rate limit"}, 429
                    else:
                        payload, code = {"partition": partition, "cursor": cursor, "records": [], "next_cursor": None}, 200
                else:
                    records = [] if partition == "south" else [
                        {"id": "A", "revision": 2, "op": "upsert", "title": "highest"},
                        {"id": "A", "revision": 2, "op": "upsert", "title": "same-version-must-not-overwrite"},
                        {"id": "A", "revision": 1, "op": "upsert", "title": "older"},
                        {"id": "D", "revision": 3, "op": "delete"},
                        {"id": "D", "revision": 2, "op": "upsert", "title": "must-not-resurrect"},
                        {"id": "BOOL", "revision": True, "op": "upsert", "title": "invalid boolean"},
                        {"id": "", "revision": 1, "op": "upsert", "title": "empty id invalid"},
                        {"id": "EMPTY_TITLE", "revision": 1, "op": "upsert", "title": ""},
                        {"id": "ZERO", "revision": 0, "op": "upsert", "title": "zero revision invalid"},
                        {"id": "OP", "revision": 1, "op": "unknown", "title": "invalid operation"},
                        {"id": " ", "revision": 1, "op": "upsert", "title": " "},
                    ]
                    payload, code = {"partition": partition, "cursor": cursor, "records": records, "next_cursor": None}, 200
            else:
                payload, code = {"error": "not found"}, 404
            data = json.dumps(payload).encode()
            try:
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for key, value in extra_headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(data)
            except OSError:
                pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), request_counts, request_log
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def adversarial_records():
    db = RUNTIME / "adversarial.db"
    with custom_source("records") as (base, counts, request_log):
        outcome = run_cli(db, base=base, tag="adversarial")
    check("adversarial_exit", outcome["returncode"] == 0, outcome)
    actual = rows(db)
    lookup = {row[1]: row for row in actual}
    check("same_revision_cannot_overwrite", lookup.get("A") == ("north", "A", 2, "highest", 0), lookup.get("A"))
    check("stale_upsert_cannot_revive_tombstone", lookup.get("D") == ("north", "D", 3, None, 1), lookup.get("D"))
    qrows = query(db, "SELECT record_index,raw_json,reason FROM quarantine ORDER BY record_index")
    qindices = {row[0] for row in qrows}
    check("bool_revision_rejected", 5 in qindices and "BOOL" not in lookup, qrows)
    check("empty_id_and_title_rejected", {6, 7}.issubset(qindices) and "" not in lookup and "EMPTY_TITLE" not in lookup, qrows)
    check("zero_revision_and_unknown_op_rejected", {8, 9}.issubset(qindices) and "ZERO" not in lookup and "OP" not in lookup, qrows)
    # Pure whitespace is ambiguous in natural language; do not use it as a scoring failure.
    check("whitespace_string_policy_observation", " " in lookup, {"stored_whitespace": " " in lookup,
          "quarantined_index_10": 10 in qindices}, note="题目只写非空字符串；空白策略仅作观察，不计失败。", advisory=True)


def failed_source(mode):
    db = RUNTIME / (mode + ".db")
    with custom_source(mode) as (base, counts, request_log):
        outcome = run_cli(db, base=base, tag=mode, timeout=75 if mode == "timeout" else 40)
    if outcome.get("timed_out"):
        check(mode + "_bounded_failure", False, {"outcome": outcome, "attempts": counts},
              note="测试观察窗口内未退出；需求未规定超时秒数，须结合代码检查，不能单凭此项认定无限重试。", advisory=(mode == "timeout"))
    else:
        check(mode + "_nonzero_exit", outcome["returncode"] != 0, outcome)
    limit = 1 if mode == "400" else 5
    check(mode + "_attempt_limit", bool(counts) and max(counts.values()) <= limit, counts, "each page <= " + str(limit))
    if mode == "permanent503":
        check("permanent503_exhausts_five_attempts", bool(counts) and max(counts.values()) == 5, counts, "at least one page attempted 5 times")
    if mode == "timeout":
        check("timeout_actually_retries", bool(counts) and max(counts.values()) >= 2, counts, ">=2 attempts")
    if db.exists():
        tables = {r[0] for r in query(db, "SELECT name FROM sqlite_master WHERE type='table'")}
        states = query(db, "SELECT partition,next_cursor,done FROM checkpoints") if "checkpoints" in tables else []
        check(mode + "_no_false_completed_checkpoint", all(not row[2] and row[1] == "start" for row in states), states)
        data = query(db, "SELECT COUNT(*) FROM announcements")[0][0] if "announcements" in tables else 0
        check(mode + "_no_business_rows", data == 0, data, 0)


def supplementary_boundaries():
    """Diagnostic probes beyond the specified fixed-source acceptance cases."""
    db = RUNTIME / "missing_cursor.db"
    with custom_source("missing_cursor") as (base, counts, request_log):
        outcome = run_cli(db, base=base, tag="missing_cursor")
    states = query(db, "SELECT partition,next_cursor,done FROM checkpoints") if db.exists() else []
    check("boundary_missing_next_cursor_rejected", outcome["returncode"] not in (None, 0)
          and all(not row[2] for row in states), {"run": outcome, "checkpoints": states},
          note="补充健壮性探测：固定源始终包含next_cursor；本项不作为基本夹具主需求失败。", advisory=True)
    db = RUNTIME / "retry_after_date.db"
    with custom_source("retry_after_date") as (base, counts, request_log):
        outcome = run_cli(db, base=base, tag="retry_after_date")
    observed = []
    for key in counts:
        requests = [entry for entry in request_log if entry["key"] == key]
        if len(requests) >= 2 and "retry_after_wall" in requests[0]:
            observed.append({"key": key, "deadline": requests[0]["retry_after_wall"],
                             "retried_at": requests[1]["wall_time"],
                             "early_by_seconds": requests[0]["retry_after_wall"] - requests[1]["wall_time"]})
    check("boundary_http_date_retry_after", outcome["returncode"] == 0 and bool(observed)
          and all(entry["early_by_seconds"] <= 0.05 for entry in observed),
          {"run": outcome, "timing": observed},
          note="补充协议边界：固定源只给秒数Retry-After，本探测使用HTTP-date，不按固定夹具核心失败计分。", advisory=True)


def guarded(name, function, *args):
    try:
        return function(*args)
    except Exception as exc:
        check(name + "_test_exception", False, {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()})


def main():
    if not (REPO / "collector.py").exists():
        raise SystemExit("Collector missing; do not run before single-turn completion.")
    if RUNTIME.exists():
        raise SystemExit("Acceptance runtime exists. Preserve evidence; choose a new directory for a subsequent run.")
    RUNTIME.mkdir()
    started = time.monotonic()
    with source_process() as base:
        db = guarded("normal", normal_and_idempotency, base)
        guarded("crash", crash_and_recover, base)
        if db:
            guarded("reset", reset_scope, base, db)
    guarded("adversarial", adversarial_records)
    guarded("supplementary", supplementary_boundaries)
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(guarded, mode, failed_source, mode) for mode in ["permanent503", "400", "timeout"]]
        for future in futures:
            future.result()
    source = (REPO / "collector.py").read_text(encoding="utf-8")
    symbols = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            symbols.append({"name": node.name, "line": node.lineno, "end_line": getattr(node, "end_lineno", node.lineno)})
    report = {"summary": {status: sum(r["status"] == status for r in RESULTS) for status in ["PASS", "FAIL", "OBSERVE"]},
              "elapsed_seconds": round(time.monotonic() - started, 3), "results": RESULTS,
              "collector_symbols": sorted(symbols, key=lambda x: x["line"]),
              "collector_sha256": hashlib.sha256((REPO / "collector.py").read_bytes()).hexdigest()}
    (WORK / "acceptance_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("SUMMARY " + json.dumps(report["summary"]) + " elapsed=" + str(report["elapsed_seconds"]), flush=True)
    return 1 if report["summary"]["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
