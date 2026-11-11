#!/usr/bin/env python3
"""Resumable, idempotent announcement collector for the local fixture source."""
from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


REQUEST_TIMEOUT_SECONDS = 2.0
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 0.25
BACKOFF_CAP_SECONDS = 4.0
CRASH_EXIT_CODE = 86
SQLITE_BUSY_TIMEOUT_MS = 30000


class CollectorError(Exception):
    """A fatal collection error that must make the command exit non-zero."""


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA busy_timeout=%d" % SQLITE_BUSY_TIMEOUT_MS)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS announcements (
            partition TEXT NOT NULL,
            id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            title TEXT,
            deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
            PRIMARY KEY (partition, id)
        );
        CREATE TABLE IF NOT EXISTS quarantine (
            partition TEXT NOT NULL,
            cursor TEXT NOT NULL,
            record_index INTEGER NOT NULL,
            raw_json TEXT NOT NULL,
            reason TEXT NOT NULL,
            UNIQUE (partition, cursor, record_index)
        );
        CREATE TABLE IF NOT EXISTS checkpoints (
            partition TEXT PRIMARY KEY,
            next_cursor TEXT,
            done INTEGER NOT NULL CHECK (done IN (0, 1))
        );
        """
    )


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _read_checkpoints(db_path: Path) -> Optional[List[Tuple[str, Optional[str], int]]]:
    """Return checkpoint rows, [] for an empty collector DB, or None if absent."""
    if not db_path.exists():
        return None
    conn = _connect(db_path)
    try:
        if not _table_exists(conn, "checkpoints"):
            return None
        return conn.execute(
            "SELECT partition, next_cursor, done FROM checkpoints ORDER BY partition"
        ).fetchall()
    finally:
        conn.close()


def _prepare_database(db_path: Path, partitions: List[str]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        for partition in partitions:
            conn.execute(
                "INSERT OR IGNORE INTO checkpoints(partition, next_cursor, done) VALUES (?, ?, 0)",
                (partition, "start"),
            )
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def _parse_retry_after(headers) -> float:
    raw = headers.get("Retry-After")
    if not raw:
        return 1.0
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return 1.0


def _backoff_seconds(retries_used: int) -> float:
    return min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** retries_used))


def _body_snippet(body: bytes) -> str:
    return body.decode("utf-8", errors="replace")[:300].strip()


def _get_json(base_url: str, path: str, timeout: float = REQUEST_TIMEOUT_SECONDS):
    """GET JSON, honoring 429 and retrying 503/timeouts up to MAX_RETRIES."""
    url = base_url.rstrip("/") + path
    retries_used = 0
    while True:
        status = 0
        body = b""
        headers = None
        try:
            request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = response.getcode()
                body = response.read()
                headers = response.headers
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = exc.read()
            headers = exc.headers
        except (socket.timeout, TimeoutError) as exc:
            if retries_used >= MAX_RETRIES:
                raise CollectorError(
                    "GET %s 重试耗尽（已追加 %d 次重试）后仍然超时" % (url, MAX_RETRIES)
                ) from exc
            delay = _backoff_seconds(retries_used)
            retries_used += 1
            time.sleep(delay)
            continue
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            is_timeout = isinstance(reason, (socket.timeout, TimeoutError)) or "timed out" in str(reason).lower()
            if retries_used >= MAX_RETRIES:
                kind = "超时" if is_timeout else "网络错误"
                raise CollectorError(
                    "GET %s 重试耗尽（已追加 %d 次重试）后仍然%s：%s"
                    % (url, MAX_RETRIES, kind, reason)
                ) from exc
            delay = _backoff_seconds(retries_used)
            retries_used += 1
            time.sleep(delay)
            continue

        if status == 200:
            try:
                return json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CollectorError("GET %s 返回了非法 JSON：%s" % (url, exc)) from exc

        if status in (429, 503):
            if retries_used >= MAX_RETRIES:
                raise CollectorError(
                    "GET %s 重试耗尽（已追加 %d 次重试），最后状态 HTTP %d：%s"
                    % (url, MAX_RETRIES, status, _body_snippet(body))
                )
            if status == 429:
                delay = _parse_retry_after(headers)
            else:
                delay = _backoff_seconds(retries_used)
            retries_used += 1
            time.sleep(delay)
            continue

        raise CollectorError(
            "GET %s 返回非成功状态 HTTP %d：%s" % (url, status, _body_snippet(body))
        )


def _health_partitions(base_url: str) -> List[str]:
    payload = _get_json(base_url, "/health")
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise CollectorError("/health 未返回 ok=true")
    partitions = payload.get("partitions")
    if not isinstance(partitions, list):
        raise CollectorError("/health 的 partitions 不是数组")
    if not all(isinstance(item, str) and item for item in partitions):
        raise CollectorError("/health 返回了非法分区名")
    if not partitions:
        raise CollectorError("/health 未返回任何分区")
    return partitions


def _validate_page(payload, partition: str, cursor: str) -> Tuple[List[object], Optional[str]]:
    if not isinstance(payload, dict):
        raise CollectorError("分区 %s 游标 %s 的响应不是 JSON 对象" % (partition, cursor))
    if payload.get("partition") != partition:
        raise CollectorError("分区 %s 游标 %s 的响应分区不匹配" % (partition, cursor))
    if payload.get("cursor") != cursor:
        raise CollectorError("分区 %s 请求游标 %s，响应游标为 %r" % (partition, cursor, payload.get("cursor")))
    records = payload.get("records")
    if not isinstance(records, list):
        raise CollectorError("分区 %s 游标 %s 的 records 不是数组" % (partition, cursor))
    next_cursor = payload.get("next_cursor")
    if next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor):
        raise CollectorError("分区 %s 游标 %s 的 next_cursor 非法" % (partition, cursor))
    return records, next_cursor


def _record_error(record) -> Optional[str]:
    if not isinstance(record, dict):
        return "记录必须为 JSON 对象"
    record_id = record.get("id")
    if not isinstance(record_id, str) or not record_id:
        return "id 必须为非空字符串"
    revision = record.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision <= 0:
        return "revision 必须为正整数（布尔值不算整数）"
    op = record.get("op")
    if op not in ("upsert", "delete"):
        return "op 必须为 upsert 或 delete"
    if op == "upsert":
        title = record.get("title")
        if not isinstance(title, str) or not title:
            return "upsert 的 title 必须为非空字符串"
    return None


def _begin_immediate(conn: sqlite3.Connection) -> None:
    for attempt in range(20):
        try:
            conn.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt >= 19:
                raise
            time.sleep(min(0.05 * (attempt + 1), 0.5))


def _apply_record(conn: sqlite3.Connection, partition: str, record) -> None:
    record_id = record["id"]
    revision = record["revision"]
    if record["op"] == "delete":
        title = None
        deleted = 1
    else:
        title = record["title"]
        deleted = 0
    conn.execute(
        "INSERT OR IGNORE INTO announcements(partition, id, revision, title, deleted) VALUES (?, ?, ?, ?, ?)",
        (partition, record_id, revision, title, deleted),
    )
    conn.execute(
        """
        UPDATE announcements
           SET revision = ?, title = ?, deleted = ?
         WHERE partition = ? AND id = ? AND revision < ?
        """,
        (revision, title, deleted, partition, record_id, revision),
    )


def _process_partition(base_url: str, db_path: Path, partition: str, crash_target: Optional[str]) -> None:
    conn = _connect(db_path)
    seen_cursors = set()
    try:
        while True:
            row = conn.execute(
                "SELECT next_cursor, done FROM checkpoints WHERE partition = ?", (partition,)
            ).fetchone()
            if row is None:
                raise CollectorError("分区 %s 缺少 checkpoint" % partition)
            cursor, done = row
            if done:
                return
            if cursor is None:
                cursor = "start"
            if cursor in seen_cursors:
                raise CollectorError("分区 %s 检测到游标循环：%s" % (partition, cursor))
            seen_cursors.add(cursor)

            query = urllib.parse.urlencode({"partition": partition, "cursor": cursor})
            payload = _get_json(base_url, "/feed?" + query)
            records, next_cursor = _validate_page(payload, partition, cursor)

            try:
                _begin_immediate(conn)
                for record_index, record in enumerate(records):
                    error = _record_error(record)
                    if error is not None:
                        raw_json = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                        conn.execute(
                            "INSERT OR IGNORE INTO quarantine(partition, cursor, record_index, raw_json, reason) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (partition, cursor, record_index, raw_json, error),
                        )
                    else:
                        _apply_record(conn, partition, record)

                done_value = 1 if next_cursor is None else 0
                conn.execute(
                    "UPDATE checkpoints SET next_cursor = ?, done = ? WHERE partition = ?",
                    (next_cursor, done_value, partition),
                )

                if crash_target == "%s:%s" % (partition, cursor):
                    print("FAULT_INJECTION %s before_commit" % crash_target, flush=True)
                    os._exit(CRASH_EXIT_CODE)

                conn.commit()
            except BaseException:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                raise

            if next_cursor is None:
                return
    finally:
        conn.close()


def _run(base_url: str, db_path: Path, workers: int, crash_target: Optional[str]) -> None:
    checkpoint_rows = _read_checkpoints(db_path)
    if checkpoint_rows:
        if all(row[2] for row in checkpoint_rows):
            print("所有分区均已完成，无需请求 /feed", flush=True)
            return
        partitions = [row[0] for row in checkpoint_rows]
        _prepare_database(db_path, partitions)
    else:
        partitions = _health_partitions(base_url)
        _prepare_database(db_path, partitions)

    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_partition, base_url, db_path, partition, crash_target): partition
            for partition in partitions
        }
        for future in as_completed(futures):
            partition = futures[future]
            try:
                future.result()
            except CollectorError as exc:
                errors.append("分区 %s：%s" % (partition, exc))
            except Exception as exc:
                errors.append("分区 %s：%s: %s" % (partition, type(exc).__name__, exc))

    if errors:
        raise CollectorError("；".join(errors))
    print("采集完成：%s" % ", ".join(partitions), flush=True)


def _status(db_path: Path) -> Dict[str, object]:
    result: Dict[str, object] = {
        "total": 0,
        "active": 0,
        "deleted": 0,
        "quarantined": 0,
        "partitions": {},
    }
    if not db_path.exists():
        return result
    conn = _connect(db_path)
    try:
        if all(_table_exists(conn, name) for name in ("announcements", "quarantine", "checkpoints")):
            result["total"] = conn.execute("SELECT COUNT(*) FROM announcements").fetchone()[0]
            result["active"] = conn.execute(
                "SELECT COUNT(*) FROM announcements WHERE deleted = 0"
            ).fetchone()[0]
            result["deleted"] = conn.execute(
                "SELECT COUNT(*) FROM announcements WHERE deleted = 1"
            ).fetchone()[0]
            result["quarantined"] = conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
            partitions: Dict[str, object] = {}
            for partition, next_cursor, done in conn.execute(
                "SELECT partition, next_cursor, done FROM checkpoints ORDER BY partition"
            ):
                partitions[partition] = {"next_cursor": next_cursor, "done": bool(done)}
            result["partitions"] = partitions
    finally:
        conn.close()
    return result


def _reset(db_path: Path) -> None:
    if not db_path.exists():
        return
    conn = _connect(db_path)
    try:
        tables = [
            name
            for name in ("announcements", "quarantine", "checkpoints")
            if _table_exists(conn, name)
        ]
        if not tables:
            return
        _begin_immediate(conn)
        for name in tables:
            conn.execute("DELETE FROM %s" % name)
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="本地公告增量采集器")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="增量采集两个分区")
    run_parser.add_argument("--base-url", required=True)
    run_parser.add_argument("--db", required=True)
    run_parser.add_argument("--workers", type=int, default=2)
    run_parser.add_argument("--crash-before-commit", default=None)

    status_parser = subparsers.add_parser("status", help="输出机器可读状态")
    status_parser.add_argument("--db", required=True)

    reset_parser = subparsers.add_parser("reset", help="清空指定采集库的数据")
    reset_parser.add_argument("--db", required=True)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    db_path = Path(args.db)
    try:
        if args.command == "run":
            if args.workers < 2:
                raise CollectorError("--workers 至少为 2")
            _run(args.base_url, db_path, args.workers, args.crash_before_commit)
        elif args.command == "status":
            print(json.dumps(_status(db_path), ensure_ascii=False), flush=True)
        elif args.command == "reset":
            _reset(db_path)
            print("已重置采集库数据：%s" % db_path, flush=True)
    except CollectorError as exc:
        print("采集失败：%s" % exc, file=sys.stderr, flush=True)
        return 1
    except (sqlite3.Error, OSError) as exc:
        print("命令失败：%s" % exc, file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
