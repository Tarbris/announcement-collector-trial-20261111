#!/usr/bin/env python3
"""Print collector table contents as JSON for demos and manual inspection."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        print(json.dumps({"announcements": [], "quarantine": [], "checkpoints": []}, ensure_ascii=False))
        return 0

    conn = sqlite3.connect(str(db_path))
    try:
        announcements = [
            {"partition": row[0], "id": row[1], "revision": row[2], "title": row[3], "deleted": row[4]}
            for row in conn.execute(
                "SELECT partition, id, revision, title, deleted FROM announcements ORDER BY partition, id"
            )
        ]
        quarantine = [
            {
                "partition": row[0],
                "cursor": row[1],
                "record_index": row[2],
                "raw_json": row[3],
                "reason": row[4],
            }
            for row in conn.execute(
                "SELECT partition, cursor, record_index, raw_json, reason "
                "FROM quarantine ORDER BY partition, cursor, record_index"
            )
        ]
        checkpoints = [
            {"partition": row[0], "next_cursor": row[1], "done": bool(row[2])}
            for row in conn.execute(
                "SELECT partition, next_cursor, done FROM checkpoints ORDER BY partition"
            )
        ]
    finally:
        conn.close()

    print(
        json.dumps(
            {"announcements": announcements, "quarantine": quarantine, "checkpoints": checkpoints},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
