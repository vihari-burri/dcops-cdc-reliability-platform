from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class ApplyResult:
    status: str
    operation: str | None = None


class CDCConsumer:
    """Apply Debezium-style envelopes with offset-level exactly-once effects."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.db = connection
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS work_orders (
              work_order_id TEXT PRIMARY KEY, equipment_id TEXT NOT NULL, status TEXT NOT NULL,
              priority TEXT NOT NULL, updated_at TEXT NOT NULL, source_lsn INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_offsets (
              topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
              processed_at TEXT NOT NULL, PRIMARY KEY(topic, partition_id, offset_id)
            );
            CREATE TABLE IF NOT EXISTS dead_letter (
              id INTEGER PRIMARY KEY, payload TEXT NOT NULL, reason TEXT NOT NULL,
              topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
              metric TEXT PRIMARY KEY, value INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO pipeline_metrics VALUES ('events_applied', 0), ('duplicates', 0), ('failures', 0);
            """
        )

    def apply(self, payload: str, *, topic: str, partition: int, offset: int) -> ApplyResult:
        if self.db.execute(
            "SELECT 1 FROM processed_offsets WHERE topic=? AND partition_id=? AND offset_id=?",
            (topic, partition, offset),
        ).fetchone():
            self._increment("duplicates")
            self.db.commit()
            return ApplyResult("duplicate")
        try:
            event = json.loads(payload)
            operation = event["op"]
            if operation not in {"c", "u", "d", "r"}:
                raise ValueError(f"unsupported operation: {operation}")
            source_lsn = int(event["source"]["lsn"])
            if operation == "d":
                key = event["before"]["work_order_id"]
                self.db.execute("DELETE FROM work_orders WHERE work_order_id=?", (key,))
            else:
                row: dict[str, Any] = event["after"]
                required = {"work_order_id", "equipment_id", "status", "priority", "updated_at"}
                missing = required - row.keys()
                if missing:
                    raise ValueError(f"missing fields: {sorted(missing)}")
                self.db.execute(
                    """
                    INSERT INTO work_orders VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(work_order_id) DO UPDATE SET
                      equipment_id=excluded.equipment_id, status=excluded.status,
                      priority=excluded.priority, updated_at=excluded.updated_at,
                      source_lsn=excluded.source_lsn
                    WHERE excluded.source_lsn > work_orders.source_lsn
                    """,
                    (row["work_order_id"], row["equipment_id"], row["status"],
                     row["priority"], row["updated_at"], source_lsn),
                )
            self.db.execute(
                "INSERT INTO processed_offsets VALUES (?, ?, ?, ?)",
                (topic, partition, offset, datetime.now(UTC).isoformat()),
            )
            self._increment("events_applied")
            self.db.commit()
            return ApplyResult("applied", operation)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            self.db.execute(
                "INSERT INTO dead_letter(payload, reason, topic, partition_id, offset_id) VALUES (?, ?, ?, ?, ?)",
                (payload, str(exc), topic, partition, offset),
            )
            self._increment("failures")
            self.db.commit()
            return ApplyResult("dead_letter")

    def prometheus_text(self, *, latest_source_epoch: float, now_epoch: float) -> str:
        values = dict(self.db.execute("SELECT metric, value FROM pipeline_metrics"))
        lag = max(0.0, now_epoch - latest_source_epoch)
        return "\n".join([
            "# HELP dcops_cdc_events_applied_total Successfully applied CDC envelopes.",
            "# TYPE dcops_cdc_events_applied_total counter",
            f"dcops_cdc_events_applied_total {values['events_applied']}",
            f"dcops_cdc_duplicates_total {values['duplicates']}",
            f"dcops_cdc_failures_total {values['failures']}",
            f"dcops_cdc_freshness_lag_seconds {lag:.3f}",
            "",
        ])

    def _increment(self, metric: str) -> None:
        self.db.execute("UPDATE pipeline_metrics SET value=value+1 WHERE metric=?", (metric,))

