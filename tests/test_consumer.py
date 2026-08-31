import json
import sqlite3

from dcops_cdc.consumer import CDCConsumer


def envelope(op="c", lsn=100, after=None, before=None):
    return json.dumps({
        "op": op, "source": {"lsn": lsn},
        "before": before,
        "after": after or {
            "work_order_id": "wo-1", "equipment_id": "AHU-1", "status": "OPEN",
            "priority": "HIGH", "updated_at": "2026-08-01T00:00:00Z",
        },
    })


def test_create_update_delete_and_replay():
    db = sqlite3.connect(":memory:")
    consumer = CDCConsumer(db)
    assert consumer.apply(envelope(), topic="maintenance", partition=0, offset=1).status == "applied"
    assert consumer.apply(envelope(), topic="maintenance", partition=0, offset=1).status == "duplicate"
    updated = json.loads(envelope(op="u", lsn=101))
    updated["after"]["status"] = "CLOSED"
    consumer.apply(json.dumps(updated), topic="maintenance", partition=0, offset=2)
    assert db.execute("select status from work_orders").fetchone()[0] == "CLOSED"
    consumer.apply(envelope(op="d", lsn=102, before={"work_order_id": "wo-1"}), topic="maintenance", partition=0, offset=3)
    assert db.execute("select count(*) from work_orders").fetchone()[0] == 0


def test_out_of_order_change_cannot_overwrite_newer_state():
    db = sqlite3.connect(":memory:")
    consumer = CDCConsumer(db)
    consumer.apply(envelope(lsn=200), topic="maintenance", partition=0, offset=1)
    old = json.loads(envelope(op="u", lsn=150))
    old["after"]["status"] = "STALE"
    consumer.apply(json.dumps(old), topic="maintenance", partition=0, offset=2)
    assert db.execute("select status from work_orders").fetchone()[0] == "OPEN"


def test_dlq_and_metrics():
    db = sqlite3.connect(":memory:")
    consumer = CDCConsumer(db)
    assert consumer.apply("not-json", topic="maintenance", partition=0, offset=1).status == "dead_letter"
    metrics = consumer.prometheus_text(latest_source_epoch=90, now_epoch=100)
    assert "dcops_cdc_failures_total 1" in metrics
    assert "dcops_cdc_freshness_lag_seconds 10.000" in metrics

