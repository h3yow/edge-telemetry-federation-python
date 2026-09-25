"""单集群归并规则测试：接受/重复/冲突/越界/迟到/采样率。"""

from __future__ import annotations

import logging

import pytest

from edge_telemetry.models import (
    IngestStatus,
    RejectReason,
    SeriesPolicy,
    StorePolicy,
    TelemetryRecord,
)
from edge_telemetry.store import ClusterStore

logger = logging.getLogger("telemetry.tests")

NOW = 1_000_000


def log_batch(title: str, records, result) -> None:
    logger.info("=== %s ===", title)
    for r in records:
        logger.info("INPUT cluster=%s device=%s seq=%s no=%s ts=%s value=%s",
                    r.cluster_id, r.device_id, r.seq_id, r.seq_no,
                    r.timestamp_ms, r.value)
    for o in result.outcomes:
        logger.info(
            "DECISION no=%s status=%s reason=%s detail=%s evidence=%s",
            o.key.seq_no, o.status.value,
            o.reason.value if o.reason else None, o.detail, o.evidence)


def R(no, ts, value=1.0, *, device="d1", seq="temp", cluster="c1"):
    return TelemetryRecord(
        cluster_id=cluster, device_id=device, seq_id=seq,
        seq_no=no, timestamp_ms=ts, value=value)


def test_accept_and_stats():
    store = ClusterStore("c1")
    batch = [R(1, NOW - 100, 3.0), R(2, NOW - 90, 5.0), R(3, NOW - 80, 4.0)]
    res = store.ingest_batch(batch, NOW)
    log_batch("accept", batch, res)
    assert (res.accepted, res.duplicate, res.rejected) == (3, 0, 0)
    snap = store.snapshot()
    stats = snap.devices["d1"]["temp"]
    assert stats.count == 3
    assert (stats.min_value, stats.max_value) == (3.0, 5.0)
    assert stats.sum_value == 12.0
    assert (stats.first_seq_no, stats.last_seq_no) == (1, 3)
    assert (stats.min_timestamp_ms, stats.max_timestamp_ms) == (NOW - 100, NOW - 80)


def test_duplicate_idempotent():
    store = ClusterStore("c1")
    b1 = [R(1, NOW - 100, 3.0), R(2, NOW - 90, 5.0)]
    b2 = [R(2, NOW - 90, 5.0), R(1, NOW - 100, 3.0)]  # 乱序重复补报
    r1 = store.ingest_batch(b1, NOW)
    r2 = store.ingest_batch(b2, NOW)
    log_batch("duplicate-batch2", b2, r2)
    assert (r1.accepted, r1.duplicate) == (2, 0)
    assert (r2.accepted, r2.duplicate, r2.rejected) == (0, 2, 0)
    assert store.snapshot().devices["d1"]["temp"].count == 2


def test_seq_conflict_distinguished_from_duplicate():
    store = ClusterStore("c1")
    store.ingest_batch([R(1, NOW - 100, 3.0)], NOW)
    b = [R(1, NOW - 100, 3.5)]  # 同序号不同值 => 自相矛盾
    res = store.ingest_batch(b, NOW)
    log_batch("conflict", b, res)
    assert res.rejected == 1
    assert res.outcomes[0].reason is RejectReason.SEQ_CONFLICT
    assert store.snapshot().devices["d1"]["temp"].count == 1


def test_value_out_of_range():
    store = ClusterStore("c1", StorePolicy(
        default=SeriesPolicy(value_min=0.0, value_max=100.0)))
    b = [R(1, NOW - 10, 120.0)]
    res = store.ingest_batch(b, NOW)
    log_batch("out-of-range", b, res)
    assert res.outcomes[0].reason is RejectReason.VALUE_OUT_OF_RANGE
    assert res.outcomes[0].evidence["value_max"] == 100.0


def test_future_skew():
    store = ClusterStore("c1", StorePolicy(
        default=SeriesPolicy(future_skew_ms=500)))
    b = [R(1, NOW + 501)]
    res = store.ingest_batch(b, NOW)
    log_batch("future", b, res)
    assert res.outcomes[0].reason is RejectReason.FUTURE_SKEW
    assert res.outcomes[0].evidence["future_limit_ms"] == NOW + 500


def test_out_of_order_inside_window_merged():
    store = ClusterStore("c1", StorePolicy(
        default=SeriesPolicy(reorder_delay_ms=1000)))
    b1 = [R(3, NOW, 5.0)]
    b2 = [R(2, NOW - 500, 4.0)]  # 迟到但在窗口内
    store.ingest_batch(b1, NOW)
    r2 = store.ingest_batch(b2, NOW)
    log_batch("late-inside", b2, r2)
    assert r2.accepted == 1
    assert store.snapshot().devices["d1"]["temp"].count == 2


def test_outside_reorder_window_rejected():
    store = ClusterStore("c1", StorePolicy(
        default=SeriesPolicy(reorder_delay_ms=1000)))
    store.ingest_batch([R(3, NOW, 5.0)], NOW)
    b = [R(2, NOW - 1500, 4.0)]  # 超出迟到窗口
    res = store.ingest_batch(b, NOW)
    log_batch("late-outside", b, res)
    assert res.outcomes[0].reason is RejectReason.OUTSIDE_REORDER_WINDOW
    ev = res.outcomes[0].evidence
    assert ev["watermark_ms"] == NOW and ev["late_limit_ms"] == NOW - 1000


def test_sample_rate_violation():
    store = ClusterStore("c1", StorePolicy(
        default=SeriesPolicy(min_sample_interval_ms=100)))
    b1 = [R(1, NOW - 200, 1.0)]
    b2 = [R(2, NOW - 150, 1.0)]  # 与上一条仅相距 50ms
    store.ingest_batch(b1, NOW)
    r2 = store.ingest_batch(b2, NOW)
    log_batch("sample-rate", b2, r2)
    assert r2.outcomes[0].reason is RejectReason.SAMPLE_RATE_VIOLATION
    assert r2.outcomes[0].evidence["gap_ms"] == 50


def test_invalid_timestamp_and_offline():
    store = ClusterStore("c1")
    bad = TelemetryRecord("c1", "d1", "temp", 1, -1, 1.0)
    res = store.ingest_batch([bad], NOW)
    assert res.outcomes[0].reason is RejectReason.INVALID_TIMESTAMP

    store.set_online(False)
    res2 = store.ingest_batch([R(1, NOW)], NOW)
    assert res2.outcomes[0].reason is RejectReason.CLUSTER_OFFLINE


@pytest.mark.parametrize("order", [[0, 1, 2], [2, 1, 0], [1, 0, 2]])
def test_batch_reorder_independent(order):
    base = [R(1, NOW - 300, 1.0), R(2, NOW - 200, 2.0), R(3, NOW - 100, 3.0)]
    store = ClusterStore("c1", StorePolicy(
        default=SeriesPolicy(reorder_delay_ms=10_000)))
    res = store.ingest_batch([base[i] for i in order], NOW)
    log_batch(f"reorder-{order}", [base[i] for i in order], res)
    assert res.accepted == 3
    assert store.snapshot().devices["d1"]["temp"].count == 3
