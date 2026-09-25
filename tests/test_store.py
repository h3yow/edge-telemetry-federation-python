"""Ingest / merge rule tests: duplicates, out-of-order, out-of-range rejection."""

import logging

import pytest

from telemetry.models import Batch, Decision, RejectReason, TelemetryRecord
from telemetry.store import ClusterStore

log = logging.getLogger("test.store")
NOW = 10_000.0


def rec(device="d1", seq=1, ts=NOW - 10, metric="cpu", value=1.0, rate=1.0):
    return TelemetryRecord(device, seq, ts, metric, value, rate)


def log_result(tag, batch, result):
    for d in result.decisions:
        log.info(
            "%s input=(dev=%s seq=%s ts=%s value=%s rate=%s) -> %s reason=%s detail=%s",
            tag, d.record.device_id, d.record.seq, d.record.timestamp,
            d.record.value, d.record.sample_rate,
            d.decision.value, d.reason.value if d.reason else "-", d.detail,
        )
    assert len(result.decisions) == len(batch.records)


@pytest.fixture
def store():
    return ClusterStore("c1", max_seq_gap=5, max_past_seconds=100.0,
                        max_future_seconds=10.0)


def test_duplicate_report_is_idempotent(store):
    batch = Batch("c1", (rec(seq=1), rec(seq=2), rec(seq=3)))
    r1 = store.ingest(batch, now=NOW)
    log_result("first-report", batch, r1)
    assert (r1.accepted, r1.duplicates, r1.rejected) == (3, 0, 0)

    r2 = store.ingest(batch, now=NOW)  # exact replay
    log_result("replay", batch, r2)
    assert (r2.accepted, r2.duplicates, r2.rejected) == (0, 3, 0)

    stats = store.device_stats("d1")
    assert stats.record_count == 3 and stats.value_sum == pytest.approx(3.0)


def test_out_of_order_and_late_arrival_merge_deterministically(store):
    batches = [
        Batch("c1", (rec(seq=3, value=3.0), rec(seq=1, value=1.0))),
        Batch("c1", (rec(seq=2, value=2.0),)),  # late, still within window
    ]
    for i, b in enumerate(batches):
        r = store.ingest(b, now=NOW)
        log_result(f"batch-{i}", b, r)
        assert r.rejected == 0
    stats = store.device_stats("d1")
    assert stats.record_count == 3
    assert (stats.seq_min, stats.seq_max) == (1, 3)
    assert stats.missing_seqs == ()
    assert stats.value_sum == pytest.approx(6.0)


def test_conflicting_duplicate_is_rejected(store):
    r1 = store.ingest(Batch("c1", (rec(seq=1, value=1.0),)), now=NOW)
    log_result("original", Batch("c1", (rec(seq=1, value=1.0),)), r1)
    conflict = Batch("c1", (rec(seq=1, value=9.9),))
    r2 = store.ingest(conflict, now=NOW)
    log_result("conflict", conflict, r2)
    assert r2.rejected == 1
    assert r2.decisions[0].reason is RejectReason.DUPLICATE_CONFLICT
    assert store.device_stats("d1").value_sum == pytest.approx(1.0)


def test_out_of_range_rejections_have_distinct_reasons(store):
    store.ingest(Batch("c1", (rec(seq=10),)), now=NOW)  # set high watermark
    cases = [
        (rec(seq=1), RejectReason.STALE_SEQUENCE),            # 1 < 10 - 5
        (rec(seq=99), RejectReason.SEQ_TOO_FAR_AHEAD),        # 99 > 10 + 5
        (rec(seq=11, ts=NOW - 500), RejectReason.TIMESTAMP_OUT_OF_RANGE),
        (rec(seq=11, ts=NOW + 500), RejectReason.TIMESTAMP_OUT_OF_RANGE),
        (rec(seq=11, rate=0.0), RejectReason.INVALID_SAMPLE_RATE),
        (rec(seq=11, rate=1.5), RejectReason.INVALID_SAMPLE_RATE),
    ]
    reasons = []
    for record, expected in cases:
        b = Batch("c1", (record,))
        r = store.ingest(b, now=NOW)
        log_result("reject-case", b, r)
        assert r.rejected == 1
        assert r.decisions[0].reason is expected
        reasons.append(r.decisions[0].reason)
    # reasons are machine-distinguishable
    assert len(set(reasons)) == 4
    assert store.device_stats("d1").record_count == 1  # nothing extra stored


def test_sampling_weight_reflected_in_stats(store):
    b = Batch("c1", (rec(seq=1, rate=1.0), rec(seq=2, rate=0.5), rec(seq=3, rate=0.25)))
    r = store.ingest(b, now=NOW)
    log_result("sampled", b, r)
    stats = store.device_stats("d1")
    assert stats.record_count == 3
    # weight = 1 / sample_rate -> 1 + 2 + 4
    assert stats.weighted_count == pytest.approx(7.0)
