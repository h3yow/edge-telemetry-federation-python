"""Batch dashboard tests: multi-device query, cluster scope, fleet summary,
scale, and read consistency under concurrent writes.
"""

import logging
import random
import threading

from telemetry.federation import Federation
from telemetry.models import Batch, TelemetryRecord
from telemetry.store import ClusterStore

log = logging.getLogger("test.dashboard")
NOW = 10_000.0


def rec(device, seq, value=None, ts=NOW - 10, metric="cpu", rate=1.0):
    return TelemetryRecord(device, seq, ts, metric,
                           float(seq) if value is None else value, rate)


def make_federation(cluster_ids=("c1", "c2")):
    fed = Federation()
    for cid in cluster_ids:
        fed.add_cluster(ClusterStore(cid))
    return fed


def log_batch(tag, result):
    s = result.summary
    log.info(
        "%s -> devices=%s with_data=%s records=%s value_sum=%s "
        "complete_devs=%s conflict_devs=%s conflict_seqs=%s online=%s/%s",
        tag, s.devices_total, s.devices_with_data, s.record_count,
        s.value_sum, s.complete_devices, s.devices_with_conflicts,
        s.conflict_seqs, s.clusters_online, s.clusters_total,
    )
    for v in result.devices:
        st = v.stats
        log.info(
            "  device=%s count=%s sum=%s missing=%s complete=%s conflicts=%s",
            v.device_id, st.record_count if st else None,
            st.value_sum if st else None,
            st.missing_seqs if st else None, v.complete,
            [c.seq for c in v.conflicts],
        )


def assert_summary_matches(devices, summary):
    """The summary must always equal the rollup of its device views."""
    assert summary.record_count == sum(
        v.stats.record_count for v in devices if v.stats
    )
    assert summary.devices_with_data == sum(1 for v in devices if v.stats)
    assert summary.devices_with_conflicts == sum(
        1 for v in devices if v.conflicts
    )
    assert summary.conflict_seqs == sum(len(v.conflicts) for v in devices)
    for v in devices:
        if v.stats:
            conflict_set = {c.seq for c in v.conflicts}
            assert conflict_set.isdisjoint(v.stats.missing_seqs)


def test_query_by_explicit_device_ids_including_unknown():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec("d1", 1, 1.0), rec("d1", 2, 2.0))), now=NOW)
    fed.ingest(Batch("c2", (rec("d2", 1, 4.0),)), now=NOW)
    result = fed.query_devices(device_ids=["d2", "d1", "ghost"])
    log_batch("explicit-ids", result)
    by_id = {v.device_id: v for v in result.devices}
    assert list(by_id) == ["d1", "d2", "ghost"]     # sorted, unknown kept
    assert by_id["ghost"].stats is None
    assert by_id["d1"].stats.record_count == 2
    assert by_id["d2"].stats.value_sum == 4.0
    assert result.summary.devices_total == 3
    assert result.summary.devices_with_data == 2
    assert result.summary.record_count == 3
    assert_summary_matches(result.devices, result.summary)


def test_query_all_devices_and_conflict_rollup():
    fed = make_federation(("c1", "c2", "c3"))
    fed.ingest(Batch("c1", (rec("d1", 1, 1.0), rec("d1", 2, 2.0))), now=NOW)
    fed.ingest(Batch("c2", (rec("d1", 1, 9.9), rec("d2", 1, 5.0))), now=NOW)
    fed.ingest(Batch("c3", (rec("d2", 1, 5.0),)), now=NOW)
    result = fed.query_devices()
    log_batch("all-devices", result)
    by_id = {v.device_id: v for v in result.devices}
    assert set(by_id) == {"d1", "d2"}
    # d1: seq1 conflict quarantined, seq2 clean
    assert by_id["d1"].stats.record_count == 1
    assert by_id["d1"].conflicts[0].seq == 1
    # d2: identical across c2/c3 counts once
    assert by_id["d2"].stats.record_count == 1
    assert by_id["d2"].conflicts == ()
    assert result.summary.devices_with_conflicts == 1
    assert result.summary.conflict_seqs == 1
    assert result.summary.complete_devices == 2
    assert_summary_matches(result.devices, result.summary)


def test_cluster_scope_still_merges_all_online_clusters():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec("d1", 1, 1.0), rec("d3", 1, 3.0))), now=NOW)
    fed.ingest(Batch("c2", (rec("d1", 1, 1.0), rec("d1", 2, 2.0),
                            rec("d2", 1, 2.0))), now=NOW)
    # scope = c1 -> devices d1,d3; but d1 is still merged over c1+c2
    result = fed.query_devices(cluster_ids=["c1"])
    log_batch("cluster-scope-c1", result)
    by_id = {v.device_id: v for v in result.devices}
    assert set(by_id) == {"d1", "d3"}
    assert by_id["d1"].stats.record_count == 2
    assert by_id["d1"].stats.value_sum == 3.0
    assert_summary_matches(result.devices, result.summary)


def test_offline_cluster_marks_every_view_incomplete():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec("d1", 1, 1.0),)), now=NOW)
    fed.ingest(Batch("c2", (rec("d2", 1, 2.0),)), now=NOW)
    fed.set_online("c2", False)
    result = fed.query_devices()
    log_batch("c2-offline", result)
    assert all(v.complete is False for v in result.devices)
    assert all(v.clusters_missing == ("c2",) for v in result.devices)
    assert result.summary.clusters_missing == ("c2",)
    assert result.summary.complete_devices == 0
    assert_summary_matches(result.devices, result.summary)


def test_dashboard_scales_to_many_devices():
    fed = make_federation()
    n_devices, n_seq = 500, 20
    for d in range(n_devices):
        fed.ingest(Batch(
            "c1", tuple(rec(f"dev-{d:04d}", s) for s in range(n_seq))
        ), now=NOW)
    result = fed.query_devices()
    log_batch("scale", result)
    assert len(result.devices) == n_devices
    assert result.summary.record_count == n_devices * n_seq
    assert result.summary.devices_with_data == n_devices
    assert result.summary.complete_devices == n_devices
    assert_summary_matches(result.devices, result.summary)


def test_batch_reads_stay_consistent_under_concurrent_writes():
    fed = make_federation(("c1", "c2"))
    devices = [f"dev-{d}" for d in range(30)]
    stop = threading.Event()

    def writer(seed):
        rng = random.Random(seed)
        i = 0
        while not stop.is_set():
            d = devices[rng.randrange(len(devices))]
            seq = i % 20                       # same deterministic value/seq
            cid = rng.choice(("c1", "c2"))
            # every (device, seq) is reported to both clusters with identical
            # payload, so no conflicts: counts must stay coherent at all times
            fed.ingest(Batch(cid, (rec(d, seq),)), now=NOW)
            i += 1

    threads = [threading.Thread(target=writer, args=(s,)) for s in range(6)]
    for t in threads:
        t.start()
    try:
        for _ in range(200):
            result = fed.query_devices(device_ids=devices)
            # point-in-time invariants on every observed snapshot
            assert len(result.devices) == len(devices)
            assert_summary_matches(result.devices, result.summary)
            for v in result.devices:
                if v.stats:
                    # every seq must fall within [0, 20)
                    assert v.stats.seq_min >= 0 and v.stats.seq_max < 20
                    # value == seq deterministically -> sum over distinct seqs
                    expect = sum(range(v.stats.seq_min, v.stats.seq_max + 1))
                    missing = set(v.stats.missing_seqs)
                    expect -= sum(missing)
                    assert v.stats.value_sum == expect
    finally:
        stop.set()
        for t in threads:
            t.join()
    log_batch("after-concurrent-rw", fed.query_devices(device_ids=devices))
