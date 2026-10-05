"""New behaviour: cross-cluster conflicts, recovery re-report, batch dashboard,
concurrent read/write consistency.

Each round logs exactly what was fed in and the resulting verdict, so a test
run shows, step by step, the input records and the merged conclusion.
"""

from __future__ import annotations

import logging
import threading

import pytest

from telemetry.federation import Federation
from telemetry.models import Batch, TelemetryRecord
from telemetry.store import ClusterStore

log = logging.getLogger("test.conflicts")
NOW = 10_000.0


def rec(device="d1", seq=1, ts=NOW - 10, metric="cpu", value=1.0, rate=1.0):
    return TelemetryRecord(device, seq, ts, metric, value, rate)


def make_federation(cluster_ids=("c1", "c2", "c3")):
    fed = Federation()
    for cid in cluster_ids:
        fed.add_cluster(ClusterStore(cid, max_seq_gap=1000))
    return fed


def feed(fed, tag, cid, records, now=NOW):
    """Ingest one round and log the input plus per-record verdicts."""
    log.info(
        "%s feed cluster=%s records=%s",
        tag, cid,
        [(r.device_id, r.seq, r.value, r.sample_rate) for r in records],
    )
    result = fed.ingest(Batch(cid, tuple(records)), now=now)
    for d in result.decisions:
        log.info(
            "%s   -> dev=%s seq=%s value=%s : %s/%s %s",
            tag, d.record.device_id, d.record.seq, d.record.value,
            d.decision.value,
            d.reason.value if d.reason else "-", d.detail,
        )
    return result


def log_query(tag, q):
    s = q.stats
    log.info(
        "%s query dev=%s count=%s weighted=%s value_sum=%s seq=[%s..%s] "
        "missing=%s complete=%s online=%s/%s missing_clusters=%s conflicts=%s",
        tag, getattr(s, "device_id", None),
        s.record_count if s else None,
        s.weighted_count if s else None,
        s.value_sum if s else None,
        s.seq_min if s else None, s.seq_max if s else None,
        s.missing_seqs if s else None,
        q.complete, q.clusters_online, q.clusters_total,
        q.clusters_missing,
        [(c.seq, list(c.cluster_ids), c.version_count) for c in q.conflicts],
    )


# ---------------------------------------------------------------------------
# 1. Cross-cluster inconsistency
# ---------------------------------------------------------------------------

def test_cross_cluster_conflict_is_quarantined_not_picked_or_merged():
    fed = make_federation()
    feed(fed, "round1", "c1", [rec(seq=1, value=1.0), rec(seq=2, value=2.0)])
    feed(fed, "round1", "c2", [rec(seq=1, value=1.0)])            # identical dup
    feed(fed, "round1", "c3", [rec(seq=2, value=9.0), rec(seq=3, value=3.0)])

    q = fed.query_device("d1")
    log_query("verdict", q)

    # seq1 agrees across c1/c2 -> counted once; seq2 conflicts -> excluded;
    # seq3 unique -> counted. Stats only cover seq1 and seq3.
    assert q.stats.record_count == 2
    assert q.stats.value_sum == pytest.approx(4.0)
    assert q.has_conflicts is True
    assert len(q.conflicts) == 1
    conflict = q.conflicts[0]
    assert conflict.seq == 2
    assert conflict.cluster_ids == ("c1", "c3")
    assert conflict.version_count == 2
    assert {v.value for v in conflict.versions} == {2.0, 9.0}
    # conflicting seq must not masquerade as a missing seq
    assert 2 not in q.stats.missing_seqs
    assert q.complete is True  # all clusters online; conflict != incompleteness


def test_conflict_is_distinct_from_offline_missing():
    fed = make_federation()
    feed(fed, "setup", "c1", [rec(seq=1, value=1.0)])
    feed(fed, "setup", "c2", [rec(seq=2, value=2.0)])

    # take c3 offline: no conflict exists, but the view is incomplete
    fed.set_online("c3", False)
    q_missing = fed.query_device("d1")
    log_query("offline-c3", q_missing)
    assert q_missing.conflicts == ()
    assert q_missing.complete is False
    assert q_missing.clusters_missing == ("c3",)

    # now a real cross-cluster conflict with everyone online
    fed.set_online("c3", True)
    feed(fed, "setup", "c3", [rec(seq=1, value=7.7)])
    q_conflict = fed.query_device("d1")
    log_query("conflict-c1-c3", q_conflict)
    assert q_conflict.complete is True
    assert q_conflict.clusters_missing == ()
    assert len(q_conflict.conflicts) == 1
    assert q_conflict.conflicts[0].seq == 1


def test_three_versions_conflict_reports_all_clusters():
    fed = make_federation()
    feed(fed, "v", "c1", [rec(seq=5, value=1.0)])
    feed(fed, "v", "c2", [rec(seq=5, value=2.0)])
    feed(fed, "v", "c3", [rec(seq=5, value=3.0)])
    q = fed.query_device("d1")
    log_query("three-versions", q)
    assert q.stats is None  # the only collection seq is fully quarantined
    c = q.conflicts[0]
    assert c.seq == 5 and c.version_count == 3
    assert c.cluster_ids == ("c1", "c2", "c3")
    assert [v.value for v in c.versions] == [1.0, 2.0, 3.0]


def test_identical_reports_even_during_conflict_still_count_once():
    fed = make_federation(("c1", "c2", "c3", "c4"))
    feed(fed, "agree", "c1", [rec(seq=1, value=1.0)])
    feed(fed, "agree", "c2", [rec(seq=1, value=1.0)])
    feed(fed, "agree", "c3", [rec(seq=1, value=1.0)])
    feed(fed, "differ", "c4", [rec(seq=1, value=2.0)])
    q = fed.query_device("d1")
    log_query("3-agree-1-differs", q)
    assert q.stats is None  # any divergence quarantines the whole seq
    assert q.conflicts[0].cluster_ids == ("c1", "c2", "c3", "c4")


# ---------------------------------------------------------------------------
# 2. Recovery re-report stability
# ---------------------------------------------------------------------------

def test_recovery_identical_replay_is_stable_across_queries_and_order():
    fed = make_federation()
    feed(fed, "initial", "c2", [rec(seq=1, value=1.0), rec(seq=2, value=2.0)])
    before = fed.query_device("d1")

    fed.set_online("c2", False)
    log.info("c2 offline")
    offline = fed.query_device("d1")
    log_query("during-outage", offline)
    assert offline.complete is False

    fed.set_online("c2", True)
    log.info("c2 recovered")
    replay = feed(fed, "replay", "c2",
                  [rec(seq=2, value=2.0), rec(seq=1, value=1.0)])  # reordered
    assert replay.duplicates == 2 and replay.accepted == 0

    after_a = fed.query_device("d1")
    after_b = fed.query_device("d1")  # repeated queries must not drift
    log_query("after-replay-1", after_a)
    log_query("after-replay-2", after_b)
    assert after_a.stats.record_count == before.stats.record_count == 2
    assert after_a.stats.value_sum == pytest.approx(before.stats.value_sum)
    assert after_a.conflicts == ()
    assert (after_a.stats.record_count, after_a.stats.value_sum) == (
        after_b.stats.record_count, after_b.stats.value_sum,
    )


def test_recovery_divergent_replay_becomes_stable_conflict():
    # c1 holds seq1=1.0; c2 went offline before ever receiving it. On recovery
    # c2 back-fills the same seq with different content -> the federation now
    # has two cluster versions and must quarantine the seq stably.
    fed = make_federation()
    feed(fed, "initial", "c1", [rec(seq=1, value=1.0)])
    fed.set_online("c2", False)
    fed.set_online("c2", True)
    feed(fed, "backfill-differs", "c2", [rec(seq=1, value=5.0)])

    q1, q2 = fed.query_device("d1"), fed.query_device("d1")
    log_query("divergent-1", q1)
    log_query("divergent-2", q2)
    for q in (q1, q2):
        assert q.stats is None
        assert len(q.conflicts) == 1
        assert q.conflicts[0].cluster_ids == ("c1", "c2")
        assert {v.value for v in q.conflicts[0].versions} == {1.0, 5.0}


def test_recovery_replay_against_own_stored_conflict_is_rejected_not_mutated():
    # If the recovering cluster already stored the seq, a divergent re-report
    # is rejected at ingest (single-store rule); the federated conclusion can
    # never silently mutate or oscillate.
    fed = make_federation()
    feed(fed, "initial", "c2", [rec(seq=1, value=1.0)])
    fed.set_online("c2", False)
    fed.set_online("c2", True)
    r = feed(fed, "self-conflict", "c2", [rec(seq=1, value=5.0)])
    assert r.rejected == 1
    q = fed.query_device("d1")
    log_query("self-conflict-verdict", q)
    assert q.stats.record_count == 1
    assert q.stats.value_sum == pytest.approx(1.0)
    assert q.conflicts == ()


def test_merge_is_order_independent():
    """Same data fed in different cluster orders yields identical verdicts."""
    def build(order):
        fed = make_federation()
        rounds = [
            ("c1", [rec(seq=1, value=1.0), rec(seq=2, value=2.0)]),
            ("c2", [rec(seq=1, value=1.0), rec(seq=2, value=2.0)]),
            ("c3", [rec(seq=2, value=9.0), rec(seq=3, value=3.0)]),
        ]
        for cid, records in sorted(rounds, key=lambda x: order.index(x[0])):
            feed(fed, f"order-{order}", cid, records)
        return fed.query_device("d1")

    a = build(["c1", "c2", "c3"])
    b = build(["c3", "c2", "c1"])
    log_query("order-a", a)
    log_query("order-b", b)
    key = lambda q: (
        q.stats.record_count, q.stats.value_sum,
        tuple(c.seq for c in q.conflicts),
        tuple(c.cluster_ids for c in q.conflicts),
    )
    assert key(a) == key(b)
    assert a.conflicts[0].seq == 2


# ---------------------------------------------------------------------------
# 3. Batch dashboard
# ---------------------------------------------------------------------------

def test_batch_dashboard_by_device_ids():
    fed = make_federation()
    feed(fed, "a", "c1", [rec("da", 1, value=1.0), rec("da", 2, value=2.0)])
    feed(fed, "a", "c2", [rec("da", 2, value=20.0)])       # conflict on da:2
    feed(fed, "b", "c1", [rec("db", 1, value=4.0)])

    dash = fed.query_devices(device_ids=["da", "db", "ghost"])
    log.info(
        "dashboard ids -> total=%s with_data=%s with_conflicts=%s "
        "records=%s conflicts=%s",
        dash.devices_total, dash.devices_with_data, dash.devices_with_conflicts,
        dash.total_record_count, dash.total_conflicts,
    )
    assert dash.devices_total == 3
    assert dash.devices_with_data == 2
    assert dash.devices_with_conflicts == 1
    assert dash.total_record_count == 2          # da:1, db:1 (da:2 quarantined)
    assert dash.total_conflicts == 1
    assert dash.results["ghost"].stats is None
    assert dash.results["ghost"].conflicts == ()
    assert dash.results["da"].conflicts[0].seq == 2
    assert dash.results["db"].stats.value_sum == pytest.approx(4.0)
    assert dash.complete is True


def test_batch_dashboard_by_cluster_scope_and_overall_incomplete():
    fed = make_federation()
    feed(fed, "a", "c1", [rec("da", 1, value=1.0)])
    feed(fed, "b", "c2", [rec("db", 1, value=2.0)])
    feed(fed, "b", "c3", [rec("dc", 1, value=3.0)])

    # c2 holds db; c3 holds dc. Cluster scope selects the device universe from
    # those stores, but reconciliation still runs over ALL online clusters.
    scoped = fed.query_devices(cluster_scope=["c2", "c3"])
    log.info("scope c2,c3 -> devices=%s", sorted(scoped.results))
    assert sorted(scoped.results) == ["db", "dc"]

    fed.set_online("c3", False)
    allv = fed.query_devices()  # universe over every cluster
    log.info(
        "universe (c3 offline) -> devices=%s complete=%s missing=%s",
        sorted(allv.results), allv.complete, allv.clusters_missing,
    )
    assert sorted(allv.results) == ["da", "db", "dc"]
    assert allv.complete is False
    assert allv.clusters_missing == ("c3",)
    # every per-device result carries the same completeness annotation
    for q in allv.results.values():
        assert q.complete is False and q.clusters_missing == ("c3",)


def test_batch_dashboard_scales_to_many_devices():
    fed = make_federation()
    n = 300
    for i in range(n):
        feed(fed, f"bulk-{i}", "c1", [rec(f"dev-{i}", 1, value=float(i))])
    dash = fed.query_devices()
    assert dash.devices_total == n
    assert dash.total_record_count == n
    assert dash.devices_with_conflicts == 0
    assert dash.total_conflicts == 0


# ---------------------------------------------------------------------------
# 4. Concurrent writes vs dashboard reads
# ---------------------------------------------------------------------------

def test_concurrent_writes_and_dashboard_reads_never_inconsistent():
    fed = make_federation(("c1", "c2", "c3"))
    n_devices, rounds_per_writer = 40, 25
    stop = threading.Event()

    def writer(tid):
        cid = f"c{tid % 3 + 1}"
        for k in range(rounds_per_writer):
            dev = f"dev-{k % n_devices}"
            feed(fed, f"writer{tid}-k{k}", cid,
                 [rec(dev, 1, value=float(k))])

    errors: list[Exception] = []

    def reader():
        while not stop.is_set():
            try:
                dash = fed.query_devices()
                # invariants that must hold for every coherent snapshot:
                # totals never negative/impossible and per-device counts never
                # exceed the number of distinct seqs actually reportable (1)
                assert dash.devices_total >= dash.devices_with_data
                assert dash.devices_with_conflicts <= dash.devices_with_data
                for q in dash.results.values():
                    # only seq 1 is ever reported in this workload
                    if q.stats is not None:
                        assert q.stats.record_count <= 1
                    # a conflicting seq is excluded from stats: counted seqs
                    # and conflict seqs must be disjoint
                    counted = {
                        s2 for s2 in range(q.stats.seq_min, q.stats.seq_max + 1)
                    } if q.stats is not None else set()
                    assert counted.isdisjoint(c.seq for c in q.conflicts)
            except Exception as exc:  # pragma: no cover - diagnostic capture
                errors.append(exc)

    readers = [threading.Thread(target=reader) for _ in range(3)]
    writers = [threading.Thread(target=writer, args=(t,)) for t in range(6)]
    for t in readers + writers:
        t.start()
    for t in writers:
        t.join()
    stop.set()
    for t in readers:
        t.join()
    assert not errors

    final = fed.query_devices()
    log_query("final-dev-0", final.results["dev-0"])
    # final state is itself query-order independent: read twice, compare
    again = fed.query_devices()
    assert final.total_record_count == again.total_record_count
    assert final.total_conflicts == again.total_conflicts
