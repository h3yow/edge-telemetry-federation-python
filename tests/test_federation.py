"""Federation tests: cluster switchover, recovery re-report, concurrent writes."""

import logging
import threading

import pytest

from telemetry.federation import Federation
from telemetry.models import Batch, TelemetryRecord
from telemetry.store import ClusterStore

log = logging.getLogger("test.federation")
NOW = 10_000.0


def rec(device="d1", seq=1, ts=NOW - 10, metric="cpu", value=1.0, rate=1.0):
    return TelemetryRecord(device, seq, ts, metric, value, rate)


def make_federation(cluster_ids=("c1", "c2")):
    fed = Federation()
    for cid in cluster_ids:
        fed.add_cluster(ClusterStore(cid, max_seq_gap=1000))
    return fed


def log_query(tag, q):
    s = q.stats
    log.info(
        "%s -> count=%s value_sum=%s seq=[%s..%s] missing_seqs=%s "
        "online=%s/%s missing_clusters=%s complete=%s",
        tag, s.record_count if s else None, s.value_sum if s else None,
        s.seq_min if s else None, s.seq_max if s else None,
        s.missing_seqs if s else None,
        q.clusters_online, q.clusters_total, q.clusters_missing, q.complete,
    )


def test_cluster_offline_query_still_returns_partial_results():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=1.0), rec(seq=2, value=2.0))), now=NOW)
    fed.ingest(Batch("c2", (rec(seq=3, value=3.0),)), now=NOW)

    fed.set_online("c2", False)
    log.info("switch: c2 -> offline")
    q = fed.query_device("d1")
    log_query("query-with-c2-offline", q)
    assert q.stats.record_count == 2            # c1's valid data still returned
    assert q.stats.value_sum == pytest.approx(3.0)
    assert q.complete is False
    assert q.clusters_missing == ("c2",)
    assert (q.clusters_online, q.clusters_total) == (1, 2)


def test_recovery_replay_does_not_double_count_or_drift():
    fed = make_federation()
    batch = Batch("c2", (rec(seq=3, value=3.0), rec(seq=4, value=4.0)))
    r1 = fed.ingest(batch, now=NOW)
    log.info("initial ingest c2: accepted=%s dup=%s rej=%s",
             r1.accepted, r1.duplicates, r1.rejected)
    before = fed.query_device("d1")
    log_query("before-outage", before)

    fed.set_online("c2", False)
    fed.set_online("c2", True)
    log.info("switch: c2 offline -> online (recovered)")

    r2 = fed.ingest(batch, now=NOW)  # re-report after recovery
    log.info("recovery replay: accepted=%s dup=%s rej=%s",
             r2.accepted, r2.duplicates, r2.rejected)
    assert r2.duplicates == 2 and r2.accepted == 0

    after = fed.query_device("d1")
    log_query("after-recovery-replay", after)
    assert after.stats.record_count == before.stats.record_count == 2
    assert after.stats.value_sum == pytest.approx(before.stats.value_sum)
    assert after.complete is True


def test_same_record_via_two_clusters_counts_once():
    fed = make_federation()
    shared = rec(seq=7, value=5.0)
    fed.ingest(Batch("c1", (shared,)), now=NOW)
    fed.ingest(Batch("c2", (shared,)), now=NOW)  # re-routed duplicate
    q = fed.query_device("d1")
    log_query("cross-cluster-dedup", q)
    assert q.stats.record_count == 1
    assert q.stats.value_sum == pytest.approx(5.0)


def test_concurrent_interleaved_ingest_converges():
    fed = make_federation(("c1", "c2", "c3"))
    n_threads, n_seq = 6, 50
    # Each thread reports an interleaved subset; every seq in [0, n_seq)
    # is reported exactly twice (once each to two different clusters).
    def worker(tid):
        cid = f"c{tid % 3 + 1}"
        seqs = range(tid, n_seq, n_threads)
        for s in seqs:
            r = fed.ingest(Batch(cid, (rec(seq=s, value=float(s)),)), now=NOW)
            assert r.rejected == 0

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    # duplicate coverage: seqs 0..n_seq-1 also replayed via another cluster
    def replay_worker():
        for s in range(n_seq):
            fed.ingest(Batch("c2", (rec(seq=s, value=float(s)),)), now=NOW)

    threads.append(threading.Thread(target=replay_worker))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    q = fed.query_device("d1")
    log_query("after-concurrent-ingest", q)
    expected = sum(float(s) for s in range(n_seq))
    assert q.stats.record_count == n_seq
    assert q.stats.value_sum == pytest.approx(expected)
    assert q.stats.missing_seqs == ()
    assert q.complete is True


def test_recovery_backfill_of_missing_data_counts_once():
    """c1 holds seq1; c2 is offline while seq2 is first seen, then recovers
    and backfills its copy: totals must not double and stay stable."""
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=1.0), rec(seq=2, value=2.0))), now=NOW)

    fed.set_online("c2", False)
    log.info("switch: c2 -> offline")
    partial = fed.query_device("d1")
    log_query("during-c2-outage", partial)
    assert partial.complete is False

    fed.set_online("c2", True)
    log.info("switch: c2 -> online (recovered)")
    backfill = Batch("c2", (rec(seq=1, value=1.0), rec(seq=2, value=2.0)))
    r = fed.ingest(backfill, now=NOW)
    log.info("backfill: accepted=%s dup=%s rej=%s",
             r.accepted, r.duplicates, r.rejected)
    # c2 reported nothing before the outage, so after recovery both records
    # are newly accepted *by c2*; the federated total still counts each once
    assert (r.accepted, r.duplicates, r.rejected) == (2, 0, 0)

    after = fed.query_device("d1")
    log_query("after-backfill", after)
    assert after.stats.record_count == 2          # no doubling
    assert after.stats.value_sum == pytest.approx(3.0)
    assert after.complete is True

    # replay yet again, from either cluster order: conclusion must not drift
    for cid in ("c1", "c2", "c2", "c1"):
        fed.ingest(backfill if cid == "c2"
                   else Batch("c1", (rec(seq=1, value=1.0), rec(seq=2, value=2.0))),
                   now=NOW)
    again = fed.query_device("d1")
    log_query("after-repeat-replays", again)
    assert again.stats.record_count == 2
    assert again.stats.value_sum == pytest.approx(3.0)
    assert again.conflicts == ()


def test_backfill_with_different_payload_is_stable_conflict():
    """Recovered cluster backfills a seq with different content: the seq is
    quarantined, and the conclusion stays identical regardless of query order
    or repeated queries (no historical drift)."""
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=1.0),)), now=NOW)

    fed.set_online("c2", False)
    fed.set_online("c2", True)
    fed.ingest(Batch("c2", (rec(seq=1, value=7.7),)), now=NOW)

    first = fed.query_device("d1")
    log_query("conflict-backfill-query-1", first)
    assert first.stats is None
    assert first.conflicts[0].seq == 1
    assert first.conflicts[0].version_count == 2

    # repeated queries, plus extra replays, must yield the same answer
    for _ in range(3):
        q = fed.query_device("d1")
        assert q.stats is None
        assert q.conflicts[0].version_count == 2
        assert q.conflicts[0].clusters == ("c1", "c2")
        assert sorted(v.value for v in q.conflicts[0].variants) == [1.0, 7.7]
    log_query("conflict-stable-after-repeats", fed.query_device("d1"))


def test_merge_independent_of_cluster_ingest_order():
    # same set of reports, two opposite ingest orders -> identical conclusion
    reports = {
        "c1": (rec(seq=1, value=1.0), rec(seq=2, value=2.0)),
        "c2": (rec(seq=1, value=9.9), rec(seq=2, value=2.0)),
        "c3": (rec(seq=3, value=3.0),),
    }
    fed_a = make_federation(("c1", "c2", "c3"))
    for cid in ("c1", "c2", "c3"):
        fed_a.ingest(Batch(cid, reports[cid]), now=NOW)
    fed_b = make_federation(("c1", "c2", "c3"))
    for cid in ("c3", "c2", "c1"):
        fed_b.ingest(Batch(cid, reports[cid]), now=NOW)

    qa, qb = fed_a.query_device("d1"), fed_b.query_device("d1")
    log_query("order-a", qa)
    log_query("order-b", qb)
    assert qa.stats.record_count == qb.stats.record_count == 2
    assert qa.stats.value_sum == qb.stats.value_sum == pytest.approx(5.0)
    assert qa.conflicts[0].seq == qb.conflicts[0].seq == 1
    assert qa.conflicts[0].version_count == qb.conflicts[0].version_count == 2
