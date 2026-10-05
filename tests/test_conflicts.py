"""Cross-cluster conflict detection: inconsistent (device, seq) versions
are quarantined out of the merged stats and reported explicitly, distinct
from cluster-offline incompleteness.
"""

import logging

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
        fed.add_cluster(ClusterStore(cid))
    return fed


def log_query(tag, q):
    s = q.stats
    log.info(
        "%s -> count=%s value_sum=%s missing=%s complete=%s "
        "missing_clusters=%s conflicts=%s",
        tag,
        s.record_count if s else None, s.value_sum if s else None,
        s.missing_seqs if s else None, q.complete, q.clusters_missing,
        [(c.seq, c.clusters, c.version_count) for c in q.conflicts],
    )


def test_conflicting_seq_is_quarantined_not_merged():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=1.0), rec(seq=2, value=2.0))), now=NOW)
    fed.ingest(Batch(
        "c2", (rec(seq=1, value=9.9), rec(seq=2, value=2.0), rec(seq=3, value=3.0))
    ), now=NOW)
    q = fed.query_device("d1")
    log_query("conflict-on-seq1", q)
    # seq1 is conflicted -> excluded; seq2 identical -> once; seq3 -> once
    assert q.stats.record_count == 2
    assert q.stats.value_sum == pytest.approx(5.0)
    assert q.stats.missing_seqs == ()
    assert len(q.conflicts) == 1
    cf = q.conflicts[0]
    assert cf.seq == 1
    assert cf.clusters == ("c1", "c2")
    assert cf.version_count == 2
    values = sorted(v.value for v in cf.variants)
    assert values == [1.0, 9.9]
    # which cluster carries which version is visible
    by_value = {v.value: v.clusters for v in cf.variants}
    assert by_value[1.0] == ("c1",)
    assert by_value[9.9] == ("c2",)


def test_conflict_distinct_from_offline_incompleteness():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=1.0),)), now=NOW)
    fed.ingest(Batch("c2", (rec(seq=1, value=9.9),)), now=NOW)
    # c3 never reported anything and is online: no conflict with it
    q = fed.query_device("d1")
    log_query("c3-silent-online", q)
    assert q.complete is True
    assert q.clusters_missing == ()
    assert len(q.conflicts) == 1

    # take a participant offline: its version drops out -> no longer a
    # conflict, but now the answer is incomplete
    fed.set_online("c2", False)
    q = fed.query_device("d1")
    log_query("c2-offline", q)
    assert q.conflicts == ()
    assert q.complete is False
    assert q.clusters_missing == ("c2",)
    assert q.stats.record_count == 1
    assert q.stats.value_sum == pytest.approx(1.0)


def test_three_versions_three_clusters_reports_all():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=1.0),)), now=NOW)
    fed.ingest(Batch("c2", (rec(seq=1, value=2.0),)), now=NOW)
    fed.ingest(Batch("c3", (rec(seq=1, value=3.0),)), now=NOW)
    q = fed.query_device("d1")
    log_query("three-versions", q)
    assert q.stats is None                    # nothing safe to aggregate
    assert q.conflicts[0].version_count == 3
    assert q.conflicts[0].clusters == ("c1", "c2", "c3")


def test_majority_agreement_does_not_pick_a_winner():
    # even 2-vs-1 agreement must not promote the majority version
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=5.0),)), now=NOW)
    fed.ingest(Batch("c2", (rec(seq=1, value=5.0),)), now=NOW)
    fed.ingest(Batch("c3", (rec(seq=1, value=8.0),)), now=NOW)
    q = fed.query_device("d1")
    log_query("majority-still-conflict", q)
    assert q.stats is None
    assert q.conflicts[0].version_count == 2
    assert {tuple(v.clusters) for v in q.conflicts[0].variants} == {
        ("c1", "c2"), ("c3",),
    }


def test_conflict_does_not_pollute_missing_seq_range():
    fed = make_federation()
    fed.ingest(Batch("c1", (rec(seq=1, value=1.0), rec(seq=3, value=3.0))), now=NOW)
    fed.ingest(Batch("c2", (rec(seq=1, value=9.9),)), now=NOW)
    q = fed.query_device("d1")
    log_query("conflict-at-edge", q)
    # seq1 quarantined, seq3 clean; the conflicted seq1 itself must not be
    # listed as missing, while seq2 (reported by nobody) stays a real gap
    assert q.stats.record_count == 1
    assert q.stats.missing_seqs == (2,)
