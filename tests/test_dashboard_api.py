"""End-to-end API tests for cross-cluster conflicts and the batch dashboard."""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from main import app

log = logging.getLogger("test.dashboard_api")
NOW = 10_000.0
client = TestClient(app)


def _post_batch(cluster, records, now=NOW):
    body = {"now": now, "records": records}
    r = client.post(f"/clusters/{cluster}/batches", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def test_conflict_and_dashboard_end_to_end():
    # identical seq1 on both clusters; divergent seq2 across clusters
    _post_batch("cluster-a", [
        {"device_id": "dx", "seq": 1, "timestamp": NOW - 1, "metric": "cpu", "value": 1.0},
        {"device_id": "dx", "seq": 2, "timestamp": NOW - 1, "metric": "cpu", "value": 2.0},
    ])
    _post_batch("cluster-b", [
        {"device_id": "dx", "seq": 1, "timestamp": NOW - 1, "metric": "cpu", "value": 1.0},
        {"device_id": "dx", "seq": 2, "timestamp": NOW - 1, "metric": "cpu", "value": 22.0},
        {"device_id": "dy", "seq": 1, "timestamp": NOW - 1, "metric": "cpu", "value": 5.0},
    ])

    # single-device query stays backward compatible and gains conflict fields
    q = client.get("/devices/dx/stats").json()
    log.info("single device query: %s", q)
    assert q["stats"]["record_count"] == 1          # only seq1 counted
    assert q["stats"]["value_sum"] == 1.0
    assert q["has_conflicts"] is True
    assert len(q["conflicts"]) == 1
    c = q["conflicts"][0]
    assert c["seq"] == 2 and c["version_count"] == 2
    assert sorted(c["cluster_ids"]) == ["cluster-a", "cluster-b"]

    # batch dashboard by explicit device ids
    r = client.post("/dashboard/query", json={"device_ids": ["dx", "dy", "ghost"]})
    assert r.status_code == 200, r.text
    dash = r.json()
    log.info("dashboard by ids: %s", dash)
    assert dash["devices_total"] == 3
    assert dash["devices_with_data"] == 2
    assert dash["devices_with_conflicts"] == 1
    assert dash["total_record_count"] == 2
    assert dash["total_conflicts"] == 1
    assert dash["devices"]["ghost"]["stats"] is None
    assert dash["devices"]["dy"]["stats"]["value_sum"] == 5.0

    # dashboard over a cluster scope
    r = client.post("/dashboard/query", json={"cluster_scope": ["cluster-b"]})
    scope = r.json()
    log.info("dashboard by scope: devices=%s", sorted(scope["devices"]))
    assert sorted(scope["devices"]) == ["dx", "dy"]

    # unknown cluster scope -> 404
    r = client.post("/dashboard/query", json={"cluster_scope": ["nope"]})
    assert r.status_code == 404


def test_dashboard_marks_incomplete_when_cluster_offline():
    client.post("/clusters/cluster-b/status", json={"online": False})
    dash = client.post("/dashboard/query", json={"device_ids": ["dx"]}).json()
    log.info("dashboard incomplete: %s", dash)
    assert dash["complete"] is False
    assert dash["clusters_missing"] == ["cluster-b"]
    assert dash["devices"]["dx"]["complete"] is False
    # with cluster-b gone, seq2's second version is hidden -> no conflict now,
    # but that absence is reported as incompleteness, not silently trusted
    assert dash["devices"]["dx"]["conflicts"] == []
    client.post("/clusters/cluster-b/status", json={"online": True})
