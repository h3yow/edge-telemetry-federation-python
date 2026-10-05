"""End-to-end API test: ingest -> switchover -> query completeness."""

import logging

from fastapi.testclient import TestClient

from main import app

log = logging.getLogger("test.api")
NOW = 10_000.0


def test_api_flow():
    c = TestClient(app)
    body = {"now": NOW, "records": [
        {"device_id": "dev-9", "seq": 1, "timestamp": NOW - 5,
         "metric": "cpu", "value": 0.5},
        {"device_id": "dev-9", "seq": 1, "timestamp": NOW - 5,
         "metric": "cpu", "value": 0.5},  # duplicate within batch
        {"device_id": "dev-9", "seq": 2, "timestamp": NOW - 5,
         "metric": "cpu", "value": 0.7, "sample_rate": 0.5},
    ]}
    r = c.post("/clusters/cluster-a/batches", json=body)
    assert r.status_code == 200
    data = r.json()
    log.info("ingest response: %s", data)
    assert (data["accepted"], data["duplicates"], data["rejected"]) == (2, 1, 0)

    # take cluster-b offline; query must stay available but incomplete
    r = c.post("/clusters/cluster-b/status", json={"online": False})
    assert r.status_code == 200
    r = c.get("/devices/dev-9/stats")
    data = r.json()
    log.info("query response: %s", data)
    assert data["stats"]["record_count"] == 2
    assert data["stats"]["weighted_count"] == 1.0 + 2.0
    assert data["complete"] is False
    assert data["clusters_missing"] == ["cluster-b"]

    # offline cluster rejects ingest with 409
    r = c.post("/clusters/cluster-b/batches", json=body)
    assert r.status_code == 409

    # recover and replay: no double counting
    c.post("/clusters/cluster-b/status", json={"online": True})
    r = c.post("/clusters/cluster-a/batches", json=body)
    assert r.json()["duplicates"] == 3
    r = c.get("/devices/dev-9/stats")
    assert r.json()["stats"]["record_count"] == 2
    assert r.json()["complete"] is True


def test_dashboard_endpoint_and_conflict_payload():
    c = TestClient(app)
    records_a = [
        {"device_id": "dash-1", "seq": 1, "timestamp": NOW,
         "metric": "cpu", "value": 1.0},
        {"device_id": "dash-1", "seq": 2, "timestamp": NOW,
         "metric": "cpu", "value": 2.0},
    ]
    records_b = [
        {"device_id": "dash-1", "seq": 1, "timestamp": NOW,
         "metric": "cpu", "value": 9.9},  # conflicts with cluster-a
        {"device_id": "dash-2", "seq": 1, "timestamp": NOW,
         "metric": "cpu", "value": 4.0},
    ]
    c.post("/clusters/cluster-a/batches", json={"now": NOW, "records": records_a})
    c.post("/clusters/cluster-b/batches", json={"now": NOW, "records": records_b})

    # single-device response now carries conflicts, old fields unchanged
    single = c.get("/devices/dash-1/stats").json()
    log.info("single query: %s", single)
    assert single["stats"]["record_count"] == 1     # seq2 only
    assert single["has_conflicts"] is True
    cf = single["conflicts"][0]
    assert cf["seq"] == 1 and cf["version_count"] == 2
    assert cf["clusters"] == ["cluster-a", "cluster-b"]

    # batch dashboard by explicit ids
    r = c.post("/devices/query", json={"device_ids": ["dash-2", "dash-1"]})
    assert r.status_code == 200
    data = r.json()
    log.info("dashboard response: %s", data)
    assert [d["device_id"] for d in data["devices"]] == ["dash-1", "dash-2"]
    assert data["summary"]["devices_total"] == 2
    assert data["summary"]["devices_with_conflicts"] == 1
    assert data["summary"]["conflict_seqs"] == 1

    # fleet-wide query (no scope)
    r = c.post("/devices/query", json={})
    assert r.status_code == 200
    assert r.json()["summary"]["devices_total"] >= 2

    # both scopes at once -> 400; unknown cluster -> 404
    r = c.post("/devices/query",
               json={"device_ids": ["x"], "cluster_ids": ["cluster-a"]})
    assert r.status_code == 400
    r = c.post("/devices/query", json={"cluster_ids": ["nope"]})
    assert r.status_code == 404
