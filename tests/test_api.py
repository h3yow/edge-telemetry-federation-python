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
