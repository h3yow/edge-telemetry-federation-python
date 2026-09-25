"""HTTP 端到端测试：上报、重复、拒绝原因、联邦视图、集群切换。"""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from edge_telemetry.api import create_app
from edge_telemetry.clock import FixedClock
from edge_telemetry.models import SeriesPolicy, StorePolicy
from edge_telemetry.service import TelemetryService

logger = logging.getLogger("telemetry.tests")

NOW = 5_000_000


def build_client() -> tuple[TestClient, TelemetryService]:
    svc = TelemetryService(clock=FixedClock(NOW))
    svc.register_cluster("c1", StorePolicy(default=SeriesPolicy(
        reorder_delay_ms=1000, future_skew_ms=500)))
    svc.register_cluster("c2")
    return TestClient(create_app(svc)), svc


def rec(no, ts, value=1.0, device="d1", seq="temp"):
    return {"device_id": device, "seq_id": seq, "seq_no": no,
            "timestamp_ms": ts, "value": value}


def log_resp(title: str, body) -> None:
    logger.info("API %s -> %s", title, body)


def test_ingest_duplicate_and_reject_reasons():
    client, _ = build_client()
    body = {"records": [rec(1, NOW - 100, 3.0), rec(2, NOW - 90, 5.0)]}
    r1 = client.post("/api/clusters/c1/ingest", json=body)
    log_resp("first batch", r1.json())
    assert r1.json()["accepted"] == 2

    # 重复补报（含乱序）
    r2 = client.post("/api/clusters/c1/ingest",
                     json={"records": [rec(2, NOW - 90, 5.0), rec(1, NOW - 100, 3.0)]})
    log_resp("duplicate batch", r2.json())
    assert r2.json()["duplicate"] == 2 and r2.json()["accepted"] == 0

    # 自相矛盾：同序号不同值
    r3 = client.post("/api/clusters/c1/ingest",
                     json={"records": [rec(1, NOW - 100, 9.9)]})
    assert r3.json()["outcomes"][0]["reason"] == "seq_conflict"

    # 超迟到窗口
    r4 = client.post("/api/clusters/c1/ingest",
                     json={"records": [rec(9, NOW - 5000, 1.0)]})
    assert r4.json()["outcomes"][0]["reason"] == "outside_reorder_window"

    # 未来偏移
    r5 = client.post("/api/clusters/c1/ingest",
                     json={"records": [rec(10, NOW + 501, 1.0)]})
    assert r5.json()["outcomes"][0]["reason"] == "future_skew"

    # 非法时间戳（Pydantic 层 422）
    r6 = client.post("/api/clusters/c1/ingest",
                     json={"records": [rec(11, -1, 1.0)]})
    assert r6.status_code == 422


def test_unknown_and_offline_cluster():
    client, _ = build_client()
    r = client.post("/api/clusters/nope/ingest", json={"records": [rec(1, NOW)]})
    log_resp("unknown cluster", r.json())
    assert r.status_code == 200
    assert r.json()["outcomes"][0]["reason"] == "cluster_unknown"

    assert client.post("/api/clusters/c1/status", json={"online": False}).status_code == 200
    r2 = client.post("/api/clusters/c1/ingest", json={"records": [rec(1, NOW)]})
    assert r2.json()["outcomes"][0]["reason"] == "cluster_offline"
    assert client.post("/api/clusters/nope/status", json={"online": True}).status_code == 404


def test_federated_view_and_failover():
    client, _ = build_client()
    client.post("/api/clusters/c1/ingest",
                json={"records": [rec(1, NOW - 100, 1.0), rec(2, NOW, 2.0)]})
    client.post("/api/clusters/c2/ingest",
                json={"records": [rec(10, NOW, 8.0, device="d2")]})

    view = client.get("/api/federation/view").json()
    log_resp("complete view", view)
    assert view["completeness"] == "complete"
    assert view["devices"]["d1"]["series"]["temp"]["count"] == 2
    assert view["devices"]["d2"]["series"]["temp"]["count"] == 1

    # c1 离线 => 降级，仅返回 c2
    client.post("/api/clusters/c1/status", json={"online": False})
    degraded = client.get("/api/federation/view").json()
    log_resp("degraded view", degraded)
    assert degraded["completeness"] == "degraded"
    assert degraded["offline_clusters"] == ["c1"]
    assert "d1" not in degraded["devices"]
    assert "d2" in degraded["devices"]

    # 设备查询
    assert client.get("/api/federation/devices/d2").status_code == 200
    assert client.get("/api/federation/devices/missing").status_code == 404
