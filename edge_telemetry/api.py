"""FastAPI 路由：遥测批量上报、联邦查询与集群上下线切换。"""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .models import to_jsonable
from .service import TelemetryService


class RecordIn(BaseModel):
    device_id: str = Field(min_length=1)
    seq_id: str = Field(min_length=1)
    seq_no: int = Field(ge=0)
    timestamp_ms: int = Field(ge=0)
    value: float


class IngestIn(BaseModel):
    records: list[RecordIn]


class ClusterStatusIn(BaseModel):
    online: bool


def create_app(service: TelemetryService | None = None) -> FastAPI:
    service = service or TelemetryService()
    app = FastAPI(
        title="Edge Telemetry Federation",
        version="0.1.0",
        description="边缘设备遥测汇聚与多集群联邦查询（本地内存模拟）",
    )

    @app.get("/")
    def read_root() -> dict:
        return {"service": "edge-telemetry-federation", "status": "ok"}

    @app.post("/api/clusters/{cluster_id}/ingest")
    def ingest(cluster_id: str, payload: IngestIn) -> dict:
        # 显式构造领域对象，cluster_id 以路径为准
        from .models import TelemetryRecord

        domain_records = [
            TelemetryRecord(
                cluster_id=cluster_id, device_id=r.device_id, seq_id=r.seq_id,
                seq_no=r.seq_no, timestamp_ms=r.timestamp_ms, value=r.value)
            for r in payload.records
        ]
        result = service.ingest(cluster_id, domain_records)
        return to_jsonable(result)

    @app.get("/api/federation/view")
    def federation_view() -> dict:
        return to_jsonable(service.query())

    @app.get("/api/federation/devices/{device_id}")
    def device_view(device_id: str) -> dict:
        view = service.query()
        dv = view.devices.get(device_id)
        if dv is None:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=404,
                content={"error": "device_not_found", "device_id": device_id,
                         "completeness": view.completeness.value})
        return to_jsonable(dv)

    @app.post("/api/clusters/{cluster_id}/status")
    def set_cluster_status(cluster_id: str, payload: ClusterStatusIn) -> dict:
        try:
            service.set_cluster_online(cluster_id, payload.online)
        except KeyError:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=404,
                content={"error": "cluster_unknown", "cluster_id": cluster_id})
        return {"cluster_id": cluster_id, "online": payload.online}

    return app
