"""HTTP API for the edge telemetry federation service."""

from __future__ import annotations

from dataclasses import asdict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from telemetry.federation import Federation
from telemetry.models import Batch, TelemetryRecord
from telemetry.store import ClusterStore

app = FastAPI(title="edge-telemetry-federation")

federation = Federation()
for cid in ("cluster-a", "cluster-b"):
    federation.add_cluster(ClusterStore(cid))


class RecordIn(BaseModel):
    device_id: str
    seq: int = Field(ge=0)
    timestamp: float
    metric: str
    value: float
    sample_rate: float = 1.0


class BatchIn(BaseModel):
    records: list[RecordIn]
    now: float | None = None  # optional logical clock for deterministic tests


class ClusterStatusIn(BaseModel):
    online: bool


class BatchDashboardIn(BaseModel):
    device_ids: list[str] | None = None
    cluster_scope: list[str] | None = None


def _conflict_dict(c) -> dict:
    return {
        "device_id": c.device_id,
        "seq": c.seq,
        "version_count": c.version_count,
        "cluster_ids": list(c.cluster_ids),
        "versions": [
            {
                "cluster_id": v.cluster_id,
                "timestamp": v.timestamp,
                "metric": v.metric,
                "value": v.value,
                "sample_rate": v.sample_rate,
            }
            for v in c.versions
        ],
    }


def _query_result_dict(result) -> dict:
    return {
        "stats": asdict(result.stats) if result.stats else None,
        "clusters_total": result.clusters_total,
        "clusters_online": result.clusters_online,
        "clusters_missing": list(result.clusters_missing),
        "complete": result.complete,
        "has_conflicts": result.has_conflicts,
        "conflicts": [_conflict_dict(c) for c in result.conflicts],
    }


@app.get("/")
def read_root():
    return {"service": "edge-telemetry-federation"}


@app.post("/clusters/{cluster_id}/batches")
def ingest_batch(cluster_id: str, body: BatchIn):
    batch = Batch(
        cluster_id=cluster_id,
        records=tuple(TelemetryRecord(**r.model_dump()) for r in body.records),
    )
    try:
        result = federation.ingest(batch, now=body.now)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "cluster_id": result.cluster_id,
        "accepted": result.accepted,
        "duplicates": result.duplicates,
        "rejected": result.rejected,
        "decisions": [
            {
                "device_id": d.record.device_id,
                "seq": d.record.seq,
                "decision": d.decision.value,
                "reason": d.reason.value if d.reason else None,
                "detail": d.detail,
            }
            for d in result.decisions
        ],
    }


@app.post("/clusters/{cluster_id}/status")
def set_cluster_status(cluster_id: str, body: ClusterStatusIn):
    try:
        federation.set_online(cluster_id, body.online)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"cluster_id": cluster_id, "online": body.online}


@app.get("/devices/{device_id}/stats")
def device_stats(device_id: str):
    return _query_result_dict(federation.query_device(device_id))


@app.post("/dashboard/query")
def dashboard_query(body: BatchDashboardIn):
    try:
        dash = federation.query_devices(
            device_ids=body.device_ids,
            cluster_scope=body.cluster_scope,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "devices_total": dash.devices_total,
        "devices_with_data": dash.devices_with_data,
        "devices_with_conflicts": dash.devices_with_conflicts,
        "complete": dash.complete,
        "clusters_missing": list(dash.clusters_missing),
        "total_record_count": dash.total_record_count,
        "total_conflicts": dash.total_conflicts,
        "devices": {
            device_id: _query_result_dict(q)
            for device_id, q in sorted(dash.results.items())
        },
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=0)
