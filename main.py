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
    result = federation.query_device(device_id)
    return {
        "stats": asdict(result.stats) if result.stats else None,
        "clusters_total": result.clusters_total,
        "clusters_online": result.clusters_online,
        "clusters_missing": list(result.clusters_missing),
        "complete": result.complete,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=0)
