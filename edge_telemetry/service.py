"""应用服务：把 HTTP 层与领域层连接起来。

- 持有 Federation 与可注入时钟；
- 负责上报入口的集群存在性/在线状态校验（返回 CLUSTER_UNKNOWN 等可区分原因）；
- 不改变领域层归并规则，仅做编排。
"""

from __future__ import annotations

import logging

from .clock import Clock, SystemClock
from .federation import Federation
from .models import (
    FederationView,
    IngestResult,
    IngestStatus,
    RecordKey,
    RecordOutcome,
    RejectReason,
    StorePolicy,
    TelemetryRecord,
)
from .store import ClusterStore

logger = logging.getLogger("edge_telemetry.service")


class TelemetryService:
    """对外门面：注册集群、上报与查询。"""

    def __init__(self, clock: Clock | None = None) -> None:
        self.federation = Federation()
        self._clock = clock or SystemClock()

    def now_ms(self) -> int:
        return self._clock.now_ms()

    def register_cluster(self, cluster_id: str,
                         policy: StorePolicy | None = None) -> ClusterStore:
        store = ClusterStore(cluster_id, policy)
        self.federation.register(store)
        return store

    def set_cluster_online(self, cluster_id: str, online: bool) -> None:
        self.federation.set_cluster_online(cluster_id, online)

    def ingest(self, cluster_id: str,
               records: list[TelemetryRecord]) -> IngestResult:
        """向指定集群批量上报；未知集群给出 CLUSTER_UNKNOWN 整批拒绝。"""

        now_ms = self.now_ms()
        try:
            store = self.federation.cluster(cluster_id)
        except KeyError:
            result = IngestResult(cluster_id=cluster_id, rejected=len(records))
            for rec in records:
                result.outcomes.append(RecordOutcome(
                    status=IngestStatus.REJECTED,
                    key=RecordKey(
                        cluster_id,
                        getattr(rec, "device_id", ""),
                        getattr(rec, "seq_id", ""),
                        getattr(rec, "seq_no", 0)),
                    reason=RejectReason.CLUSTER_UNKNOWN,
                    detail=f"cluster {cluster_id} is not registered",
                    timestamp_ms=getattr(rec, "timestamp_ms", 0),
                    value=getattr(rec, "value", 0.0)))
            self._log_batch(result, records)
            return result

        result = store.ingest_batch(records, now_ms)
        self._log_batch(result, records)
        return result

    def query(self) -> FederationView:
        return self.federation.query()

    @staticmethod
    def _log_batch(result: IngestResult, records: list[TelemetryRecord]) -> None:
        for r in records:
            logger.info(
                "INGEST cluster=%s device=%s seq=%s no=%s ts=%s value=%s",
                r.cluster_id, r.device_id, r.seq_id, r.seq_no,
                r.timestamp_ms, r.value)
        for o in result.outcomes:
            logger.info(
                "OUTCOME no=%s status=%s reason=%s detail=%s evidence=%s",
                o.key.seq_no, o.status.value,
                o.reason.value if o.reason else "-", o.detail, o.evidence)
