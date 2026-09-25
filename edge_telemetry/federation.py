"""多集群联邦：跨集群去重归并视图与集群上下线切换。

语义（详见 docs）：
- 查询只读取“在线”集群；任意单集群异常/离线不影响其余集群结果，
  并在视图中以 completeness 与 offline_clusters/errors 标注完整性。
- 跨集群幂等键 = (device_id, seq_id, seq_no)：集群切换后旧集群对同一序号的
  补报与新集群重复时只计一次，计数不翻倍。
- 归并采用单趟分桶：同序号按 (timestamp_ms, value) 分桶。恰好一种载荷 =>
  接受并把持有该载荷的集群全部计入 sources；存在多种载荷 => 自相矛盾，
  该序号不纳入统计并在 conflicts 中标注。结论只取决于载荷集合，与集群遍历、
  上报到达顺序无关，因此集群恢复后的补报不会改变历史结论。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .models import (
    Completeness,
    DeviceView,
    FederationView,
    SeriesStats,
    TelemetryRecord,
)
from .store import ClusterStore


@dataclass
class _SeqBucket:
    """同一 (device, seq, seq_no) 在多个集群中的载荷收集桶。"""

    # payload (timestamp_ms, value) -> 持有该载荷的集群（有序去重）
    payloads: dict[tuple[int, float], list[str]] = field(default_factory=dict)

    def add(self, rec: TelemetryRecord, cluster_id: str) -> None:
        srcs = self.payloads.setdefault((rec.timestamp_ms, rec.value), [])
        if cluster_id not in srcs:
            srcs.append(cluster_id)

    @property
    def is_conflict(self) -> bool:
        return len(self.payloads) > 1

    def sources(self) -> list[str]:
        out: list[str] = []
        for srcs in self.payloads.values():
            for cid in srcs:
                if cid not in out:
                    out.append(cid)
        return sorted(out)

    def record(self) -> TelemetryRecord:
        ts, value = next(iter(self.payloads))
        return TelemetryRecord("*", "", "", 0, ts, value)  # 占位，统计只用载荷


class Federation:
    """管理多个集群并提供跨集群归并视图。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stores: dict[str, ClusterStore] = {}

    def register(self, store: ClusterStore) -> None:
        with self._lock:
            if store.cluster_id in self._stores:
                raise ValueError(f"cluster {store.cluster_id} already registered")
            self._stores[store.cluster_id] = store

    def set_cluster_online(self, cluster_id: str, online: bool) -> None:
        with self._lock:
            if cluster_id not in self._stores:
                raise KeyError(f"unknown cluster {cluster_id}")
            self._stores[cluster_id].set_online(online)

    def cluster(self, cluster_id: str) -> ClusterStore:
        with self._lock:
            return self._stores[cluster_id]

    def query(self) -> FederationView:
        with self._lock:
            ids = sorted(self._stores)
            if not ids:
                return FederationView(completeness=Completeness.NO_CLUSTER)

            online = [cid for cid in ids if self._stores[cid].online]
            offline = [cid for cid in ids if not self._stores[cid].online]
            errors: list[str] = []
            readable: list[str] = []

            # (device_id, seq_id, seq_no) -> bucket
            buckets: dict[tuple[str, str, int], _SeqBucket] = {}

            for cid in online:
                try:
                    records = self._stores[cid].all_records()
                except Exception as exc:  # 单集群故障隔离，不拖垮整体查询
                    errors.append(f"{cid}: {type(exc).__name__}: {exc}")
                    offline.append(cid)
                    continue
                readable.append(cid)
                for rec in records:
                    bkey = (rec.device_id, rec.seq_id, rec.seq_no)
                    buckets.setdefault(bkey, _SeqBucket()).add(rec, cid)

            view = FederationView(
                online_clusters=readable,
                offline_clusters=offline,
                errors=sorted(errors),
            )

            # 收集每个设备下“已接受序号”的载荷，用于一次性重建统计
            accepted: dict[tuple[str, str], list[tuple[int, int, float, list[str]]]] = {}
            conflicts: dict[str, list[str]] = {}
            for (device_id, seq_id, seq_no) in sorted(buckets):
                bucket = buckets[(device_id, seq_id, seq_no)]
                if bucket.is_conflict:
                    conflicts.setdefault(device_id, []).append(
                        f"{seq_id}#{seq_no}: {len(bucket.payloads)} distinct payloads "
                        f"across {bucket.sources()}")
                    continue
                (ts, value), srcs = next(iter(bucket.payloads.items()))
                accepted.setdefault((device_id, seq_id), []).append(
                    (seq_no, ts, value, bucket.sources()))

            for (device_id, seq_id), items in sorted(accepted.items()):
                records = [
                    TelemetryRecord("*", device_id, seq_id, no, ts, value)
                    for no, ts, value, _ in items]
                dv = view.devices.setdefault(device_id, DeviceView(
                    device_id=device_id, series_count=0, record_count=0))
                dv.series[seq_id] = self._build_stats(device_id, seq_id, records)

            # 仅有冲突、没有任何有效记录的设备也要出现在视图中以承载冲突标注
            for device_id in sorted(conflicts):
                view.devices.setdefault(device_id, DeviceView(
                    device_id=device_id, series_count=0, record_count=0))

            for device_id, dv in view.devices.items():
                dv.series_count = len(dv.series)
                dv.record_count = sum(s.count for s in dv.series.values())
                src_set = {
                    cid
                    for (d, _s), items in accepted.items()
                    if d == device_id
                    for _, _, _, srcs in items
                    for cid in srcs
                }
                dv.sources = sorted(src_set)
                dv.conflicts = sorted(conflicts.get(device_id, []))

            if not readable:
                view.completeness = Completeness.NO_CLUSTER
            elif offline or errors:
                view.completeness = Completeness.DEGRADED
            else:
                view.completeness = Completeness.COMPLETE
            return view

    @staticmethod
    def _build_stats(device_id: str, seq_id: str,
                     records: list[TelemetryRecord]) -> SeriesStats:
        ts = [r.timestamp_ms for r in records]
        vals = [r.value for r in records]
        nos = [r.seq_no for r in records]
        return SeriesStats(
            cluster_id="*", device_id=device_id, seq_id=seq_id,
            count=len(records),
            min_value=min(vals), max_value=max(vals), sum_value=sum(float(v) for v in vals),
            first_seq_no=min(nos), last_seq_no=max(nos),
            min_timestamp_ms=min(ts), max_timestamp_ms=max(ts))
