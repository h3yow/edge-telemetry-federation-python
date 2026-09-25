"""单集群本地存储：批量上报、幂等归并与状态查询。

归并规则（确定性，详见 docs）：
- 幂等键 = (cluster_id, device_id, seq_id, seq_no)；同键重复上报记为 duplicate，
  不重复计数。
- 同键但 timestamp/value 不一致 => SEQ_CONFLICT（自相矛盾，拒绝）。
- 校验顺序固定：集群在线 -> 字段 -> 时间戳 -> 值域 -> 未来偏移 -> 幂等/冲突
  -> 迟到窗口 -> 采样率。
- 水线 = 该序列已接受记录的最大采集时间；timestamp < watermark - reorder_delay
  的记录视为超出迟到接受范围。
- 整批在同一把锁内按到达顺序原子应用，保证并发批量写入的最终状态一致。
"""

from __future__ import annotations

import threading
from dataclasses import replace

from .models import (
    ClusterSnapshot,
    IngestResult,
    IngestStatus,
    RecordKey,
    RecordOutcome,
    RejectReason,
    SeriesPolicy,
    SeriesStats,
    StorePolicy,
    TelemetryRecord,
)


def _is_real_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


class ClusterStore:
    """模拟一个集群的本地遥测存储。"""

    def __init__(self, cluster_id: str, policy: StorePolicy | None = None) -> None:
        if not cluster_id:
            raise ValueError("cluster_id must not be empty")
        self.cluster_id = cluster_id
        self.policy = policy or StorePolicy()
        self.online = True
        self._lock = threading.RLock()
        # (device_id, seq_id) -> {seq_no: TelemetryRecord}
        self._series: dict[tuple[str, str], dict[int, TelemetryRecord]] = {}
        self._stats: dict[str, dict[str, SeriesStats]] = {}
        self._watermarks: dict[tuple[str, str], int] = {}

    # ---- 集群状态 -----------------------------------------------------------

    def set_online(self, online: bool) -> None:
        with self._lock:
            self.online = bool(online)

    def _policy_for(self, device_id: str, seq_id: str) -> SeriesPolicy:
        return self.policy.overrides.get((device_id, seq_id), self.policy.default)

    # ---- 上报主流程 ---------------------------------------------------------

    def ingest_batch(self, records: list[TelemetryRecord], now_ms: int) -> IngestResult:
        """原子地应用一整批记录，返回每条记录的判定。"""

        result = IngestResult(cluster_id=self.cluster_id)
        with self._lock:
            for rec in records:
                outcome = self._ingest_one(rec, now_ms)
                result.outcomes.append(outcome)
                if outcome.status is IngestStatus.ACCEPTED:
                    result.accepted += 1
                elif outcome.status is IngestStatus.DUPLICATE:
                    result.duplicate += 1
                else:
                    result.rejected += 1
        return result

    def _reject(self, rec: TelemetryRecord, reason: RejectReason,
                detail: str, evidence: dict | None = None) -> RecordOutcome:
        return RecordOutcome(
            status=IngestStatus.REJECTED,
            key=RecordKey(self.cluster_id, rec.device_id, rec.seq_id, rec.seq_no),
            reason=reason,
            detail=detail,
            timestamp_ms=getattr(rec, "timestamp_ms", 0),
            value=getattr(rec, "value", 0.0),
            evidence=evidence or {},
        )

    def _ingest_one(self, rec: TelemetryRecord, now_ms: int) -> RecordOutcome:
        key = RecordKey(self.cluster_id, rec.device_id, rec.seq_id, rec.seq_no)

        # 0) 集群在线状态
        if not self.online:
            return self._reject(rec, RejectReason.CLUSTER_OFFLINE,
                                f"cluster {self.cluster_id} is offline")

        # 1) 字段合法性（自相矛盾/不可解析在入口处拦截）
        if (not isinstance(rec, TelemetryRecord)
                or not _is_real_number(rec.seq_no)
                or isinstance(rec.seq_no, bool)
                or int(rec.seq_no) < 0
                or not isinstance(rec.device_id, str) or not rec.device_id
                or not isinstance(rec.seq_id, str) or not rec.seq_id):
            return self._reject(rec, RejectReason.MALFORMED,
                                "device_id/seq_id must be non-empty strings and "
                                "seq_no must be a non-negative int")
        if rec.cluster_id != self.cluster_id:
            return self._reject(rec, RejectReason.MALFORMED,
                                f"record cluster {rec.cluster_id!r} != store {self.cluster_id!r}")

        # 2) 时间戳合法性
        if not isinstance(rec.timestamp_ms, int) or isinstance(rec.timestamp_ms, bool) \
                or rec.timestamp_ms < 0:
            return self._reject(rec, RejectReason.INVALID_TIMESTAMP,
                                "timestamp_ms must be a non-negative int")

        # 3) 数值合法性与值域
        if not _is_real_number(rec.value):
            return self._reject(rec, RejectReason.MALFORMED,
                                "value must be a finite number")
        policy = self._policy_for(rec.device_id, rec.seq_id)
        if rec.value < policy.value_min or rec.value > policy.value_max:
            return self._reject(rec, RejectReason.VALUE_OUT_OF_RANGE,
                                f"value {rec.value} outside "
                                f"[{policy.value_min}, {policy.value_max}]",
                                {"value_min": policy.value_min,
                                 "value_max": policy.value_max})

        # 4) 未来时钟偏移
        future_limit = now_ms + policy.future_skew_ms
        if rec.timestamp_ms > future_limit:
            return self._reject(rec, RejectReason.FUTURE_SKEW,
                                f"timestamp {rec.timestamp_ms} exceeds now {now_ms} "
                                f"+ skew {policy.future_skew_ms}",
                                {"now_ms": now_ms,
                                 "future_skew_ms": policy.future_skew_ms,
                                 "future_limit_ms": future_limit})

        series_key = (rec.device_id, rec.seq_id)
        existing = self._series.get(series_key, {}).get(rec.seq_no)

        # 5) 幂等 / 自相矛盾
        if existing is not None:
            if existing.timestamp_ms == rec.timestamp_ms and existing.value == rec.value:
                return RecordOutcome(
                    status=IngestStatus.DUPLICATE, key=key,
                    detail="identical record already ingested",
                    timestamp_ms=rec.timestamp_ms, value=rec.value,
                    evidence={"existing_timestamp_ms": existing.timestamp_ms,
                              "existing_value": existing.value})
            return self._reject(rec, RejectReason.SEQ_CONFLICT,
                                f"seq_no {rec.seq_no} already present with different "
                                f"payload",
                                {"existing_timestamp_ms": existing.timestamp_ms,
                                 "existing_value": existing.value,
                                 "incoming_timestamp_ms": rec.timestamp_ms,
                                 "incoming_value": rec.value})

        # 6) 迟到窗口：相对序列水线
        wm = self._watermarks.get(series_key)
        if wm is not None:
            late_limit = wm - policy.reorder_delay_ms
            if rec.timestamp_ms < late_limit:
                return self._reject(rec, RejectReason.OUTSIDE_REORDER_WINDOW,
                                    f"timestamp {rec.timestamp_ms} older than watermark "
                                    f"{wm} - delay {policy.reorder_delay_ms} = {late_limit}",
                                    {"watermark_ms": wm,
                                     "reorder_delay_ms": policy.reorder_delay_ms,
                                     "late_limit_ms": late_limit})

        # 7) 采样率：与该序列任一已接受记录的最小时间差不得低于阈值
        if policy.min_sample_interval_ms > 0:
            gap, neighbour = self._min_gap(series_key, rec.timestamp_ms)
            if neighbour is not None and gap < policy.min_sample_interval_ms:
                return self._reject(rec, RejectReason.SAMPLE_RATE_VIOLATION,
                                    f"gap {gap}ms to record at {neighbour} < min interval "
                                    f"{policy.min_sample_interval_ms}ms",
                                    {"min_sample_interval_ms": policy.min_sample_interval_ms,
                                     "nearest_timestamp_ms": neighbour,
                                     "gap_ms": gap})

        # ---- 接受：更新归并状态 ----
        self._accept(series_key, rec)
        return RecordOutcome(
            status=IngestStatus.ACCEPTED, key=key,
            detail="accepted and merged",
            timestamp_ms=rec.timestamp_ms, value=rec.value,
            evidence={"watermark_ms_after": self._watermarks[series_key]})

    def _min_gap(self, series_key: tuple[str, str],
                 ts: int) -> tuple[int, int | None]:
        records = self._series.get(series_key, {})
        best: int | None = None
        neighbour: int | None = None
        for other in records.values():
            gap = abs(ts - other.timestamp_ms)
            if best is None or gap < best:
                best, neighbour = gap, other.timestamp_ms
        return (best if best is not None else 0), neighbour

    def _accept(self, series_key: tuple[str, str], rec: TelemetryRecord) -> None:
        device_id, seq_id = series_key
        self._series.setdefault(series_key, {})[rec.seq_no] = rec

        prev_wm = self._watermarks.get(series_key)
        self._watermarks[series_key] = (
            rec.timestamp_ms if prev_wm is None else max(prev_wm, rec.timestamp_ms))

        device = self._stats.setdefault(device_id, {})
        stats = device.get(seq_id)
        if stats is None:
            stats = SeriesStats(
                cluster_id=self.cluster_id, device_id=device_id, seq_id=seq_id,
                count=1, min_value=rec.value, max_value=rec.value,
                sum_value=float(rec.value), first_seq_no=rec.seq_no,
                last_seq_no=rec.seq_no, min_timestamp_ms=rec.timestamp_ms,
                max_timestamp_ms=rec.timestamp_ms)
            device[seq_id] = stats
            return

        stats.count += 1
        stats.sum_value += float(rec.value)
        stats.min_value = min(stats.min_value, rec.value)
        stats.max_value = max(stats.max_value, rec.value)
        stats.first_seq_no = min(stats.first_seq_no, rec.seq_no)
        stats.last_seq_no = max(stats.last_seq_no, rec.seq_no)
        stats.min_timestamp_ms = min(stats.min_timestamp_ms, rec.timestamp_ms)
        stats.max_timestamp_ms = max(stats.max_timestamp_ms, rec.timestamp_ms)

    # ---- 读取视图 -----------------------------------------------------------

    def all_records(self) -> list[TelemetryRecord]:
        """返回该集群已接受记录的副本（按确定顺序），供联邦层跨集群去重。"""

        with self._lock:
            out: list[TelemetryRecord] = []
            for (device_id, seq_id) in sorted(self._series):
                for seq_no in sorted(self._series[(device_id, seq_id)]):
                    out.append(self._series[(device_id, seq_id)][seq_no])
            return [replace(r) for r in out]

    def snapshot(self) -> ClusterSnapshot:
        with self._lock:
            devices: dict[str, dict[str, SeriesStats]] = {}
            total = 0
            for device_id, seqs in self._stats.items():
                devices[device_id] = {
                    seq_id: replace(s) for seq_id, s in sorted(seqs.items())}
                total += sum(s.count for s in seqs.values())
            return ClusterSnapshot(
                cluster_id=self.cluster_id, online=self.online,
                devices=devices, record_count=total)
