"""领域模型：边缘设备遥测上报、归并与联邦查询所涉及的类型定义。

这里只放数据结构，不包含业务逻辑。时间戳统一使用整数毫秒（epoch millis），
使乱序/迟到判定与测试断言都是确定性的。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class IngestStatus(str, Enum):
    """单条记录的处置结果。"""

    ACCEPTED = "accepted"        # 首次接受并计入统计
    DUPLICATE = "duplicate"      # 幂等命中：同键重复，不重复计数
    REJECTED = "rejected"        # 明确拒绝，附带可区分的拒绝原因


class RejectReason(str, Enum):
    """拒绝原因码，互斥且可核对。"""

    MALFORMED = "malformed"                    # 字段缺失/类型非法
    INVALID_TIMESTAMP = "invalid_timestamp"    # 时间戳非正或无法解析
    VALUE_OUT_OF_RANGE = "value_out_of_range"  # 指标值越界
    FUTURE_SKEW = "future_skew"                # 超过允许的未来时钟偏移
    OUTSIDE_REORDER_WINDOW = "outside_reorder_window"  # 迟到且超出接受范围
    SAMPLE_RATE_VIOLATION = "sample_rate_violation"    # 小于该序列最小采样间隔
    SEQ_CONFLICT = "seq_conflict"              # 同序列号但内容自相矛盾
    CLUSTER_UNKNOWN = "cluster_unknown"        # 上报到未注册集群
    CLUSTER_OFFLINE = "cluster_offline"        # 集群处于离线状态


class Completeness(str, Enum):
    """联邦视图的数据完整性标注。"""

    COMPLETE = "complete"          # 所有已注册集群均在线并参与
    DEGRADED = "degraded"          # 部分集群不可用，结果仅来自在线集群
    NO_CLUSTER = "no_cluster"      # 没有任何已注册集群


@dataclass(frozen=True)
class RecordKey:
    """同集群内一条遥测记录的幂等键。"""

    cluster_id: str
    device_id: str
    seq_id: str       # 采集序列标识
    seq_no: int       # 序列内单调递增的序号


@dataclass(frozen=True)
class TelemetryRecord:
    """一条设备遥测上报。"""

    cluster_id: str
    device_id: str
    seq_id: str
    seq_no: int
    timestamp_ms: int   # 采集时间
    value: float


@dataclass
class RecordOutcome:
    """单条记录的判定结论，含可核对的判定依据。"""

    status: IngestStatus
    key: RecordKey
    reason: RejectReason | None = None
    detail: str = ""
    timestamp_ms: int = 0
    value: float = 0.0
    # 判定依据快照（如水线、窗口边界、被比对记录），供日志核对
    evidence: dict = field(default_factory=dict)


@dataclass
class IngestResult:
    """一次批量上报的处置汇总。"""

    cluster_id: str
    accepted: int = 0
    duplicate: int = 0
    rejected: int = 0
    outcomes: list[RecordOutcome] = field(default_factory=list)


@dataclass
class SeriesStats:
    """单个采集序列的归并状态。"""

    cluster_id: str
    device_id: str
    seq_id: str
    count: int = 0
    min_value: float = 0.0
    max_value: float = 0.0
    sum_value: float = 0.0
    first_seq_no: int = 0
    last_seq_no: int = 0
    min_timestamp_ms: int = 0
    max_timestamp_ms: int = 0


@dataclass
class DeviceView:
    """联邦归并后的单设备视图（跨在线集群去重）。"""

    device_id: str
    series_count: int
    record_count: int
    sources: list[str] = field(default_factory=list)  # 贡献数据的集群
    series: dict[str, SeriesStats] = field(default_factory=dict)
    conflicts: list[str] = field(default_factory=list)


@dataclass
class FederationView:
    """多集群联邦查询结果，带完整性标注。"""

    completeness: Completeness = Completeness.DEGRADED
    online_clusters: list[str] = field(default_factory=list)
    offline_clusters: list[str] = field(default_factory=list)
    devices: dict[str, DeviceView] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class ClusterSnapshot:
    """单集群内部状态快照，便于测试与调试核对。"""

    cluster_id: str
    online: bool
    devices: dict[str, dict[str, SeriesStats]] = field(default_factory=dict)
    record_count: int = 0


@dataclass(frozen=True)
class SeriesPolicy:
    """单个采集序列的丢弃/接受策略。

    - reorder_delay_ms: 相对已观察到的最大采集时间，迟到多少毫秒内仍可接受。
    - future_skew_ms: 相对参考“现在”，允许的未来时钟偏移上限。
    - min_sample_interval_ms: 与最近一条已接受记录之间的最小采样间隔，
      0 表示不启用采样率检查。
    """

    reorder_delay_ms: int = 30_000
    future_skew_ms: int = 5_000
    min_sample_interval_ms: int = 0
    value_min: float = float("-inf")
    value_max: float = float("inf")


@dataclass(frozen=True)
class StorePolicy:
    """集群存储的默认策略及按序列覆盖项。"""

    default: SeriesPolicy = field(default_factory=SeriesPolicy)
    overrides: dict[tuple[str, str], SeriesPolicy] = field(default_factory=dict)


def to_jsonable(obj):
    """把 dataclass/Enum 等递归转为可 JSON 序列化的普通结构。"""

    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if hasattr(obj, "__dataclass_fields__"):
        return {k: to_jsonable(getattr(obj, k)) for k in obj.__dataclass_fields__}
    if isinstance(obj, float) and (obj == obj):
        return obj
    return obj
