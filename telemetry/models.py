"""Type definitions for telemetry ingest / merge / query."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class RejectReason(enum.Enum):
    """Machine-distinguishable rejection reasons."""

    DUPLICATE_CONFLICT = "duplicate_conflict"  # same (device, seq), different payload
    STALE_SEQUENCE = "stale_sequence"          # seq below accepted window
    SEQ_TOO_FAR_AHEAD = "seq_too_far_ahead"    # seq beyond accepted window
    TIMESTAMP_OUT_OF_RANGE = "timestamp_out_of_range"  # outside time window policy
    INVALID_SAMPLE_RATE = "invalid_sample_rate"        # sample_rate not in (0, 1]
    INVALID_PAYLOAD = "invalid_payload"                # malformed record fields


class Decision(enum.Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"  # idempotent replay of identical record
    REJECTED = "rejected"


@dataclass(frozen=True)
class TelemetryRecord:
    device_id: str
    seq: int                 # per-device collection sequence number
    timestamp: float         # epoch seconds of collection
    metric: str
    value: float
    sample_rate: float = 1.0  # (0, 1]; weight = 1 / sample_rate


@dataclass(frozen=True)
class Batch:
    cluster_id: str
    records: tuple[TelemetryRecord, ...]


@dataclass
class RecordDecision:
    record: TelemetryRecord
    decision: Decision
    reason: RejectReason | None = None
    detail: str = ""


@dataclass
class BatchResult:
    cluster_id: str
    decisions: list[RecordDecision] = field(default_factory=list)

    @property
    def accepted(self) -> int:
        return sum(1 for d in self.decisions if d.decision is Decision.ACCEPTED)

    @property
    def duplicates(self) -> int:
        return sum(1 for d in self.decisions if d.decision is Decision.DUPLICATE)

    @property
    def rejected(self) -> int:
        return sum(1 for d in self.decisions if d.decision is Decision.REJECTED)


@dataclass
class DeviceStats:
    device_id: str
    record_count: int = 0
    weighted_count: float = 0.0
    value_sum: float = 0.0
    seq_min: int | None = None
    seq_max: int | None = None
    missing_seqs: tuple[int, ...] = ()
    first_timestamp: float | None = None
    last_timestamp: float | None = None


@dataclass
class QueryResult:
    stats: DeviceStats | None
    clusters_total: int
    clusters_online: int
    clusters_missing: tuple[str, ...] = ()
    complete: bool = True
