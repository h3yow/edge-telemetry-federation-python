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


@dataclass(frozen=True)
class ConflictVersion:
    """One cluster's reported version of a conflicting (device, seq)."""

    cluster_id: str
    timestamp: float
    metric: str
    value: float
    sample_rate: float


@dataclass(frozen=True)
class SeqConflict:
    """A (device, seq) reported with >= 2 distinct payloads across clusters.

    The collection sequence is excluded from the merged statistics; none of
    the versions is treated as the single valid value. This is distinct from
    a missing cluster (offline), which is reported via clusters_missing.
    """

    device_id: str
    seq: int
    versions: tuple[ConflictVersion, ...]   # sorted by cluster_id
    cluster_ids: tuple[str, ...]            # sorted, distinct clusters involved

    @property
    def version_count(self) -> int:
        return len(self.versions)


@dataclass
class QueryResult:
    stats: DeviceStats | None
    clusters_total: int
    clusters_online: int
    clusters_missing: tuple[str, ...] = ()
    complete: bool = True
    conflicts: tuple[SeqConflict, ...] = ()

    @property
    def has_conflicts(self) -> bool:
        return bool(self.conflicts)


@dataclass
class BatchDashboardResult:
    """Batch dashboard view over many devices plus an overall rollup."""

    results: dict[str, QueryResult]
    devices_total: int = 0
    devices_with_data: int = 0
    devices_with_conflicts: int = 0
    complete: bool = True
    clusters_missing: tuple[str, ...] = ()
    total_record_count: int = 0
    total_conflicts: int = 0
