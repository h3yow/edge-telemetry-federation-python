"""Single-cluster local store: idempotent, order-independent merge of telemetry."""

from __future__ import annotations

import threading
import time

from .models import (
    Batch,
    BatchResult,
    Decision,
    DeviceStats,
    RecordDecision,
    RejectReason,
    TelemetryRecord,
)


class ClusterStore:
    """Local storage simulating one cluster's device telemetry.

    Merge rules (deterministic, order-independent):
      - key = (device_id, seq); identical replay -> DUPLICATE (not double counted)
      - same key, different payload -> REJECTED / DUPLICATE_CONFLICT
      - seq outside accepted window (relative to the device's current high
        watermark) -> REJECTED (STALE_SEQUENCE / SEQ_TOO_FAR_AHEAD)
      - timestamp outside time policy -> REJECTED / TIMESTAMP_OUT_OF_RANGE
      - sample_rate not in (0, 1] -> REJECTED / INVALID_SAMPLE_RATE

    Because stats are always derived from the stored set keyed by
    (device_id, seq), out-of-order or interleaved concurrent writes converge
    to the same state.
    """

    def __init__(
        self,
        cluster_id: str,
        max_seq_gap: int = 1000,
        max_past_seconds: float = 86400.0,
        max_future_seconds: float = 300.0,
    ) -> None:
        self.cluster_id = cluster_id
        self.max_seq_gap = max_seq_gap
        self.max_past_seconds = max_past_seconds
        self.max_future_seconds = max_future_seconds
        self._lock = threading.Lock()
        self._records: dict[tuple[str, int], TelemetryRecord] = {}
        self._high_watermark: dict[str, int] = {}

    # -- validation helpers -------------------------------------------------

    def _validate(self, rec: TelemetryRecord, now: float) -> RecordDecision | None:
        """Return a REJECTED decision if the record violates a policy, else None."""
        if not rec.device_id or rec.seq < 0 or not rec.metric:
            return RecordDecision(
                rec, Decision.REJECTED, RejectReason.INVALID_PAYLOAD,
                "device_id/metric must be non-empty and seq >= 0",
            )
        if not (0.0 < rec.sample_rate <= 1.0):
            return RecordDecision(
                rec, Decision.REJECTED, RejectReason.INVALID_SAMPLE_RATE,
                f"sample_rate={rec.sample_rate} not in (0, 1]",
            )
        if rec.timestamp < now - self.max_past_seconds:
            return RecordDecision(
                rec, Decision.REJECTED, RejectReason.TIMESTAMP_OUT_OF_RANGE,
                f"timestamp {rec.timestamp} older than {self.max_past_seconds}s before {now}",
            )
        if rec.timestamp > now + self.max_future_seconds:
            return RecordDecision(
                rec, Decision.REJECTED, RejectReason.TIMESTAMP_OUT_OF_RANGE,
                f"timestamp {rec.timestamp} more than {self.max_future_seconds}s ahead of {now}",
            )
        hw = self._high_watermark.get(rec.device_id)
        if hw is not None:
            if rec.seq < hw - self.max_seq_gap:
                return RecordDecision(
                    rec, Decision.REJECTED, RejectReason.STALE_SEQUENCE,
                    f"seq {rec.seq} below high watermark {hw} - gap {self.max_seq_gap}",
                )
            if rec.seq > hw + self.max_seq_gap:
                return RecordDecision(
                    rec, Decision.REJECTED, RejectReason.SEQ_TOO_FAR_AHEAD,
                    f"seq {rec.seq} above high watermark {hw} + gap {self.max_seq_gap}",
                )
        return None

    @staticmethod
    def _same_payload(a: TelemetryRecord, b: TelemetryRecord) -> bool:
        return (
            a.timestamp == b.timestamp
            and a.metric == b.metric
            and a.value == b.value
            and a.sample_rate == b.sample_rate
        )

    # -- public API -----------------------------------------------------------

    def ingest(self, batch: Batch, now: float | None = None) -> BatchResult:
        if now is None:
            now = time.time()
        result = BatchResult(cluster_id=batch.cluster_id)
        with self._lock:
            for rec in batch.records:
                rejected = self._validate(rec, now)
                if rejected is not None:
                    result.decisions.append(rejected)
                    continue
                key = (rec.device_id, rec.seq)
                existing = self._records.get(key)
                if existing is not None:
                    if self._same_payload(existing, rec):
                        result.decisions.append(RecordDecision(
                            rec, Decision.DUPLICATE, None,
                            "identical record already stored; ignored idempotently",
                        ))
                    else:
                        result.decisions.append(RecordDecision(
                            rec, Decision.REJECTED, RejectReason.DUPLICATE_CONFLICT,
                            f"seq {rec.seq} already stored with different payload",
                        ))
                    continue
                self._records[key] = rec
                hw = self._high_watermark.get(rec.device_id)
                if hw is None or rec.seq > hw:
                    self._high_watermark[rec.device_id] = rec.seq
                result.decisions.append(RecordDecision(
                    rec, Decision.ACCEPTED, None, "stored",
                ))
        return result

    def device_stats(self, device_id: str) -> DeviceStats | None:
        with self._lock:
            recs = sorted(
                (r for (dev, _seq), r in self._records.items() if dev == device_id),
                key=lambda r: r.seq,
            )
            if not recs:
                return None
            seqs = [r.seq for r in recs]
            missing = tuple(
                s for s in range(seqs[0], seqs[-1] + 1) if s not in set(seqs)
            )
            return DeviceStats(
                device_id=device_id,
                record_count=len(recs),
                weighted_count=sum(1.0 / r.sample_rate for r in recs),
                value_sum=sum(r.value for r in recs),
                seq_min=seqs[0],
                seq_max=seqs[-1],
                missing_seqs=missing,
                first_timestamp=min(r.timestamp for r in recs),
                last_timestamp=max(r.timestamp for r in recs),
            )

    def device_records(self, device_id: str) -> list[TelemetryRecord]:
        """Stored records for a device, ordered by seq (deterministic)."""
        with self._lock:
            return sorted(
                (r for (dev, _seq), r in self._records.items() if dev == device_id),
                key=lambda r: r.seq,
            )

    def device_ids(self) -> list[str]:
        with self._lock:
            return sorted({dev for (dev, _seq) in self._records})
