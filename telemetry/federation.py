"""Federation layer: multi-cluster merge view with online/offline switching.

Merge semantics (shared by the single-device query and the batch dashboard,
pure/deterministic so conclusions never drift with ingest order):

- Records are keyed by ``(device_id, seq)`` *across clusters*.
- If every online cluster that reports a seq carries the exact same payload,
  the seq is counted exactly once (identical re-routed / replayed reports).
- If the same seq is reported with *different payloads* by different online
  clusters, the seq is a **conflict**: it is excluded from the merged stats
  entirely (no version is treated as canonical, and versions are never
  flattened into one) and reported in ``conflicts`` with the per-version
  cluster breakdown.
- An offline cluster simply does not participate; that is *incompleteness*
  (``complete=False`` / ``clusters_missing``), never a conflict.

A batch dashboard read takes one snapshot of every online cluster while
holding the federation lock (the same lock ingests take first), so it is a
point-in-time read: it can never observe a half-applied concurrent write.
"""

from __future__ import annotations

import logging
import threading

from .models import (
    Batch,
    BatchQueryResult,
    BatchResult,
    ConflictVariant,
    DashboardSummary,
    DeviceStats,
    DeviceView,
    QueryResult,
    SeqConflict,
    TelemetryRecord,
)
from .store import ClusterStore

log = logging.getLogger("telemetry.federation")


def _payload_key(r: TelemetryRecord) -> tuple:
    return (r.timestamp, r.metric, r.value, r.sample_rate)


def merge_device(
    device_id: str,
    per_cluster: "list[tuple[str, list[TelemetryRecord]]]",
) -> tuple[DeviceStats | None, tuple[SeqConflict, ...]]:
    """Merge one device's records from online clusters.

    ``per_cluster`` must be ordered by cluster id; the result is independent
    of that order anyway (all grouping uses sorted ids). Returns the stats
    over clean seqs and the tuple of cross-cluster conflicts.
    """
    # seq -> payload key -> clusters reporting that exact version
    versions: dict[int, dict[tuple, list[str]]] = {}
    representatives: dict[int, TelemetryRecord] = {}
    for cid, recs in per_cluster:  # clusters already sorted by caller
        for r in recs:
            if r.device_id != device_id:
                continue
            key = _payload_key(r)
            buckets = versions.setdefault(r.seq, {})
            buckets.setdefault(key, []).append(cid)
            representatives.setdefault((r.seq, key), r)

    clean: list[TelemetryRecord] = []
    conflicts: list[SeqConflict] = []
    conflict_seqs: set[int] = set()
    for seq in sorted(versions):
        buckets = versions[seq]
        if len(buckets) == 1:
            (key, cids), = buckets.items()
            clean.append(representatives[(seq, key)])
            continue
        conflict_seqs.add(seq)
        variants = tuple(
            ConflictVariant(
                clusters=tuple(sorted(buckets[key])),
                timestamp=key[0], metric=key[1], value=key[2],
                sample_rate=key[3],
            )
            for key in sorted(buckets)
        )
        involved = tuple(sorted({c for key in buckets for c in buckets[key]}))
        conflict = SeqConflict(
            device_id=device_id, seq=seq, clusters=involved,
            version_count=len(buckets), variants=variants,
        )
        conflicts.append(conflict)
        log.info(
            "conflict device=%s seq=%s clusters=%s versions=%s",
            device_id, seq, involved, len(buckets),
        )

    stats: DeviceStats | None = None
    if clean:
        seqs = sorted(r.seq for r in clean)
        have = set(seqs)
        seen_all = have | conflict_seqs
        lo, hi = min(seen_all), max(seen_all)
        stats = DeviceStats(
            device_id=device_id,
            record_count=len(clean),
            weighted_count=sum(1.0 / r.sample_rate for r in clean),
            value_sum=sum(r.value for r in clean),
            seq_min=seqs[0],
            seq_max=seqs[-1],
            missing_seqs=tuple(
                s for s in range(lo, hi + 1)
                if s not in have and s not in conflict_seqs
            ),
            first_timestamp=min(r.timestamp for r in clean),
            last_timestamp=max(r.timestamp for r in clean),
        )
    return stats, tuple(conflicts)


class Federation:
    """Owns a set of ClusterStores and merges query results across them."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stores: dict[str, ClusterStore] = {}
        self._online: dict[str, bool] = {}

    def add_cluster(self, store: ClusterStore) -> None:
        with self._lock:
            self._stores[store.cluster_id] = store
            self._online.setdefault(store.cluster_id, True)

    def set_online(self, cluster_id: str, online: bool) -> None:
        with self._lock:
            if cluster_id not in self._stores:
                raise KeyError(f"unknown cluster {cluster_id!r}")
            self._online[cluster_id] = online

    def ingest(self, batch: Batch, now: float | None = None) -> BatchResult:
        with self._lock:
            store = self._stores.get(batch.cluster_id)
            online = self._online.get(batch.cluster_id, False)
        if store is None:
            raise KeyError(f"unknown cluster {batch.cluster_id!r}")
        if not online:
            raise RuntimeError(f"cluster {batch.cluster_id!r} is offline")
        result = store.ingest(batch, now=now)
        log.info(
            "ingest cluster=%s records=%s -> %s",
            batch.cluster_id,
            [(r.device_id, r.seq, r.value, r.sample_rate)
             for r in batch.records],
            [(r.record.seq, r.decision.value,
              r.reason.value if r.reason else "-")
             for r in result.decisions],
        )
        return result

    # -- queries -------------------------------------------------------------

    def _online_snapshot(
        self, device_filter: "set[str] | None"
    ) -> tuple[list[tuple[str, dict[str, list[TelemetryRecord]]]], tuple[str, ...], int]:
        """Caller must hold ``self._lock``. Point-in-time copy of online stores."""
        snaps: list[tuple[str, dict[str, list[TelemetryRecord]]]] = []
        for cid in sorted(self._stores):
            if self._online[cid]:
                snaps.append(
                    (cid, self._stores[cid].records_snapshot(device_filter))
                )
        missing = tuple(
            cid for cid in sorted(self._stores) if not self._online[cid]
        )
        return snaps, missing, len(self._stores)

    def query_device(self, device_id: str) -> QueryResult:
        with self._lock:
            snaps, missing, total = self._online_snapshot({device_id})
        per_cluster = [
            (cid, snap.get(device_id, [])) for cid, snap in snaps
        ]
        stats, conflicts = merge_device(device_id, per_cluster)
        return QueryResult(
            stats=stats,
            clusters_total=total,
            clusters_online=total - len(missing),
            clusters_missing=missing,
            complete=not missing,
            conflicts=conflicts,
        )

    def query_devices(
        self,
        device_ids: "list[str] | tuple[str, ...] | None" = None,
        cluster_ids: "list[str] | tuple[str, ...] | None" = None,
    ) -> BatchQueryResult:
        """Batch dashboard query.

        Device selection (exactly one scope):
        - ``device_ids``: the given device ids (unknown ids still appear, with
          ``stats=None``),
        - ``cluster_ids``: every device known to those clusters,
        - neither: every device known to the federation.

        Every selected device is merged over **all online clusters**, so each
        per-device view is identical to calling ``query_device`` for it.
        """
        with self._lock:
            if cluster_ids is not None:
                scoped = []
                for cid in cluster_ids:
                    if cid not in self._stores:
                        raise KeyError(f"unknown cluster {cid!r}")
                    scoped.append(self._stores[cid])
            else:
                scoped = list(self._stores.values())

            if device_ids is not None:
                targets = sorted(set(device_ids))
            else:
                known: set[str] = set()
                for store in scoped:
                    known.update(store.device_ids())
                targets = sorted(known)

            snaps, missing, total = self._online_snapshot(set(targets) or None)

        views: list[DeviceView] = []
        summary = DashboardSummary(
            devices_total=len(targets),
            devices_with_data=0,
            record_count=0,
            weighted_count=0.0,
            value_sum=0.0,
            complete_devices=0,
            devices_with_conflicts=0,
            conflict_seqs=0,
            clusters_total=total,
            clusters_online=total - len(missing),
            clusters_missing=missing,
        )
        for dev in targets:
            per_cluster = [(cid, snap.get(dev, [])) for cid, snap in snaps]
            stats, conflicts = merge_device(dev, per_cluster)
            view = DeviceView(
                device_id=dev,
                stats=stats,
                clusters_total=total,
                clusters_online=total - len(missing),
                clusters_missing=missing,
                complete=not missing,
                conflicts=conflicts,
            )
            views.append(view)
            if stats is not None:
                summary.devices_with_data += 1
                summary.record_count += stats.record_count
                summary.weighted_count += stats.weighted_count
                summary.value_sum += stats.value_sum
                if not missing:
                    summary.complete_devices += 1
            if conflicts:
                summary.devices_with_conflicts += 1
                summary.conflict_seqs += len(conflicts)

        return BatchQueryResult(devices=tuple(views), summary=summary)
