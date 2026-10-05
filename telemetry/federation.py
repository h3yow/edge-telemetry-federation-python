"""Federation layer: multi-cluster merge view with online/offline switching."""

from __future__ import annotations

import threading

from .models import (
    Batch,
    BatchDashboardResult,
    BatchResult,
    ConflictVersion,
    DeviceStats,
    QueryResult,
    SeqConflict,
)
from .store import ClusterStore


def _payload_sig(r) -> tuple:
    return (r.timestamp, r.metric, r.value, r.sample_rate)


def _merge_device_view(
    device_id: str,
    per_cluster: dict[str, list],
    clusters_total: int,
    clusters_online: int,
    clusters_missing: tuple[str, ...],
) -> QueryResult:
    """Pure, deterministic merge of one device's records across clusters.

    Rules:
      - identical (device, seq) payloads from several clusters count once
      - a seq with two or more DISTINCT payloads across clusters is a
        conflict: every version is reported, and the seq is excluded from
        statistics (no version is treated as the unique valid value)
      - conflicts never merge with each other; ordering of clusters does not
        change the result, so conclusions cannot drift between queries
    """
    by_seq: dict[int, dict[str, object]] = {}
    for cid in sorted(per_cluster):
        for r in per_cluster[cid]:
            by_seq.setdefault(r.seq, {})[cid] = r

    canonical: list = []
    conflicts: list[SeqConflict] = []
    conflict_seqs: set[int] = set()
    for seq in sorted(by_seq):
        cluster_records = by_seq[seq]
        signatures = {_payload_sig(r) for r in cluster_records.values()}
        if len(signatures) > 1:
            versions = tuple(
                ConflictVersion(
                    cluster_id=cid,
                    timestamp=r.timestamp,
                    metric=r.metric,
                    value=r.value,
                    sample_rate=r.sample_rate,
                )
                for cid, r in sorted(cluster_records.items())
            )
            conflicts.append(SeqConflict(
                device_id=device_id,
                seq=seq,
                versions=versions,
                cluster_ids=tuple(sorted(cluster_records)),
            ))
            conflict_seqs.add(seq)
            continue  # quarantined: excluded from every aggregate
        # all reporting clusters agree; take the lowest cluster_id's record
        # deterministically (payloads are identical, the choice is cosmetic)
        cid = min(cluster_records)
        canonical.append(cluster_records[cid])

    stats: DeviceStats | None = None
    if canonical:
        seqs = sorted(r.seq for r in canonical)
        have = set(seqs)
        # gaps only: a quarantined (conflicting) seq is a conflict, not a gap,
        # so it is deliberately excluded from missing_seqs.
        stats = DeviceStats(
            device_id=device_id,
            record_count=len(canonical),
            weighted_count=sum(1.0 / r.sample_rate for r in canonical),
            value_sum=sum(r.value for r in canonical),
            seq_min=seqs[0],
            seq_max=seqs[-1],
            missing_seqs=tuple(
                s for s in range(seqs[0], seqs[-1] + 1)
                if s not in have and s not in conflict_seqs
            ),
            first_timestamp=min(r.timestamp for r in canonical),
            last_timestamp=max(r.timestamp for r in canonical),
        )
    return QueryResult(
        stats=stats,
        clusters_total=clusters_total,
        clusters_online=clusters_online,
        clusters_missing=clusters_missing,
        complete=not clusters_missing,
        conflicts=tuple(conflicts),
    )


class Federation:
    """Owns a set of ClusterStores and merges query results across them.

    - A cluster can be marked offline; queries then skip it and report
      completeness = False with the missing cluster ids, while still
      returning the valid results from the remaining online clusters.
    - Re-ingesting the same batch after a cluster recovers is idempotent:
      counts never double and historical conclusions never drift, because
      each store deduplicates on (device_id, seq).
    """

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
        return store.ingest(batch, now=now)

    # -- internal snapshot ---------------------------------------------------

    def _gather(self, device_id: str) -> QueryResult:
        """Read every online cluster under one federation-level snapshot.

        Online status is frozen and all cluster stores are read while holding
        the federation lock; stores are never written under that lock, so this
        cannot deadlock, and a batch query sees one coherent membership view
        (no device can be read against two different cluster states).
        """
        with self._lock:
            snapshot = [
                (cid, store, self._online[cid])
                for cid, store in sorted(self._stores.items())
            ]
            per_cluster: dict[str, list] = {}
            for cid, store, online in snapshot:
                if online:
                    recs = store.device_records(device_id)
                    if recs:
                        per_cluster[cid] = recs
            missing = tuple(cid for cid, _s, on in snapshot if not on)
            return _merge_device_view(
                device_id, per_cluster,
                clusters_total=len(snapshot),
                clusters_online=len(snapshot) - len(missing),
                clusters_missing=missing,
            )

    def known_device_ids(self) -> list[str]:
        """Union of device ids known by any cluster (online or offline)."""
        with self._lock:
            ids: set[str] = set()
            for store in self._stores.values():
                ids.update(store.device_ids())
        return sorted(ids)

    def query_devices(
        self,
        device_ids: list[str] | None = None,
        cluster_scope: list[str] | None = None,
    ) -> BatchDashboardResult:
        """Batch dashboard: merged view for many devices under one snapshot.

        Selection:
          - ``device_ids``: an explicit list of device ids (unknown ids return
            an empty result with completeness annotations, not an error)
          - ``cluster_scope``: restrict the device universe to data held by
            these clusters; merge still runs over all online clusters, so a
            device whose data also lives elsewhere is still fully reconciled
          - neither: every device known to any cluster

        All devices are gathered within the same membership snapshot, so a
        dashboard read cannot straddle a concurrent ingest/switchover.
        """
        with self._lock:
            snapshot = [
                (cid, store, self._online[cid])
                for cid, store in sorted(self._stores.items())
            ]
            known_by_scope: set[str] | None = None
            if cluster_scope is not None:
                known_by_scope = set()
                for cid in cluster_scope:
                    if cid not in self._stores:
                        raise KeyError(f"unknown cluster {cid!r}")
                    known_by_scope.update(self._stores[cid].device_ids())

            if device_ids is None:
                if known_by_scope is not None:
                    targets = sorted(known_by_scope)
                else:
                    universe: set[str] = set()
                    for _cid, store, _on in snapshot:
                        universe.update(store.device_ids())
                    targets = sorted(universe)
            else:
                # de-duplicate while preserving a deterministic order
                targets = sorted(set(device_ids))
                if known_by_scope is not None:
                    targets = [d for d in targets if d in known_by_scope]

            missing = tuple(cid for cid, _s, on in snapshot if not on)
            total = len(snapshot)
            online = total - len(missing)

            results: dict[str, QueryResult] = {}
            for device_id in targets:
                per_cluster: dict[str, list] = {}
                for cid, store, on in snapshot:
                    if on:
                        recs = store.device_records(device_id)
                        if recs:
                            per_cluster[cid] = recs
                results[device_id] = _merge_device_view(
                    device_id, per_cluster,
                    clusters_total=total,
                    clusters_online=online,
                    clusters_missing=missing,
                )

        with_data = sum(1 for q in results.values() if q.stats is not None)
        with_conflicts = sum(1 for q in results.values() if q.has_conflicts)
        return BatchDashboardResult(
            results=results,
            devices_total=len(results),
            devices_with_data=with_data,
            devices_with_conflicts=with_conflicts,
            complete=not missing,
            clusters_missing=missing,
            total_record_count=sum(
                q.stats.record_count for q in results.values() if q.stats
            ),
            total_conflicts=sum(len(q.conflicts) for q in results.values()),
        )

    def query_device(self, device_id: str) -> QueryResult:
        return self._gather(device_id)
