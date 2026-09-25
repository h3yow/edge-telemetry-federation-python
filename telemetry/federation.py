"""Federation layer: multi-cluster merge view with online/offline switching."""

from __future__ import annotations

import threading

from .models import Batch, BatchResult, DeviceStats, QueryResult
from .store import ClusterStore


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

    def query_device(self, device_id: str) -> QueryResult:
        with self._lock:
            snapshot = [
                (cid, store, self._online[cid])
                for cid, store in sorted(self._stores.items())
            ]
        total = len(snapshot)
        missing = tuple(cid for cid, _s, on in snapshot if not on)
        merged: DeviceStats | None = None
        seen_seqs: set[int] = set()
        for _cid, store, online in snapshot:
            if not online:
                continue
            stats = store.device_stats(device_id)
            if stats is None:
                continue
            # Cross-cluster merge: the same (device_id, seq) may exist in
            # several clusters (e.g. re-routed reports); count it once.
            recs = store.device_records(device_id)
            new_recs = [r for r in recs if r.seq not in seen_seqs]
            seen_seqs.update(r.seq for r in new_recs)
            if merged is None:
                merged = DeviceStats(device_id=device_id)
            if new_recs:
                merged.record_count += len(new_recs)
                merged.weighted_count += sum(1.0 / r.sample_rate for r in new_recs)
                merged.value_sum += sum(r.value for r in new_recs)
                ts = [r.timestamp for r in new_recs]
                merged.first_timestamp = (
                    min(merged.first_timestamp, *ts)
                    if merged.first_timestamp is not None else min(ts)
                )
                merged.last_timestamp = (
                    max(merged.last_timestamp, *ts)
                    if merged.last_timestamp is not None else max(ts)
                )
        if merged is not None and merged.record_count > 0:
            seqs = sorted(seen_seqs)
            merged.seq_min, merged.seq_max = seqs[0], seqs[-1]
            have = set(seqs)
            merged.missing_seqs = tuple(
                s for s in range(seqs[0], seqs[-1] + 1) if s not in have
            )
        return QueryResult(
            stats=merged if merged and merged.record_count else None,
            clusters_total=total,
            clusters_online=total - len(missing),
            clusters_missing=missing,
            complete=not missing,
        )
