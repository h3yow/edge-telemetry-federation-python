"""联邦归并测试：去重、集群切换、完整性降级、恢复补报、跨集群冲突。"""

from __future__ import annotations

import logging

from edge_telemetry.federation import Federation
from edge_telemetry.models import Completeness, StorePolicy, TelemetryRecord
from edge_telemetry.store import ClusterStore

logger = logging.getLogger("telemetry.tests")

NOW = 2_000_000


def R(cid, no, ts, value=1.0, *, device="d1", seq="temp"):
    return TelemetryRecord(cluster_id=cid, device_id=device, seq_id=seq,
                           seq_no=no, timestamp_ms=ts, value=value)


def log_view(title: str, view) -> None:
    logger.info("=== %s: completeness=%s online=%s offline=%s errors=%s ===",
                title, view.completeness.value, view.online_clusters,
                view.offline_clusters, view.errors)
    for d, dv in sorted(view.devices.items()):
        for sid, s in sorted(dv.series.items()):
            logger.info(
                "VIEW device=%s seq=%s count=%s min=%s max=%s sum=%s sources=%s",
                d, sid, s.count, s.min_value, s.max_value, s.sum_value, dv.sources)
        for c in dv.conflicts:
            logger.info("CONFLICT device=%s %s", d, c)


def test_no_cluster_and_complete():
    fed = Federation()
    assert fed.query().completeness is Completeness.NO_CLUSTER

    fed.register(ClusterStore("c1", StorePolicy()))
    fed.cluster("c1").ingest_batch([R("c1", 1, NOW, 1.0)], NOW)
    view = fed.query()
    log_view("complete", view)
    assert view.completeness is Completeness.COMPLETE
    assert view.devices["d1"].series["temp"].count == 1


def test_cluster_offline_degraded_but_others_returned():
    fed = Federation()
    c1, c2 = ClusterStore("c1"), ClusterStore("c2")
    fed.register(c1)
    fed.register(c2)
    c1.ingest_batch([R("c1", 1, NOW, 3.0)], NOW)
    c2.ingest_batch([R("c2", 10, NOW, 7.0)], NOW)

    fed.set_cluster_online("c1", False)
    view = fed.query()
    log_view("c1-offline", view)
    assert view.completeness is Completeness.DEGRADED
    assert view.offline_clusters == ["c1"]
    # c1 数据不可见，仅 c2
    assert view.devices["d1"].series["temp"].count == 1
    assert view.devices["d1"].sources == ["c2"]


def test_cross_cluster_dedup_on_failover_no_double_count():
    fed = Federation()
    c1, c2 = ClusterStore("c1"), ClusterStore("c2")
    fed.register(c1)
    fed.register(c2)
    # 初始由 c1 上报 #1 #2
    c1.ingest_batch([R("c1", 1, NOW - 100, 1.0), R("c1", 2, NOW - 50, 2.0)], NOW)
    fed.set_cluster_online("c1", False)
    # 切换到 c2，c2 持有同样的 #1 #2 补报 + 新的 #3
    c2.ingest_batch(
        [R("c2", 1, NOW - 100, 1.0), R("c2", 2, NOW - 50, 2.0),
         R("c2", 3, NOW, 3.0)], NOW)

    view = fed.query()
    log_view("failover-c2", view)
    assert view.devices["d1"].series["temp"].count == 3
    assert view.devices["d1"].sources == ["c2"]

    # c1 恢复后再次重复补报 #1 #2，不翻倍、不漂移
    fed.set_cluster_online("c1", True)
    c1.ingest_batch([R("c1", 1, NOW - 100, 1.0)], NOW)
    view2 = fed.query()
    log_view("failover-recovered", view2)
    stats = view2.devices["d1"].series["temp"]
    assert stats.count == 3
    assert (stats.min_value, stats.max_value, stats.sum_value) == (1.0, 3.0, 6.0)
    assert view2.devices["d1"].sources == ["c1", "c2"]
    assert view2.completeness is Completeness.COMPLETE


def test_cross_cluster_conflict_excluded_and_flagged():
    fed = Federation()
    c1, c2 = ClusterStore("c1"), ClusterStore("c2")
    fed.register(c1)
    fed.register(c2)
    c1.ingest_batch([R("c1", 1, NOW, 1.0)], NOW)
    c2.ingest_batch([R("c2", 1, NOW, 9.0)], NOW)  # 同序号不同值
    view = fed.query()
    log_view("conflict", view)
    # 冲突序号不纳入统计：该序列无有效记录，仅在 conflicts 中可见
    assert "temp" not in view.devices["d1"].series
    assert view.devices["d1"].record_count == 0
    assert any("temp#1" in c for c in view.devices["d1"].conflicts)


def test_same_payload_from_two_clusters_counts_once():
    fed = Federation()
    c1, c2 = ClusterStore("c1"), ClusterStore("c2")
    fed.register(c1)
    fed.register(c2)
    c1.ingest_batch([R("c1", 1, NOW, 5.0)], NOW)
    c2.ingest_batch([R("c2", 1, NOW, 5.0)], NOW)
    view = fed.query()
    log_view("same-payload-two-clusters", view)
    assert view.devices["d1"].series["temp"].count == 1
    assert view.devices["d1"].sources == ["c1", "c2"]


def test_three_clusters_bucket_merge_order_independent():
    """同序号在 3 个集群中：两集群一致 + 一集群矛盾 => 判冲突；
    改变集群注册顺序不影响结论。"""

    payloads = {
        "c1": [R("c1", 1, NOW, 1.0), R("c1", 2, NOW, 2.0)],
        "c2": [R("c2", 1, NOW, 1.0)],          # 与 c1 一致
        "c3": [R("c3", 1, NOW, 9.0), R("c3", 3, NOW, 3.0)],  # #1 矛盾，#3 新数据
    }

    def run(order):
        fed = Federation()
        for cid in order:
            store = ClusterStore(cid)
            fed.register(store)
            store.ingest_batch(payloads[cid], NOW)
        view = fed.query()
        dv = view.devices["d1"]
        return (sorted(dv.series.keys()),
                dv.series["temp"].count if "temp" in dv.series else 0,
                tuple(dv.conflicts))

    a = run(["c1", "c2", "c3"])
    b = run(["c3", "c2", "c1"])
    c = run(["c2", "c3", "c1"])
    logger.info("BUCKET-MERGE result=%s", a)
    assert a == b == c
    # #1 冲突剔除；#2、#3 有效 => count=2，冲突列表含 temp#1
    assert a[1] == 2
    assert any("temp#1" in x for x in a[2])
