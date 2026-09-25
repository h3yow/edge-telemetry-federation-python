"""并发上报一致性测试：交错批量写入后统计必须确定且可解释。"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from edge_telemetry.federation import Federation
from edge_telemetry.models import SeriesPolicy, StorePolicy, TelemetryRecord
from edge_telemetry.store import ClusterStore

logger = logging.getLogger("telemetry.tests")

NOW = 7_000_000


def R(cid, no, ts, value=1.0, *, device="d1", seq="temp"):
    return TelemetryRecord(cluster_id=cid, device_id=device, seq_id=seq,
                           seq_no=no, timestamp_ms=ts, value=value)


def test_concurrent_batches_final_stats_consistent():
    # 宽乱序窗口，使记录无论以何种交错顺序到达都应被接受
    store = ClusterStore("c1", StorePolicy(default=SeriesPolicy(
        reorder_delay_ms=1_000_000, future_skew_ms=1_000_000)))

    # 把 200 条记录切成 10 个乱序批次，多线程并发提交
    records = [R("c1", no, NOW - 1000 + no, float(no)) for no in range(200)]
    shuffled = records[::7] + records[1::7] + records[2::7] + records[3::7] + \
        records[4::7] + records[5::7] + records[6::7]
    batches = [shuffled[i::10] for i in range(10)]

    totals = {"accepted": 0, "duplicate": 0, "rejected": 0}
    lock = threading.Lock()

    def worker(batch):
        res = store.ingest_batch(batch, NOW)
        logger.info("CONCURRENT BATCH size=%d accepted=%d duplicate=%d rejected=%d",
                    len(batch), res.accepted, res.duplicate, res.rejected)
        with lock:
            totals["accepted"] += res.accepted
            totals["duplicate"] += res.duplicate
            totals["rejected"] += res.rejected

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, batches))

    logger.info("CONCURRENT TOTALS %s", totals)
    snap = store.snapshot()
    stats = snap.devices["d1"]["temp"]
    # 200 条唯一记录全部接受，无拒绝；总接受数与计数一致
    assert totals == {"accepted": 200, "duplicate": 0, "rejected": 0}
    assert stats.count == 200
    assert (stats.first_seq_no, stats.last_seq_no) == (0, 199)
    assert stats.sum_value == float(sum(range(200)))
    assert (stats.min_value, stats.max_value) == (0.0, 199.0)


def test_concurrent_repeated_batches_are_idempotent():
    # 同一批记录被多个线程重复提交：最终只计一次
    store = ClusterStore("c1", StorePolicy(default=SeriesPolicy(
        reorder_delay_ms=1_000_000, future_skew_ms=1_000_000)))
    batch = [R("c1", no, NOW - 500 + no, float(no)) for no in range(20)]

    accepted_total = {"n": 0}
    lock = threading.Lock()

    def worker(_):
        res = store.ingest_batch(batch, NOW)
        logger.info("REPEATED BATCH accepted=%d duplicate=%d",
                    res.accepted, res.duplicate)
        with lock:
            accepted_total["n"] += res.accepted

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(16)))

    stats = store.snapshot().devices["d1"]["temp"]
    assert stats.count == 20  # 不翻倍
    assert accepted_total["n"] == 20
    assert stats.sum_value == float(sum(range(20)))


def test_concurrent_failover_reporting_no_double_count():
    # c1 与 c2 并发补报重叠数据，联邦视图必须去重且结论与到达顺序无关
    def run_once() -> dict:
        fed = Federation()
        c1, c2 = ClusterStore("c1"), ClusterStore("c2")
        fed.register(c1)
        fed.register(c2)
        overlap = [R("c1", no, NOW - 100 + no, float(no)) for no in range(10)]
        overlap2 = [R("c2", no, NOW - 100 + no, float(no)) for no in range(10)]
        c2_only = [R("c2", no, NOW + no, float(no)) for no in range(10, 15)]

        with ThreadPoolExecutor(max_workers=4) as pool:
            f1 = pool.submit(c1.ingest_batch, overlap, NOW)
            f2 = pool.submit(c2.ingest_batch, overlap2 + c2_only, NOW)
            f1.result()
            f2.result()

        view = fed.query()
        st = view.devices["d1"].series["temp"]
        return {"count": st.count, "sum": st.sum_value,
                "sources": view.devices["d1"].sources}

    first = run_once()
    for _ in range(5):
        assert run_once() == first
    assert first["count"] == 15
    assert first["sources"] == ["c1", "c2"]
    logger.info("FAILOVER CONCURRENT RESULT %s", first)
