# 边缘设备遥测汇聚与多集群联邦查询

本服务用**本地内存存储**模拟多个边缘集群的设备遥测上报，提供批量上报、幂等归并、
乱序/迟到合并、多集群联邦视图与集群上下线切换。不依赖任何外部服务、真实凭据或
不可访问资源；仅使用 FastAPI（HTTP）与标准库。

- 语言/运行：Python 3.14，包管理 uv
- 并发模型：每集群一把可重入锁，整批原子应用；联邦查询读快照
- 时间：时间戳统一为 **epoch 毫秒整数**；`now` 由可注入 `Clock` 提供，测试用
  `FixedClock`，保证判定完全确定、可复现

## 目录

| 模块 | 职责 |
| --- | --- |
| `edge_telemetry/models.py` | 领域类型、拒绝原因码、完整性枚举、策略 |
| `edge_telemetry/clock.py` | 可注入时钟（系统时钟 / 固定时钟） |
| `edge_telemetry/store.py` | 单集群：批量校验、幂等归并、序列统计 |
| `edge_telemetry/federation.py` | 多集群：跨集群去重归并视图、完整性标注 |
| `edge_telemetry/service.py` | 应用门面：注册集群、上报、查询、判定日志 |
| `edge_telemetry/api.py` | FastAPI 路由 |
| `tests/` | 单测 + HTTP 集成测试 + 并发一致性测试 |

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/clusters/{cluster_id}/ingest` | 向某集群批量上报遥测 |
| GET | `/api/federation/view` | 多集群归并视图（带完整性标注） |
| GET | `/api/federation/devices/{device_id}` | 单设备归并视图 |
| POST | `/api/clusters/{cluster_id}/status` | 集群上线/下线切换，body `{"online": false}` |

上报 body：

```json
{
  "records": [
    {"device_id": "dev-1", "seq_id": "temp", "seq_no": 1,
     "timestamp_ms": 1700000000000, "value": 21.5}
  ]
}
```

每条记录的处置结果（`outcomes[]`）含 `status`、`reason`、`detail` 与 `evidence`，
`evidence` 给出判定时使用的水线、窗口边界、被比对记录等，可逐条核对。

## 一、上报与归并规则

### 幂等键（同集群内）

```
key = (cluster_id, device_id, seq_id, seq_no)
```

- **完全相同**（`timestamp_ms` 与 `value` 都一致）的重复上报 => `duplicate`，
  不重复计数，可安全重试/补报。
- 同键但 `timestamp_ms` 或 `value` 不一致 => 判定为**自相矛盾**，拒绝原因
  `seq_conflict`，与“重复”严格区分；已有数据不变。

### 校验顺序与拒绝原因（互斥、可区分）

按固定顺序逐条判定，先命中先生效：

1. `cluster_offline` —— 目标集群离线
2. `malformed` —— `device_id`/`seq_id` 为空、`seq_no` 非非负整数、`value` 非数值
3. `invalid_timestamp` —— 时间戳不是非负整数
4. `value_out_of_range` —— 指标值超出策略配置的 `[value_min, value_max]`
5. `future_skew` —— `timestamp_ms > now + future_skew_ms`（容忍时钟小幅前移）
6. 幂等命中 `duplicate` 或冲突 `seq_conflict`
7. `outside_reorder_window` —— 见下
8. `sample_rate_violation` —— 见下

未命中任何拒绝条件 => `accepted`，并入该序列聚合。

### 乱序 / 迟到合并（确定性规则）

- 每个 `(device_id, seq_id)` 维护一条**水线** = 已接受记录的最大采集时间。
- 接收迟到下限：`late_limit = watermark - reorder_delay_ms`。
  - `timestamp_ms >= late_limit`：迟到但在接受范围内，**照常合并**（更新
    min/max/sum、序号边界、时间边界），不因为“先到的序号大”而丢弃。
  - `timestamp_ms < late_limit`：超出接受范围，拒绝 `outside_reorder_window`，
    `evidence` 中给出 `watermark_ms / reorder_delay_ms / late_limit_ms`。
- 未来数据：`timestamp_ms > now + future_skew_ms` 拒绝 `future_skew`。
- 水线只在记录被接受后推进，因此“拒绝”不会移动窗口。

### 采样率丢弃策略（可核对依据）

按序列可配置 `min_sample_interval_ms`（默认 0 表示不启用）。新记录与该序列
**任一已接受记录**的最小时间差若小于该阈值，拒绝 `sample_rate_violation`，
`evidence` 给出 `nearest_timestamp_ms` 与实际 `gap_ms`。判定依据与阈值均在
响应和日志中可核对。该策略与幂等互不冲突：完全相同的补报在第 6 步即命中
`duplicate`，不会再走采样率检查。

### 序列统计

接受后对每个 `(device_id, seq_id)` 维护：`count、min/max/sum(value)`、
`first/last_seq_no`、`min/max_timestamp_ms`。乱序合并只影响边界与累加，结果
与到达顺序无关（在接受范围内）。

## 二、多集群归并与集群切换语义

### 跨集群幂等键

```
federation_key = (device_id, seq_id, seq_no)
```

集群切换（failover）后，新集群可能持有与旧集群相同序号的记录，旧集群恢复后
也可能补报。联邦查询对每个序号做**单趟分桶归并**：

- 按 `(timestamp_ms, value)` 对各集群记录分桶；
- 恰好一种载荷 => 接受，计数一次，持有该载荷的所有集群都列入 `sources`；
- 存在多种载荷 => 判定跨集群**矛盾**，该序号**不纳入统计**，并在设备的
  `conflicts` 中列出（如 `temp#1: 2 distinct payloads across ['c1','c2']`）。

结论只取决于“有哪些不同载荷”，与集群遍历顺序、上报先后无关，因此：

- **重复补报 / 重叠持有不会翻倍**：同一序号在任意多个集群中重复出现只计一次；
- **历史结论不漂移**：集群恢复后再次补报，不改变 count/sum/min/max。

### 完整性语义

`GET /api/federation/view` 仅读取**在线**集群，并标注：

- `completeness = complete`：所有已注册集群均在线并成功读取；
- `completeness = degraded`：存在离线或读取异常的集群，
  `offline_clusters` / `errors` 列出受影响集群，结果仅来自
  `online_clusters`；
- `completeness = no_cluster`：没有任何已注册/可读集群。

单个集群不可用**不抛整体错误**：其余集群数据照常返回。设备的 `sources`
标明每条归并结论实际由哪些集群贡献，便于核对降级期间的数据来源。

> 注意：离线集群的数据在离线期间不参与视图（模拟不可达）；其重新上线后，
> 联邦层按上述跨集群键去重，重叠部分不会重复计数。

## 三、并发一致性

- `ClusterStore.ingest_batch` 在集群级 `RLock` 内**整批原子应用**，多线程
  交错提交时，每条幂等键恰好被接受一次；其余为 `duplicate`。
- 联邦查询在联邦锁内对各在线集群取记录快照后做无状态归并，读不阻塞写之外
  的共享中间状态。
- 并发测试以多线程切碎、乱序、重复提交同一批数据，断言最终
  `count/sum/min/max/序号边界` 与串行结果一致。

## 四、验证方法

```bash
# 安装依赖（uv 会使用项目内配置的镜像源，无需外部服务）
uv sync

# 运行全部测试；pytest 已配置日志，终端会打印每个输入批次(INPUT)与判定(DECISION)
uv run pytest

# 只跑某类场景
uv run pytest tests/test_store.py        # 重复/乱序/越界/采样率
uv run pytest tests/test_federation.py  # 集群切换/跨集群去重/冲突
uv run pytest tests/test_concurrency.py # 并发写入一致性
uv run pytest tests/test_api.py         # HTTP 端到端

# 启动服务（本地内存，无外部依赖）
uv run python main.py                  # http://127.0.0.1:8000/docs
```

### 手工验证一次完整切换生命周期

```bash
# cluster-a 上报 #1 #2
curl -s -XPOST localhost:8000/api/clusters/cluster-a/ingest -H 'content-type: application/json' -d '{
 "records":[{"device_id":"d1","seq_id":"temp","seq_no":1,"timestamp_ms":1700000000000,"value":21.5},
            {"device_id":"d1","seq_id":"temp","seq_no":2,"timestamp_ms":1700000001000,"value":22.0}]}'

# 重复补报 => duplicate，不计数
curl -s -XPOST localhost:8000/api/clusters/cluster-a/ingest -H 'content-type: application/json' -d '{
 "records":[{"device_id":"d1","seq_id":"temp","seq_no":1,"timestamp_ms":1700000000000,"value":21.5}]}'

# cluster-b 持有相同 #1#2 + 新 #3（集群切换），联邦视图 count 仍为 3
curl -s localhost:8000/api/federation/view

# cluster-a 下线 => degraded，仅 cluster-b；再上线重复补报 => count 不翻倍
curl -s -XPOST localhost:8000/api/clusters/cluster-a/status -H 'content-type: application/json' -d '{"online":false}'
curl -s localhost:8000/api/federation/view
```

### 测试覆盖场景对照

| 需求场景 | 测试 |
| --- | --- |
| 重复上报幂等 | `test_store.py::test_duplicate_idempotent`、`test_concurrent_repeated_batches_are_idempotent` |
| 乱序/迟到合并 | `test_out_of_order_inside_window_merged`、`test_batch_reorder_independent[*]` |
| 超范围拒绝、原因可区分 | `test_value_out_of_range`、`test_future_skew`、`test_outside_reorder_window_rejected`、`test_sample_rate_violation`、`test_seq_conflict_distinguished_from_duplicate` |
| 集群切换/恢复补报不翻倍 | `test_federation.py::test_cross_cluster_dedup_on_failover_no_double_count`、`test_api.py::test_federated_view_and_failover` |
| 部分集群不可用仍返回 + 完整性 | `test_cluster_offline_degraded_but_others_returned` |
| 跨集群矛盾剔除与标注 | `test_cross_cluster_conflict_excluded_and_flagged`、`test_three_clusters_bucket_merge_order_independent` |
| 并发批量写入聚合一致 | `test_concurrency.py::*` |

所有测试在日志中以 `INPUT ...` 打印输入批次、以 `DECISION/OUTCOME ... evidence=...`
打印判定结论与依据，可逐条核对。
