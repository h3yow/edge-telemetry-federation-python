# Edge Telemetry Federation

边缘设备遥测的汇聚与查询服务。用本地存储模拟多集群设备上报，保证在
**重复上报、乱序到达、集群上下线切换、并发写入** 下数据状态可预期。

## 结构

- `telemetry/models.py` — 类型定义：记录、批次、判定（含可区分的拒绝原因）、设备统计、查询结果。
- `telemetry/store.py` — 单集群本地存储 `ClusterStore`：幂等、与到达顺序无关的归并。
- `telemetry/federation.py` — 多集群归并视图 `Federation`：集群上下线切换与跨集群去重。
- `main.py` — FastAPI HTTP 接口。
- `tests/` — 覆盖重复上报、乱序、超范围拒绝、集群切换、并发写入。

## 上报与归并规则

每条记录以 `(device_id, seq)` 为去重键（`seq` 为设备采集序列号）：

| 情形 | 判定 | 说明 |
| --- | --- | --- |
| 新记录 | `accepted` | 入库并推进设备高水位线 |
| 同键同内容重报 | `duplicate` | 幂等忽略，**不重复计数** |
| 同键不同内容 | `rejected / duplicate_conflict` | 自相矛盾，保留原值 |
| `seq < 高水位 - max_seq_gap` | `rejected / stale_sequence` | 过旧，超出接受窗口 |
| `seq > 高水位 + max_seq_gap` | `rejected / seq_too_far_ahead` | 超前，超出接受窗口 |
| 时间戳超出 `[now - max_past, now + max_future]` | `rejected / timestamp_out_of_range` | 时间窗策略丢弃 |
| `sample_rate ∉ (0, 1]` | `rejected / invalid_sample_rate` | 采样率非法 |

- **乱序/迟到**：窗口内的乱序记录照常接受；统计由已存储集合按 `seq` 派生，
  与到达顺序无关，因此交错并发写入收敛到同一状态。
- **采样率**：统计中的 `weighted_count = Σ 1/sample_rate`，丢弃/加权依据
  （判定原因与 detail）随每批结果返回，可逐条核对。
- 每次上报返回逐条 `decision + reason + detail`，拒绝原因机器可区分。

## 集群切换的完整性语义

- 查询跨所有**在线**集群归并；同一 `(device_id, seq)` 在多个集群出现只计一次
  （例如改道重报），恢复后的补报不会计数翻倍、历史结论不漂移。
- 集群离线时查询仍返回其余集群的有效结果，并标注：
  `complete=false`、`clusters_missing=[...]`、`clusters_online/total`。
- 离线集群拒绝写入（HTTP 409）；恢复上线后重放历史批次全部判为
  `duplicate`，统计不变。

## HTTP 接口

- `POST /clusters/{id}/batches` — 批量上报，可带 `now` 作为逻辑时钟（便于确定性测试）。
- `POST /clusters/{id}/status` — `{"online": true|false}` 集群上下线。
- `GET /devices/{id}/stats` — 归并后的设备统计与完整性标注。

## 验证方法

```bash
uv run pytest tests/ -q        # 运行全部测试（日志打印输入批次与判定依据）
uv run python main.py          # 启动服务（127.0.0.1，随机端口）
```

测试覆盖：

- `tests/test_store.py` — 重复上报幂等、乱序/迟到合并、矛盾记录与各类
  超范围拒绝（原因可区分）、采样率加权。
- `tests/test_federation.py` — 集群离线部分查询、恢复补报不翻倍、
  跨集群去重、多线程交错写入收敛一致。
- `tests/test_api.py` — HTTP 端到端：上报 → 切换 → 完整性标注 → 恢复重放。
