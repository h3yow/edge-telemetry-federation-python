# Edge Telemetry Federation

边缘设备遥测的汇聚与查询服务。用本地存储模拟多集群设备上报，保证在
**重复上报、乱序到达、集群上下线切换、并发写入** 下数据状态可预期。

## 结构

- `telemetry/models.py` — 类型定义：记录、批次、判定（含可区分的拒绝原因）、设备统计、查询结果、跨集群冲突（`SeqConflict/ConflictVersion`）、批量看板结果。
- `telemetry/store.py` — 单集群本地存储 `ClusterStore`：幂等、与到达顺序无关的归并。
- `telemetry/federation.py` — 多集群归并视图 `Federation`：集群上下线切换、跨集群去重、确定性冲突剔除（`_merge_device_view` 纯函数）、批量看板快照。
- `main.py` — FastAPI HTTP 接口（含 `/dashboard/query`）。
- `tests/` — 覆盖重复上报、乱序、超范围拒绝、集群切换、跨集群冲突、恢复补报、批量看板、并发读写。

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

## 跨集群冲突判定（与"缺失"区分）

跨集群归并以 `(device_id, seq)` 为单位，对每个采集序号收集所有**在线**集群
上报的版本：

| 情形 | 判定 |
| --- | --- |
| 所有上报集群内容一致（含重报/改道） | 该序号**只计一次**，纳入统计 |
| 存在两个及以上**不同内容**版本 | 该序号判定为**冲突**，整体剔除出汇总口径 |

冲突序号的处理原则：

- **不挑任一版本当唯一有效值**，也**不把多条并成一条**；`record_count`、
  `weighted_count`、`value_sum`、时间范围等统计均不含该序号。
- 结果里通过 `conflicts` 明确标出：`seq`、参与冲突的 `cluster_ids`、
  `version_count`，以及每个版本的 `cluster_id/timestamp/metric/value/sample_rate`，
  可直接看出是哪些集群、几个版本对不上。
- 冲突序号**不计入** `missing_seqs`：它有数据但自相矛盾，不属于"缺口"。
- 判定只取决于"在线集群里该序号有几种不同内容"，与集群遍历/补报先后顺序
  无关，是纯函数式归并；同一批数据查询任意次、按任意集群顺序喂入，结论一致，
  不会历史漂移。集群恢复后若补报**相同**内容，全部判 `duplicate`、统计不变；
  若对"本集群此前没有的序号"补报了与其他集群**不同**的内容，该序号稳定地转为
  冲突；若本集群已存有该序号，则在写入侧即被 `duplicate_conflict` 拒绝，
  原值不变。

**冲突 vs 完整性（缺失）是两件正交的事**：

- `complete=false / clusters_missing=[...]` 表示有集群**没上线**，其数据看不到，
  属于"缺失"。被隐藏集群的版本不参与冲突计算，因此一个序号可能在集群离线时
  看似正常、恢复补报后才暴露冲突——结果里会同时保留两种标注，不会相互掩盖。
- `has_conflicts=true / conflicts=[...]` 表示所有（相关）集群都在，但数据
  互相矛盾。冲突时 `complete` 仍可为 `true`。

## 批量巡检看板

`POST /dashboard/query` 一次查询多台设备，返回每台设备与单设备查询同构的
归并结果（统计、`missing_seqs`、完整性标注、冲突明细），外加整体汇总：

- 选择方式（二选一或都不给）：
  - `{"device_ids": ["dev-1", "dev-2"]}`：显式设备编号；未知编号返回空结果
    并带完整性标注，不报错。
  - `{"cluster_scope": ["cluster-a"]}`：以这些集群持有的设备为查询范围
    （未知集群返回 404）；归并仍跨**所有在线**集群进行，因此范围外集群上的
    同一设备数据仍会参与一致性核对。
  - 两者都不给：查询所有集群已知的全部设备。
- 整体汇总字段：`devices_total / devices_with_data / devices_with_conflicts /
  complete / clusters_missing / total_record_count / total_conflicts`。
- 整次看板查询在同一份集群成员快照下完成（在线状态冻结、各集群数据在
  federation 级锁内读取，且持该锁时不写入集群存储，无死锁），因此看板读数
  不会跨在一次并发写入/切换的前后之间；设备数量多时按目标集合逐台归并，
  无额外的笛卡尔放大。

## 兼容性

- `POST /clusters/{id}/batches`、`POST /clusters/{id}/status`、
  `GET /devices/{id}/stats` 的**既有字段与判定语义保持不变**；单设备查询仅
  **增量**返回 `has_conflicts` 与 `conflicts` 两个字段，旧调用方忽略即可，
  既有统计口径在无冲突场景下与改动前完全一致。
- 上报行为不变：同集群内同键不同内容依旧在写入侧被 `duplicate_conflict`
  拒绝；新规则只作用于跨集群查询归并层。
- 新增能力仅为新接口 `/dashboard/query` 与新增返回字段。

## HTTP 接口

- `POST /clusters/{id}/batches` — 批量上报，可带 `now` 作为逻辑时钟（便于确定性测试）。
- `POST /clusters/{id}/status` — `{"online": true|false}` 集群上下线。
- `GET /devices/{id}/stats` — 归并后的设备统计、完整性标注与冲突明细。
- `POST /dashboard/query` — 多设备批量看板（`device_ids` / `cluster_scope`）与整体汇总。

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
- `tests/test_conflicts.py` — 跨集群冲突剔除（双版本/三版本/多一致一异）、
  冲突与离线缺失正交、恢复后相同补报稳定、分歧补报稳定成冲突、本集群自冲突
  在写入侧拒绝、集群喂入顺序无关、批量看板（按设备/按集群范围/规模）、
  并发看板读与写入的快照一致性。
- `tests/test_api.py` — HTTP 端到端：上报 → 切换 → 完整性标注 → 恢复重放。
- `tests/test_dashboard_api.py` — HTTP 端到端：冲突字段、批量看板
  （按设备/按范围/404）、离线下看板完整性标注。

`pytest` 已开启 `log_cli`，跑测试时会逐轮打印每个批次喂入的记录与对应的
`accepted/duplicate/rejected + reason`，以及每次归并查询的统计、缺口、
完整性与冲突判定。
