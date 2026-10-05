# Edge Telemetry Federation

边缘设备遥测的汇聚与查询服务。用本地存储模拟多集群设备上报，保证在
**重复上报、乱序到达、集群上下线切换、并发写入** 下数据状态可预期。

## 结构

- `telemetry/models.py` — 类型定义：记录、批次、判定（含可区分的拒绝原因）、设备统计、单设备查询、跨集群冲突、批量看板结果。
- `telemetry/store.py` — 单集群本地存储 `ClusterStore`：幂等、与到达顺序无关的归并，以及一次性一致快照。
- `telemetry/federation.py` — 多集群归并视图 `Federation`：集群上下线切换、跨集群去重、**跨集群冲突剔除**与**批量看板查询**。
- `main.py` — FastAPI HTTP 接口。
- `tests/` — 覆盖重复上报、乱序、超范围拒绝、集群切换、跨集群冲突、恢复补报、批量查询、并发读写。

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

## 跨集群冲突与完整性（两块语义严格区分）

查询按 `(device_id, seq)` 跨**在线**集群归并，纯函数式、与上报/查询
顺序无关，因此同一批数据无论查多少次、各集群先后顺序如何，结论一致：

| 情形 | 处理 | 在结果里的位置 |
| --- | --- | --- |
| 多个在线集群上报的同 `seq` **内容完全一致** | 只计一次（改道/补报不翻倍） | 计入 `stats` |
| 同 `seq` 在不同在线集群出现 **两个及以上不同版本** | **整条采集序列剔除**出汇总口径：不选任何一个版本当有效值，也不把多版本并成一条；多数一致也不“少数服从多数” | 单列 `conflicts` |
| 某集群**离线**、根本没参与 | 其余在线集群照常归并 | 标记 `complete=false` + `clusters_missing`，**不算冲突** |
| 在线但没报过该设备/该 seq 的集群 | 不参与比较 | 既不算冲突也不算缺失 |

- 冲突项 `conflicts[]` 给出：`seq`、涉及的 `clusters`（排序后）、
  `version_count`，以及每个版本的 `variants`（`clusters` + 完整内容），
  看板上能直接看出“是哪些集群、几个版本对不上”。
- 被剔除的 seq 不会计入统计，也不会混进 `missing_seqs`；真正所有集群都
  没报的 seq 才留在 `missing_seqs`。冲突不影响 `complete` 的含义
  （`complete` 只描述集群是否都在线）。
- 离线集群重新上线后补报：内容相同则按幂等去重，总数不翻倍；内容不同则
  该 seq 进入 `conflicts`，重复查询结论稳定，不发生历史结论漂移。

## 批量巡检看板

`POST /devices/query` 一次查询多台设备，两种选设备范围（二选一，都不传
表示查全部已知设备）：

- `{"device_ids": ["d1", "d2"]}` — 指定一组设备编号（未知编号也会出现在
  结果中，其 `stats=null`）。
- `{"cluster_ids": ["cluster-a"]}` — 给定集群范围：取这些集群已知的全部
  设备；每台设备仍跨**所有在线集群**归并，与单独调用单设备查询等价。

返回 `devices[]`：每台设备的合并统计 `stats`、`missing_seqs`、完整性标注
（`complete` / `clusters_missing` / `clusters_online/total`）、冲突提示
（`has_conflicts` / `conflicts`）；以及整体 `summary`：设备数、有数据设备数、
干净记录总数/加权总数/数值总和、完整设备数、冲突设备数、冲突序列数、
集群在线情况。

- 看板读在联邦锁内对全部在线集群取一次快照（与写入共用同一把锁），是
  时间点一致读：设备很多时也不会读到并发写一半的中间态；`summary` 始终
  等于各设备结果的汇总。

## 兼容性

- 原有 `POST /clusters/{id}/batches`、`POST /clusters/{id}/status`、
  `GET /devices/{id}/stats` 的路径、入参和原有响应字段保持不变。
- 单设备查询仅**新增** `has_conflicts`、`conflicts` 两个字段（无冲突时为
  `false` / `[]`）；老调用方读取旧字段的行为完全不变。

## HTTP 接口

- `POST /clusters/{id}/batches` — 批量上报，可带 `now` 作为逻辑时钟（便于确定性测试）。
- `POST /clusters/{id}/status` — `{"online": true|false}` 集群上下线。
- `GET /devices/{id}/stats` — 归并后的设备统计与完整性标注（含跨集群冲突字段）。
- `POST /devices/query` — 批量看板：按 `device_ids` / `cluster_ids` 选范围，返回每台设备的统计/缺失/完整性/冲突与整体汇总。

## 验证方法

```bash
uv run pytest tests/ -q        # 运行全部测试（日志打印每轮喂进去的数据与判定结果）
uv run python main.py          # 启动服务（127.0.0.1，随机端口）
```

也可直接体验看板与冲突：

```bash
uv run uvicorn main:app --reload
# 两个集群对同一 (device, seq) 上报不同 value -> 该 seq 进 conflicts、不进 stats
curl -s -XPOST localhost:8000/clusters/cluster-a/batches -H 'content-type: application/json' \
  -d '{"now":10000,"records":[{"device_id":"d1","seq":1,"timestamp":9990,"metric":"cpu","value":1.0}]}'
curl -s -XPOST localhost:8000/clusters/cluster-b/batches -H 'content-type: application/json' \
  -d '{"now":10000,"records":[{"device_id":"d1","seq":1,"timestamp":9990,"metric":"cpu","value":9.9}]}'
curl -s localhost:8000/devices/d1/stats
curl -s -XPOST localhost:8000/devices/query -H 'content-type: application/json' -d '{}'
```

测试覆盖：

- `tests/test_store.py` — 重复上报幂等、乱序/迟到合并、矛盾记录与各类
  超范围拒绝（原因可区分）、采样率加权。
- `tests/test_federation.py` — 集群离线部分查询、恢复补报不翻倍、
  跨集群去重、多线程交错写入收敛一致、**补报冲突结论稳定、归并与上报
  顺序无关**。
- `tests/test_conflicts.py` — **冲突序列剔除、版本/集群明细、多数不一致
  仍冲突、冲突与离线缺失区分、冲突不污染 `missing_seqs`**。
- `tests/test_dashboard.py` — **按设备/按集群范围批量查询、整体汇总、
  500 台设备规模、并发写入下的时间点一致性**。
- `tests/test_api.py` — HTTP 端到端：上报 → 切换 → 完整性标注 → 恢复重放，
  以及看板入口与冲突响应。
