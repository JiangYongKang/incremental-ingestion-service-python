# 增量数据摄取服务（本地模拟版）

用**本地文件存储**模拟"源端推送事件 → 服务幂等摄取 → 目标端落库"的完整链路，
不依赖任何外部服务或真实凭据。服务保证：重复投递、并发投递、异常事件下，
数据状态一致且结果可预期、可通过接口区分。

- 语言/框架：Python 3.14 + FastAPI（uv 管理依赖）
- 存储：`data/state/<source>.json`（摄取状态）+ `data/target/<source>.json`（目标端落库）
- 并发控制：进程内线程锁 + `flock` 进程间文件锁 + 原子写（tmp + fsync + rename）

## 目录结构

```
app/
  models.py    判定结论 Verdict、错误分类 ErrorClass、事件与结果模型
  errors.py    业务异常（非法事件 / 瞬时失败 / 检查点错误）
  retry.py     重试与终止策略（退避 + 最大尝试次数）
  storage.py   本地 JSON 存储、锁、原子持久化、目标端落库
  service.py   核心摄取逻辑：校验→幂等判定→检查点→缓冲→重试→隔离
  main.py      FastAPI 路由
tests/         重复投递 / 并发重复 / 检查点回退 / 畸形事件 / 重启恢复 / HTTP 接口
```

## 快速开始

```bash
uv sync                         # 安装依赖（含 pytest 开发组）
uv run uvicorn app.main:app --port 8000    # 启动服务
# 或： uv run fastapi dev app/main.py
```

环境变量：`INGEST_DATA_DIR`（默认 `data`）、`INGEST_MAX_ATTEMPTS`（默认 5）、`LOG_LEVEL`。

投递一条事件：

```bash
curl -s -X POST http://127.0.0.1:8000/sources/orders/events \
  -H 'content-type: application/json' \
  -d '{"event_id":"evt-1001","seq":1,"op":"upsert",
       "payload":{"key":"order-1","value":{"amount":100}}}'
```

事件结构：

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | 非空字符串 | 事件唯一标识 |
| `seq` | `>=1` 整数 | 来源内单调递增位点 |
| `op` | `upsert` / `delete` | 变更类型 |
| `payload.key` | 字符串/整数 | 目标端记录主键（必填） |
| `payload.value` | 任意 | `upsert` 必填；`delete` 可省 |
| `payload._fail_next` | bool | 仅演示用：让本次投递模拟上游瞬时失败（不持久化） |

## 一、幂等规则

每个来源维护一份**幂等账本** `applied: seq -> {event_id, op, payload}`。
一次投递在来源级临界区（线程锁 + 文件锁）内完成"判定→落库→记账→持久化"：

1. **同一事件重复投递**（同 `seq` 且同 `event_id`，且该位点已生效）
   → 返回 `duplicate`（HTTP 200），返回体带既有 `checkpoint` 与判定原因；
   **不二次落库、不改状态**。
2. **同一位点被不同 event_id 占用** → `rejected / invalid_structure`（HTTP 400）并隔离。
3. **正在缓冲/重试中的同一事件重复投递** → 分别返回 `buffered` / `retrying`，
   不重复登记、不重置已尝试次数。
4. 并发（多线程或多进程）重复投递：恰好 **1 次 accepted**，其余全部 duplicate，
   目标端只有一条记录（由 `test_concurrent_delivery.py` 覆盖）。

判定结论 `verdict` 与 HTTP 状态码：

| verdict | HTTP | 含义 |
|---|---|---|
| `accepted` | 200 | 首次成功应用 |
| `duplicate` | 200 | 幂等命中，未二次写入（与 accepted 同码但 verdict 可区分） |
| `buffered` | 202 | 位点超前，已缓存等待缺口 |
| `retrying` | 202 | 上游暂时失败，已登记可重试 |
| `stale` | 409 | 位点回退到检查点之前（账本无记录） |
| `dead_letter` | 422 | 重试耗尽，终止并隔离 |
| `rejected` | 400 | 非法事件，按错误分类隔离 |

## 二、检查点协议

- 检查点 `checkpoint` 是每个来源**独立**的整数，初值为 0，含义为
  "所有 `seq <= checkpoint` 的事件都已到达终态（生效或死信越过）"。
- **只能随队头事件生效单调推进**，不允许回退、不允许跳跃：
  - 回退：`POST /checkpoint {"seq": <小值>}` → 409 `checkpoint_regression`；
  - 跳过未处理事件：→ 409 拒绝。
- **跨来源串用天然拒绝**：`source` 是路径参数，状态文件、锁、账本、检查点
  完全按来源隔离；A 来源的位点对 B 来源无任何意义。
- **乱序投递**：`seq > checkpoint+1` 的事件进入缓冲；当缺口补齐、队头连续生效时，
  缓冲事件按序级联应用，检查点一次推进到最远连续位点。
- 重启后从磁盘恢复检查点与账本，重复消费只得到 `duplicate`，未完成事件可续跑，
  做到不丢事件、不重复落库。

## 三、错误分类与异常链路

非法事件**绝不静默丢弃**，按可区分原因拒绝并写入 `rejected` 隔离清单，
同时不推进检查点、不落库：

| error_class | 触发条件 |
|---|---|
| `missing_field` | 缺 `event_id/seq/op/payload`，或缺 `payload.key`、upsert 缺 `value` |
| `type_mismatch` | `seq` 非整数/小于 1、`event_id` 非字符串、`payload` 非对象等 |
| `invalid_structure` | 顶层非对象、非法 JSON、`op` 非 upsert/delete、位点被其他事件占用 |
| `checkpoint_regression` | 检查点回退 |
| `transient` | 上游暂时失败（可重试） |
| `poison` | 重试耗尽的毒消息（进入死信区） |

**上游暂时失败的重试与终止策略**（`retry.py`）：

- 失败后事件进入 `retrying` 队列并保留 `attempts`（跨重启保留）；
- 固定退避（默认 0.05s，仅用于解释/调度，接口续跑不强制 sleep）；
- 达到最大尝试次数（默认 5，`INGEST_MAX_ATTEMPTS` 可调）后转入
  **死信隔离区 `dead_letters`**，返回 `dead_letter`（422），响应与日志给出
  "已尝试 N/M、终止重试"的可解释原因；
- 毒消息视为到达终态并越过该位点，**不阻塞同来源后续事件，更不阻塞其他来源**；
- 运维确认后可 `POST /sources/{source}/dead-letters/{seq}/replay` 显式重放，
  成功后补登幂等账本。

## 四、接口一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/sources/{source}/events` | 投递增量事件 |
| POST | `/sources/{source}/retry` | 续跑未完成事件（body 可传 `{"force_fail": true}` 演示连续失败） |
| POST | `/sources/{source}/checkpoint` | 显式检查点断言/推进 `{"seq": n}` |
| POST | `/sources/{source}/dead-letters/{seq}/replay` | 重放死信 |
| GET | `/sources/{source}/status` | 检查点、已生效、缓冲、重试、死信、拒绝清单 |
| GET | `/sources/{source}/target` | 目标端落库快照 |
| GET | `/sources` | 来源列表 |

所有响应均包含 `event_id`、`seq`、`checkpoint`、`verdict`、`reason`，
拒绝类响应额外带 `error_class`，便于区分与追溯。

## 五、本地验证

### 自动化测试

```bash
uv run pytest              # 全部 18 个用例，-s 可查看判定日志
```

测试覆盖（日志中打印事件标识、检查点、判定依据）：

- `test_duplicate_delivery.py`：重复投递只生效一次；重启后仍幂等；
- `test_concurrent_delivery.py`：20 线程并发重复（1 accepted / 19 duplicate）、
  8 进程并发重复（flock 保证 1 accepted）；
- `test_checkpoint_protocol.py`：检查点回退拒绝、跳跃拒绝、跨来源隔离、乱序缓冲级联；
- `test_error_handling.py`：6 类畸形事件分类隔离、瞬时失败重试成功、
  重试耗尽死信且不阻塞后续/其他来源；
- `test_restart_recovery.py`：重启后续跑 retrying 事件、恢复缓冲缺口、重复消费不重写；
- `test_http_api.py`：接口级判定结论与 HTTP 状态码可区分。

### 手工 curl 演练

```bash
uv run uvicorn app.main:app --port 8000 &
B=http://127.0.0.1:8000

# 首次 / 重复
curl -s -X POST $B/sources/orders/events -H 'content-type: application/json' \
  -d '{"event_id":"e1","seq":1,"op":"upsert","payload":{"key":"k1","value":"v1"}}'
curl -s -X POST $B/sources/orders/events -H 'content-type: application/json' \
  -d '{"event_id":"e1","seq":1,"op":"upsert","payload":{"key":"k1","value":"v1"}}'
# -> 第二次 verdict=duplicate

# 检查点回退（409）
curl -s -w '\n%{http_code}\n' -X POST $B/sources/orders/checkpoint \
  -H 'content-type: application/json' -d '{"seq":0}'

# 畸形事件（400 + error_class）
curl -s -X POST $B/sources/orders/events -H 'content-type: application/json' \
  -d '{"event_id":"bad","seq":2}'

# 重试 -> 死信 -> 不阻塞 -> 重放
curl -s -X POST $B/sources/pay/events -H 'content-type: application/json' \
  -d '{"event_id":"p1","seq":1,"op":"upsert","payload":{"key":"p1","value":1,"_fail_next":true}}'
curl -s -X POST $B/sources/pay/retry -H 'content-type: application/json' -d '{"force_fail":true}'
curl -s -X POST $B/sources/pay/retry -H 'content-type: application/json' -d '{"force_fail":true}'
curl -s $B/sources/pay/status
curl -s -X POST $B/sources/pay/dead-letters/1/replay

# 查看落库结果
curl -s $B/sources/orders/target
```

## 一致性保障小结

- **临界区**：同一来源的"判定→落库→记账→推进检查点"在锁内原子完成，
  状态文件原子落盘，崩溃不会读到半截状态；
- **先记账后可见**：落库与账本、检查点在同一临界区持久化，重启后以账本为准，
  重复消费命中 duplicate；
- **顺序约束**：严格按 `seq` 连续应用，乱序只缓冲不错位；
- **故障隔离**：坏消息进 rejected/dead_letters 清单可审计，不影响其他任何来源。
