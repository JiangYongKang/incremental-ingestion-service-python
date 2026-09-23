# 增量数据摄取服务（本地模拟版）

用本地文件存储模拟“源端推送 → 幂等摄取 → 目标端落库”的增量链路，不依赖任何外部服务或真实凭据。
服务基于 FastAPI，可直接本地启动，并通过 HTTP 接口 / pytest 验证以下性质：

- 重复投递、并发投递下，同一事件**只生效一次**，重复请求返回可区分的 `DUPLICATE`，不二次写入、不改既有结果。
- 每个来源的检查点**只能单调推进**；回退返回 `409 CHECKPOINT_ROLLBACK`，跨来源串用返回 `400 SOURCE_MISMATCH`。
- 服务重启或重复消费后**不丢事件、不重复落库**；未完成事件可重投或 `/resume` 续跑。
- 字段缺失 / 类型不匹配 / 结构非法的坏事件按可区分原因**拒绝或隔离**，并可通过故障注入演示**有界重试 → 死信 → 显式重放**。
- 单条坏事件通过推进位点避免阻塞同来源后续事件；来源之间文件与锁完全隔离，互不阻塞。

## 快速开始

```bash
uv sync                       # 安装依赖
uv run pytest                 # 运行全部测试（含判定日志）
uv run uvicorn main:app --port 8011   # 启动服务
# 交互式接口文档： http://127.0.0.1:8011/docs
```

数据默认写入运行目录下的 `data/`，可用环境变量 `INGEST_DATA_DIR` 覆盖。
存储全部为 JSON 文件（临时文件 + 原子替换），进程内用 `RLock`、进程间用 `fcntl.flock` 排他锁。

## 事件格式

```json
{
  "event_id": "evt-1001",   // 必填，非空字符串，幂等键
  "source": "shop",         // 必填，非空字符串，必须与 URL 中的来源一致
  "seq": 12,                // 必填，正整数，来源内用于推进检查点
  "payload": { "any": "json-object" }  // 必填，JSON 对象
}
```

投递到 `POST /v1/sources/{source}/events`。

## 幂等规则

| 账本既有状态 | 再次投递的判定 | HTTP | 是否写库/改结果 |
| --- | --- | --- | --- |
| 不存在 | `APPLIED` | 200 | 首次写入，位点推进到 `seq` |
| `APPLIED` | `DUPLICATE`（`duplicate_of=event_id`） | 200 | 不写入、不覆盖，保留原 payload |
| `QUARANTINED` | `DUPLICATE`（`reason` 回显原隔离原因） | 200 | 不重复隔离 |
| `RETRYING` | 继续续跑，尝试次数 +1，成功后 `APPLIED` | 202 / 200 | 成功才写入一次 |
| `DEAD_LETTER` | `DEAD_LETTER`（提示需 replay） | 503 | 不写入、不自动重试 |

实现要点：

1. 同一 `(source, event_id)` 在进程内用**每键专用锁**串行化：并发重复请求中只有一个执行应用，其余在锁释放后读到账本并得到 `DUPLICATE`。
2. 目标写入本身是“**锁内存在性检查 + 条件写入**”（`put_record` 返回是否首次写入）。即使出现两个进程/崩溃恢复路径同时到达，也不可能产生第二条目标记录；对“写入成功但账本未及更新”的崩溃窗口，续跑时发现目标记录已存在，会确认成功并把事件标记为 `APPLIED`，仍不二次写入。
3. 幂等键是**来源内**的 `event_id`；不同来源允许使用相同的 `event_id`，彼此独立。
4. 用被篡改的 payload 重复投递同一 `event_id` 不会覆盖既有结果。

## 检查点协议

- 位点按来源独立保存于 `<data>/<source>/checkpoint.json`，初始为 `0`。
- 事件到达 `APPLIED` 或 `QUARANTINED` 终态时，位点自动 `max(current, seq)` 单调推进。
  - 坏事件也推进位点，以避免同一条坏消息反复卡住后续事件（坏消息本身可在隔离区检索）。
- 显式提交：`POST /v1/checkpoints {"source","seq"}`。
  - `seq < current`：拒绝，`409 CHECKPOINT_ROLLBACK`，位点不变。
  - `seq == current`：幂等空操作，返回当前值。
  - `seq > current`：推进。
- 跨来源串用：事件体内 `source` 与 URL 来源不一致时返回 `400 SOURCE_MISMATCH`，两侧均不落库、不记账本。
- 重启后位点来自磁盘，回退提交依然被拒绝。

> 说明：本实现以“单调推进”为协议核心，不强制 seq 连续（允许跳号），
> 因此没有单独的 `CHECKPOINT_GAP` 拒绝分支；任意小于当前位点的提交统一按回退拒绝。

## 错误分类

### 400 直接拒绝（无法标识/无法路由，不进隔离区）

| reason / error | 触发条件 |
| --- | --- |
| `MISSING_EVENT_ID` | 缺少 `event_id` 或为空 |
| `MALFORMED_STRUCTURE` | 请求体不是合法 JSON、不是 JSON 对象、`seq` 非正整数等 |
| `TYPE_MISMATCH` | `event_id` 类型不是字符串 |
| `SOURCE_MISMATCH` | 体内 `source` 与 URL 来源不一致（跨来源串用） |

### 422 隔离（事件可标识，进入隔离区并推进位点）

`POST /v1/sources/{source}/events` 返回 `status=QUARANTINED`，并可通过 `GET /v1/quarantine` 检索：

| reason | 触发条件 |
| --- | --- |
| `MISSING_FIELD` | 缺少 `source` / `seq` / `payload` 等必填字段 |
| `TYPE_MISMATCH` | `seq` 不是整数（布尔值也不接受）、`payload` 不是对象等 |
| `MALFORMED_STRUCTURE` | 结构非法，如 `payload` 键不是字符串 |

被隔离事件重复投递返回 `DUPLICATE` 并回显原原因，隔离区不新增记录。

### 202 / 503 上游暂时失败：有界重试与终止

- 通过管理接口注入故障模拟目标端暂时不可用（**仅本地演示/测试用**）：
  `POST /v1/admin/faults/{source}/{event_id}?fail_times=N`，清除用 `DELETE /v1/admin/faults`。
- 每次投递/续跑只计一次尝试：失败返回 `202 RETRYING`，`attempts` 递增，账本与 payload 快照立即落盘。
- 达到上限（默认 5 次，可配置）返回 `503 DEAD_LETTER / RETRY_EXHAUSTED`，事件进入死信队列，之后重复投递维持终止态，不再自动重试。
- 故障解除后用 `POST /v1/dead-letters/{source}/{event_id}/replay` 显式重放，给予一轮全新尝试。
- 服务重启后未完成事件可重新投递，或调用 `POST /v1/sources/{source}/resume` 续跑到终态。

## 接口一览

| 方法 路径 | 说明 |
| --- | --- |
| `POST /v1/sources/{source}/events` | 投递事件（幂等应用 / 隔离 / 重试） |
| `POST /v1/checkpoints` | 显式提交位点（回退 409） |
| `GET  /v1/sources/{source}/checkpoint` | 查看位点 |
| `POST /v1/sources/{source}/resume` | 续跑该来源所有未完成事件 |
| `GET  /v1/sources/{source}/records` | 查看目标落库记录（验证只写一次） |
| `GET  /v1/sources/{source}/events` | 查看事件账本（状态/尝试次数） |
| `GET  /v1/quarantine?source=` | 隔离区 |
| `GET  /v1/dead-letters?source=` | 死信队列 |
| `POST /v1/dead-letters/{source}/{event_id}/replay` | 死信显式重放 |
| `POST /v1/admin/faults/{source}/{event_id}?fail_times=` | 注入暂时故障 |
| `DELETE /v1/admin/faults` | 清除全部故障 |

## 本地验证方法

### 1. curl 手工验证

```bash
uv run uvicorn main:app --port 8011 &
B=http://127.0.0.1:8011

# 首次 / 重复
curl -s -X POST $B/v1/sources/demo/events -H 'Content-Type: application/json' \
  -d '{"event_id":"c1","source":"demo","seq":1,"payload":{"n":1}}'
curl -s -X POST $B/v1/sources/demo/events -H 'Content-Type: application/json' \
  -d '{"event_id":"c1","source":"demo","seq":1,"payload":{"n":1}}'   # -> DUPLICATE

# 坏事件
curl -s -X POST $B/v1/sources/demo/events -H 'Content-Type: application/json' \
  -d '{"event_id":"c2","source":"demo","seq":2}'                      # -> QUARANTINED/MISSING_FIELD

# 位点回退
curl -s -X POST $B/v1/checkpoints -H 'Content-Type: application/json' \
  -d '{"source":"demo","seq":0}'                                      # -> 409 CHECKPOINT_ROLLBACK

# 故障 -> 重试 -> 续跑
curl -s -X POST "$B/v1/admin/faults/demo/c3?fail_times=2"
curl -s -X POST $B/v1/sources/demo/events -H 'Content-Type: application/json' \
  -d '{"event_id":"c3","source":"demo","seq":3,"payload":{}}'         # -> 202 RETRYING
curl -s -X POST $B/v1/sources/demo/resume                             # -> 续跑到 APPLIED

curl -s $B/v1/sources/demo/records    # count 应为事件个数（无重复落库）
```

### 2. 自动化测试

```bash
uv run pytest          # 23 个用例；live log 逐条打印事件标识/位点/判定/原因/尝试次数
```

覆盖场景：

- **重复投递**：首次 `APPLIED`，后续 `DUPLICATE`，目标库始终一条，payload 不被篡改覆盖。
- **并发重复**：20 线程同时投递同一事件，断言恰有 1 个 `APPLIED`、19 个 `DUPLICATE`、落库 1 条。
- **检查点回退 / 持平 / 跨来源隔离 / 跨来源事件独立**。
- **重启恢复**：用同一数据目录重建应用，已完成事件仍判 `DUPLICATE`，`RETRYING` 事件 resume 后成功且不重不丢。
- **畸形事件**：缺字段、类型错、非对象、非法 JSON 分别得到可区分原因；坏事件不阻塞同来源/他来源。
- **重试/死信/重放**：有界尝试、`RETRY_EXHAUSTED`、死信维持终止态、清故障后 replay 成功。

测试与服务日志统一格式，例如：

```
... | INFO | event=f1 source=shop seq=1 cp=0 verdict=RETRYING reason=- attempts=1 | 第 1/5 次尝试暂时失败...
... | INFO | event=f1 source=shop seq=1 cp=1 verdict=APPLIED reason=- attempts=3 | 续跑后应用成功
... | INFO | event=c2 source=demo seq=2 cp=2 verdict=QUARANTINED reason=MISSING_FIELD attempts=0 | 缺少必填字段 payload
```
