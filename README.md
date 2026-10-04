# 增量数据摄取服务（本地模拟版）

用本地文件存储模拟“源端推送 → 幂等摄取 → 目标端落库”的增量链路，不依赖任何外部服务或真实凭据。
服务基于 FastAPI，可直接本地启动，并通过 HTTP 接口 / pytest 验证以下性质：

- 重复投递、并发投递下，同一事件**只生效一次**，重复请求返回可区分的 `DUPLICATE`，不二次写入、不改既有结果。
- **批量投递**：一次请求提交同一来源的一批事件，响应逐条给出判定与原因；个别重复/畸形/暂时失败不连累同批其他事件，整批不整体失败；批内重复标识最终只生效一条。
- **多进程重叠批次**：多个 OS 进程同时对同一来源投递有重叠的批次，仍保证每条事件只生效一次、重复结论可区分、位点单调不回退不串来源；单个大批次不会卡住其他来源。
- **来源级进度对账**：按来源查看当前位点、已生效/隔离/死信/未处理条数，数字与账本和落库一致，并发写入期间读取也是自洽快照。
- 每个来源的检查点**只能单调推进**；回退返回 `409 CHECKPOINT_ROLLBACK`，跨来源串用返回 `400 SOURCE_MISMATCH`。
- 服务重启或重复消费后**不丢事件、不重复落库**；未完成事件可重投或 `/resume` 续跑。
- 字段缺失 / 类型不匹配 / 结构非法的坏事件按可区分原因**拒绝或隔离**，并可通过故障注入演示**有界重试 → 死信 → 显式重放**。
- 单条坏事件通过推进位点避免阻塞同来源后续事件；来源之间文件与锁完全隔离，互不阻塞。

> **兼容性**：批量投递与进度对账是在原有能力上的**增量扩展**。旧的单条接口
> `POST /v1/sources/{source}/events` 及其请求/响应、状态码、判定规则**完全保持不变**；
> 老客户端无需任何改动。批量只是把同一个单条处理逻辑对数组逐条套用，两条路径共用同一套
> 幂等账本、位点与锁，因此单条与批量混用也不会重复生效。

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

## 批量投递

`POST /v1/sources/{source}/events/batch`，请求体：

```json
{
  "events": [
    {"event_id": "b1", "source": "shop", "seq": 1, "payload": {"n": 1}},
    {"event_id": "b2", "source": "shop", "seq": 2}
  ]
}
```

规则：

- 一批事件必须同属一个来源（URL 中的 `source`）；逐条事件的字段要求与单条接口完全一致。
- **只要请求信封合法**（是含 `events` 数组的 JSON 对象），HTTP **恒为 200**，即使里面有坏事件/重复/暂时失败也不会让整批失败；每条事件的结论看响应体 `results`，其 `index` 与请求数组下标一一对应。
- 只有**请求信封本身**非法（不是合法 JSON、没有 `events`、`events` 不是数组）才整体返回 `400 BATCH_MALFORMED`。
- 每条事件独立判定，状态取值与单条一致：`APPLIED` / `DUPLICATE` / `QUARANTINED` / `RETRYING` / `DEAD_LETTER`；另外批量中无法标识或跨来源的单条会得到 `REJECTED`（带可区分 `reason`），**只拒绝该条，不连累其他条**。
- **同一批里出现重复 `event_id`**：只有第一条真正 `APPLIED`，其余为 `DUPLICATE`，最终落库只有一条。
- 批量与单条、批量与批量之间共用同一套账本和锁，**混用也不会重复生效**。

响应结构：

```json
{
  "source": "shop",
  "summary": {"received": 2, "applied": 1, "duplicate": 0, "quarantined": 1,
              "retrying": 0, "dead_letter": 0, "rejected": 0},
  "results": [
    {"index": 0, "event_id": "b1", "source": "shop", "seq": 1, "status": "APPLIED", ...},
    {"index": 1, "event_id": "b2", "source": "shop", "seq": 2,
     "status": "QUARANTINED", "reason": "MISSING_FIELD", ...}
  ]
}
```

批量中单条 `REJECTED` 的 `reason`：

| reason | 触发条件 |
| --- | --- |
| `MISSING_EVENT_ID` | 该位置事件缺少/为空 `event_id` |
| `MALFORMED_STRUCTURE` | 该位置不是 JSON 对象等 |
| `TYPE_MISMATCH` | `event_id` 类型错等 |
| `SOURCE_MISMATCH` | 该条事件体内来源与 URL 来源不一致（跨来源串用，不落入任何一侧） |
| `PROCESSING_ERROR` | 单条处理意外错误的防御性兜底（正常不会出现） |

### 多进程并发下的重叠批次

服务按 `(source, event_id)` 做了**两层串行化**：进程内每键锁 + 跨进程文件锁
（`fcntl.flock`，锁文件位于 `<data>/<source>/_locks/`）。因此多个进程同时投递内容
重叠的批次时：

- 每条事件在所有进程里**恰好一个**得到 `APPLIED`，其余进程得到可区分的 `DUPLICATE`；
- 落库条数恒等于去重后的事件数，位点单调推进、不回退、不串来源；
- 乱序到达的批次（大 seq 先到、小 seq 后到）不会让位点倒退，也不会把已生效事件重写；
- 事件锁按来源分目录，处理某个来源的大批次**不会阻塞其他来源**。

## 来源级进度对账

`GET /v1/sources/{source}/progress` 返回该来源当前的对账快照：

```json
{
  "source": "shop",
  "last_seq": 12,
  "applied_count": 10,
  "duplicate_count": 7,
  "quarantined_count": 1,
  "dead_letter_count": 1,
  "pending_count": 0,
  "ledger_total": 12,
  "checked_at": "2026-10-04T04:00:00+00:00"
}
```

| 字段 | 含义 |
| --- | --- |
| `last_seq` | 当前位点（单调，只由 APPLIED/QUARANTINED 推进） |
| `applied_count` | 已生效条数，**等于目标库实际落库记录数** |
| `duplicate_count` | 累计重复投递请求次数（不新增落库，仅计数） |
| `quarantined_count` | 隔离条数，与隔离区该来源条数一致 |
| `dead_letter_count` | 死信条数，与死信队列该来源条数一致 |
| `pending_count` | 尚未处理完（`RETRYING`）的条数 |
| `ledger_total` | 账本总条数 = applied + quarantined + dead_letter + pending |

实现要点：`applied_count / quarantine / dead_letter / 账本 / 位点` 在 store 的**同一把
排他锁内一次性读出**并计数，因此即使在其他进程并发写入的同时读取，拿到的也是一个
内部自洽的快照（不会出现“账本说已生效却查不到落库记录”这类矛盾）。
`duplicate_count` 仅用于对账“上游重推了多少次”，不计入 `ledger_total`。

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
| `SOURCE_MISMATCH` | 体内 `source` 与 URL 来源不一致（跨来源串用）；批量中表现为该条 `REJECTED` |
| `BATCH_MALFORMED` | 批量请求不是合法 JSON、不是含 `events` 数组的对象、`events` 不是数组（整批 400） |

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
| `POST /v1/sources/{source}/events` | 投递单条事件（幂等应用 / 隔离 / 重试），旧接口保持不变 |
| `POST /v1/sources/{source}/events/batch` | **批量投递**同来源一批事件，恒 200，逐条给结论 |
| `GET  /v1/sources/{source}/progress` | **来源级进度对账**：位点/已生效/隔离/死信/未处理条数 |
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

# 批量投递：好事件 + 缺 payload 的坏事件 + 批内重复 + 缺 event_id + 跨来源，整批仍 200
curl -s -X POST $B/v1/sources/demo/events/batch -H 'Content-Type: application/json' -d '{
  "events": [
    {"event_id":"p1","source":"demo","seq":10,"payload":{"n":10}},
    {"event_id":"p2","source":"demo","seq":11},
    {"event_id":"p1","source":"demo","seq":10,"payload":{"n":10}},
    {"source":"demo","seq":12,"payload":{}},
    {"event_id":"p3","source":"crm","seq":13,"payload":{}}
  ]
}'
# -> summary: applied=1 duplicate=1 quarantined=1 rejected=2；results 逐条给出 index/reason

# 进度对账：数字与 records/quarantine/dead-letters 对得上
curl -s $B/v1/sources/demo/progress
```

### 2. 多进程并发（重叠批次）验证

真实多进程测试已内置（使用 `multiprocessing` 的 `spawn`，不是线程模拟）：

```bash
uv run pytest tests/test_batch_concurrency_multiproc.py -v
```

它会启动多个独立 OS 进程，各自构建独立的 service（各自独立的内存锁/故障表），
并发投递**同一批**事件，然后用一个全新“验证者”进程核对磁盘真相：每条事件恰好一个
`APPLIED`、其余全是可区分的 `DUPLICATE`、落库条数 == 去重事件数、位点单调不串来源，
并覆盖单条/批量路径混用、多来源同时投递互不串账。

也可以手工开两个服务进程（同一 `INGEST_DATA_DIR`）观察：

```bash
INGEST_DATA_DIR=./data-mp uv run uvicorn main:app --port 8011 &
INGEST_DATA_DIR=./data-mp uv run uvicorn main:app --port 8012 &
# 对 8011 / 8012 同时 POST 同一个 /v1/sources/shop/events/batch，再 GET progress 对账
```

### 3. 自动化测试

```bash
uv run pytest          # 全部用例；live log 逐条打印事件标识/位点/判定/原因/尝试次数
```

覆盖场景：

- **重复投递**：首次 `APPLIED`，后续 `DUPLICATE`，目标库始终一条，payload 不被篡改覆盖。
- **并发重复**：20 线程同时投递同一事件，断言恰有 1 个 `APPLIED`、19 个 `DUPLICATE`、落库 1 条。
- **批量投递**（`tests/test_batch.py`）：部分坏事件/部分重复/暂时失败不连累其他事件、整批不整体失败；批内重复标识只生效一条；乱序批次位点不倒退不重写；大批次不阻塞其他来源；信封非法才 400。
- **多进程重叠批次**（`tests/test_batch_concurrency_multiproc.py`，真实进程非线程）：重叠批次每条只生效一次、重复可区分、落库数==去重数、位点单调不串来源、单条/批量混用一致。
- **进度对账**（`tests/test_progress.py`）：applied/隔离/死信/未处理计数与账本、落库、各队列逐项对得上；重复计数；并发写入同时反复读取，快照始终自洽、单调不回退。
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
