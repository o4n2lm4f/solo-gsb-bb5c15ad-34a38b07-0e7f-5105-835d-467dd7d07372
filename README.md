# Local Webhook Relay（本地 Webhook 投递中继）

基于 **FastAPI + SQLite** 的本地 Webhook 投递中继服务：创建带签名密钥的订阅，提交事件后为每个订阅生成独立的持久化投递任务，由后台并发工作器可靠投递，失败按指数退避自动重试。

## 功能特性

- **订阅管理**：创建/查询/删除订阅（目标 URL + secret），支持持久化**暂停/恢复**（`POST /subscriptions/{id}/pause|resume`，幂等）
- **暂停语义**：暂停期间工作器不再认领该订阅的新投递任务（已在途的 `in_progress` 尝试照常完成），`/events`、`/ingest` 照常入库为 `pending`；恢复时**不批量改写** `next_attempt_at`，按原到期时间、原 `task_id` 严格顺序继续；状态写入 SQLite，重启保持
- **HMAC 受保护入站接口**：`POST /ingest` 使用 `INGEST_SECRET` 校验 `X-Webhook-Signature`（覆盖时间戳、ID 与原始 JSON），并强制 ±300 秒时间窗
- **入站幂等与防重放**：已接收的 `X-Webhook-Id` 持久化在 SQLite 中；重复 ID + 相同内容返回首次接收结果且不重复投递，相同 ID 不同内容返回 `409`
- **事件投递**：一次提交可指定多个订阅，每个订阅生成独立任务
- **按订阅严格有序**：同一订阅的任务按 `task_id` 升序（即提交顺序）依次投递——前一条任务处于 `pending`/`in_progress` 时后续任务不会被认领，前一条 `delivered` 或 `failed` 后才放行下一条；不同订阅之间互不影响，仍可由多个工作器并行投递
- **持久化与断点续投**：任务存储在 SQLite 中，服务重启后自动恢复未完成任务（包括崩溃时处于 `in_progress` 状态的任务），有序约束在重启后同样生效
- **并发安全**：多个工作器通过数据库级原子认领（SELECT + UPDATE 在同一临界区内）保证同一轮尝试不会被重复领取
- **可靠重试**：非 2xx、连接错误、超时均视为失败，按 **1s → 2s → 4s** 退避重试，最多 **4 次**尝试后标记为失败
- **幂等去重**：`(event_id, subscription_id)` 唯一约束，重复提交不会产生重复任务
- **HMAC-SHA256 签名**：每个投递请求携带签名头，接收方可校验来源与完整性
- **状态查询**：按事件查询各目标的尝试次数、下次执行时间、最终结果及完整尝试历史

## 快速开始

### Docker（推荐，单条命令）

```bash
docker compose up --build
```

服务启动后访问 **http://localhost:8000**（交互式 API 文档：http://localhost:8000/docs）。
SQLite 数据库保存在名为 `webhook-data` 的卷中，容器重启后任务不丢失。

`/ingest` 的密钥默认使用 compose 内置的开发值 `dev-ingest-secret-change-me`，生产环境请通过环境变量覆盖：

```bash
INGEST_SECRET="$(openssl rand -hex 32)" docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
export INGEST_SECRET="dev-ingest-secret-change-me"   # 启用 POST /ingest
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API 使用示例

### 1. 创建订阅

```bash
curl -s -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"target_url": "http://localhost:9001/hook", "secret": "my-secret"}'
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00"}
```

### 2. 提交入站事件（HMAC 保护，POST /ingest）

`/ingest` 面向外部事件源，使用共享密钥 `INGEST_SECRET` 做 HMAC-SHA256 认证。
每个请求必须携带三个头部：

| 头部 | 说明 |
|------|------|
| `X-Webhook-Timestamp` | 发送时刻 Unix 时间戳（秒），与服务端时差不得超过 **±300 秒** |
| `X-Webhook-Id` | 事件唯一 ID（最长 128 字符），用于幂等去重 |
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(secret, timestamp + "." + id + "." + 原始请求体)>` |

请求体为原始 JSON，**必须**包含 `subscription_ids`（非空数组，1–100 个），其余字段原样转发给投递目标：

```bash
SECRET="dev-ingest-secret-change-me"
TS=$(date +%s)
ID="evt-ingest-001"
BODY='{"subscription_ids": ["44a79d7077ce"], "type": "order.paid", "order_id": 42}'
SIG="sha256=$(printf '%s' "${TS}.${ID}.${BODY}" | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.*= //')"

curl -s -X POST http://localhost:8000/ingest \
  -H 'Content-Type: application/json' \
  -H "X-Webhook-Timestamp: $TS" \
  -H "X-Webhook-Id: $ID" \
  -H "X-Webhook-Signature: $SIG" \
  -d "$BODY"
```

Python 生成签名示例：

```python
import hashlib, hmac, time

secret = "dev-ingest-secret-change-me"
ts = str(int(time.time()))
webhook_id = "evt-ingest-001"
body = b'{"subscription_ids": ["44a79d7077ce"], "type": "order.paid"}'
signed = ts.encode() + b"." + webhook_id.encode() + b"." + body
sig = "sha256=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
```

验签与幂等行为：

- **首次验证成功**：`201 Created`，按 `subscription_ids` 创建投递任务，响应与 `POST /events` 相同（`created` / `duplicates`）。
- **重复 ID + 相同原始 body**：`200 OK`，返回与首次完全一致的接收结果，**不会重复创建投递任务**（含并发重试：多个相同请求同时到达也只有一个 `201`）。
- **相同 ID 但 body 不同**：`409 Conflict`，不产生任何任务。
- **签名错误**（含签名未覆盖时间戳/ID/原始 body）：`401 Unauthorized`。
- **时间戳超出 ±300 秒窗口**或无法解析：`401 Unauthorized`。
- **缺少必需头部**、空 body、body 非 JSON：`400 Bad Request`；body 缺少 `subscription_ids`：`422`。
- **包含不存在的订阅 ID**：`400 Bad Request`，且该事件不会持久化。
- 服务端未配置 `INGEST_SECRET` 时端点失败关闭，返回 `503`。

入站事件的 `X-Webhook-Id` 即事件 ID，可用 `GET /events/{id}` 查询投递状态；投递给订阅目标的出站请求体就是 `/ingest` 收到的**原始 JSON**（出站签名使用该订阅自己的 secret，见下文）。

### 3. 提交事件（内部接口 POST /events）

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
        "event_id": "evt-001",
        "subscription_ids": ["44a79d7077ce", "94d3c624dad2"],
        "payload": {"msg": "hello", "n": 1}
      }'
```

```json
{
  "event_id": "evt-001",
  "created": [
    {"task_id": 1, "subscription_id": "44a79d7077ce", "status": "pending"},
    {"task_id": 2, "subscription_id": "94d3c624dad2", "status": "pending"}
  ],
  "duplicates": []
}
```

- 重复提交相同 `event_id` + 订阅：返回 `200`，已存在的任务出现在 `duplicates` 中，不会重复投递
- 相同 `event_id` 但 payload 不同：返回 `409 Conflict`
- 包含不存在的订阅 ID：返回 `400`

### 4. 查询事件状态

```bash
curl -s http://localhost:8000/events/evt-001
```

```json
{
  "event_id": "evt-001",
  "created_at": "2026-09-17T11:11:24+00:00",
  "targets": [
    {
      "task_id": 2,
      "subscription_id": "94d3c624dad2",
      "target_url": "http://localhost:9002/hook",
      "status": "delivered",
      "attempts": 3,
      "max_attempts": 4,
      "next_attempt_at": null,
      "last_status_code": 200,
      "last_error": null,
      "result": "delivered",
      "history": [
        {"attempt_no": 1, "status_code": 500, "error": "non-2xx response: 500", ...},
        {"attempt_no": 2, "status_code": 500, "error": "non-2xx response: 500", ...},
        {"attempt_no": 3, "status_code": 200, "error": null, ...}
      ]
    }
  ]
}
```

`status` 取值：`pending`（等待执行）/ `in_progress`（投递中）/ `delivered`（成功）/ `failed`（最终失败）。
`next_attempt_at` 仅在 `pending` 时有值，即下次执行时间。

注意：若同一订阅存在更早的未完成任务，后续任务会因有序调度而保持 `pending`（被前序任务阻塞），状态查询会如实展示这些被阻塞的任务。

### 5. 暂停 / 恢复订阅

```bash
# 暂停：之后不再领取该订阅的新投递任务
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/pause

# 恢复：按原到期时间、原 task_id 顺序继续，不批量改写 next_attempt_at
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/resume
```

```json
{
  "id": "44a79d7077ce",
  "target_url": "http://localhost:9001/hook",
  "created_at": "2026-09-17T11:11:24+00:00",
  "paused": true,
  "paused_at": "2026-09-17T12:00:00+00:00"
}
```

行为约定：

- **暂停后不再领取新投递**：该订阅所有 `pending` 任务（含退避等待中的任务）都不会被任何工作器认领；
- **在途尝试不中断**：暂停时已经处于 `in_progress` 的投递允许正常完成（成功则 `delivered`，失败仍按原退避重试——重试的下一轮在恢复前不会被认领）；
- **入库不受影响**：暂停期间 `POST /events` 与 `POST /ingest` 正常接收，新任务照常持久化且状态为 `pending`，暂停不影响入站幂等/验签逻辑；
- **恢复不改时间戳**：恢复只清除暂停标记，不批量更新任何任务的 `next_attempt_at`——暂停期间已到期的任务恢复后立即被认领，未到期的继续等到原时间点；按订阅严格有序的 `task_id` 顺序约束在恢复后同样生效；
- **幂等**：对已暂停的订阅再次 `pause`、对活动订阅再次 `resume` 均为无操作并返回当前状态（`200`）；重复暂停不会刷新 `paused_at`；订阅不存在时两个端点均返回 `404`；
- **持久化**：`paused` 与 `paused_at` 写入 SQLite（`subscriptions` 表），服务或容器重启后保持；旧数据库首次启动时自动补列，缺省视为未暂停；
- `GET /subscriptions` 与 `GET /subscriptions/{id}` 的响应均包含 `paused`（布尔）与 `paused_at`（ISO 8601，未暂停时为 `null`）。

### 其他端点

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/ingest` | HMAC 保护的入站事件接口（时间窗 + 签名 + ID 幂等） |
| `POST` | `/events` | 内部提交事件（event_id + payload，无需签名） |
| `GET` | `/subscriptions` | 列出全部订阅（含 `paused` / `paused_at`） |
| `GET` | `/subscriptions/{id}` | 查询单个订阅（含 `paused` / `paused_at`） |
| `POST` | `/subscriptions/{id}/pause` | 暂停订阅（幂等；不存在返回 `404`） |
| `POST` | `/subscriptions/{id}/resume` | 恢复订阅（幂等；不重排到期时间；不存在返回 `404`） |
| `DELETE` | `/subscriptions/{id}` | 删除订阅（已有任务保留历史） |
| `GET` | `/healthz` | 健康检查 |

## 按订阅严格有序投递

同一 `subscription_id` 下的投递任务按 `tasks.id` 升序（即任务创建顺序）逐条投递，语义如下：

- 较早任务处于 `pending`（含退避等待重试中）或 `in_progress`（投递中）时，同订阅的后续任务**不会被任何工作器认领**；
- 较早任务到达终态——`delivered`（成功）或 `failed`（重试耗尽后最终失败）——才放行同订阅的下一条任务；
- 不同订阅之间没有顺序约束，`WORKER_COUNT` 个工作器可并行投递不同订阅的任务；
- 约束在认领时基于 SQLite 中的持久化状态判定，因此失败重试、服务重启（含崩溃恢复）后依然有效；
- 被阻塞的任务状态保持 `pending`，通过 `GET /events/{event_id}` 可正常查询到。

## 暂停 / 恢复的并发边界

- 暂停是**认领门闩（claim gate）**而非取消：原子认领 SQL 额外要求订阅 `paused = 0`。由于认领只选 `pending` 行，暂停时刻已处于 `in_progress` 的那一次尝试**不会被中断**，会照常走完 HTTP 请求并记录结果；
- 因此暂停后该订阅最多还有 **1 个在途投递**（严格有序保证同订阅不会有第二个 `in_progress`），其结果可能是 `delivered`、`failed`，或失败后退回 `pending` 等待退避——退回的任务在恢复前不会被再次认领；
- 暂停与任务提交完全解耦：`/events`、`/ingest` 不检查暂停状态，暂停期间事件正常入库、回执正常返回，任务一律落为 `pending`；`/ingest` 的验签、时间窗、ID 幂等语义均不受影响；
- 恢复不重放时钟：暂停期间积压任务的 `next_attempt_at` 保持原值，恢复后「已到期 → 下一轮轮询立即领取（0.2s 内）；未到期 → 等到原时间点」，且仍按 `task_id` 升序逐条放行；暂停时长不会被追加到退避上；
- 暂停/恢复本身在单个连接锁临界区内完成条件 UPDATE，与认领互斥，重复调用幂等；暂停状态持久化在 `subscriptions` 表，重启（含恢复流程把残留 `in_progress` 重置为 `pending`）后依旧生效。

## 接收端验签

每个投递请求携带以下头部：

| 头部 | 说明 |
|------|------|
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(secret, 请求体)>` 十六进制摘要 |
| `X-Webhook-Event-Id` | 事件 ID |
| `X-Webhook-Task-Id` | 投递任务 ID |
| `X-Webhook-Attempt` | 当前第几次尝试 |
| `X-Webhook-Timestamp` | 发送时间戳（秒） |

Python 验签示例：

```python
import hashlib, hmac

def is_valid(secret: str, body: bytes, signature_header: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature_header)
```

## 设计说明

- **任务模型**：事件存于 `events` 表（payload 只存一份），每个 `(event_id, subscription_id)` 对应 `tasks` 表中一行（唯一约束保证幂等）；每次尝试写入 `attempts` 历史表。
- **入站幂等**：每个验签通过的 `X-Webhook-Id` 在 `ingested_events` 表中记录一行（原始 body 字节 + 首次回执）。查重、冲突判定、建事件、建任务在同一个 SQLite 事务中完成；重放请求直接返回存储的首次回执，即使并发到达也只会有一个请求建任务。
- **原子认领**：工作器在单个连接锁保护的临界区内完成「选中最旧到期任务 → 置为 `in_progress`」，并发工作器不会领到同一任务。
- **暂停门闩**：`subscriptions.paused` 持久化在 SQLite；认领查询 `LEFT JOIN` 订阅并附加 `COALESCE(s.paused, 0) = 0`（`COALESCE` 兼容订阅删除后残留的任务）。暂停/恢复为带条件的原子 UPDATE（仅在实际发生状态翻转时写 `paused_at`），天然幂等；恢复不触碰任何 `tasks.next_attempt_at`，积压任务按原到期时间继续调度。旧库通过启动时的 `PRAGMA table_info` + `ALTER TABLE` 自动补列。
- **按订阅有序**：认领查询附带 `NOT EXISTS` 条件——同订阅存在 id 更小且状态为 `pending`/`in_progress` 的任务时，当前任务不可认领；只有前序任务进入终态（`delivered`/`failed`）才放行下一条。条件针对持久化状态在认领时求值，故重试退避、崩溃恢复（`in_progress` 重置为 `pending` 后依然阻塞后续任务）与多工作器并发下均成立；不同订阅互不影响，可并行投递。
- **崩溃恢复**：启动时将残留的 `in_progress` 任务重置为 `pending` 并立即到期；`pending` 任务按原 `next_attempt_at` 继续调度。
- **退避策略**：第 n 次失败后延迟 `2^(n-1)` 秒（1、2、4），第 4 次失败后任务终止，结果为 `failed`。
- **成功判定**：仅 HTTP 2xx 视为成功；其余状态码、连接错误、超时均触发重试。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `INGEST_SECRET` | *无（compose 内为 `dev-ingest-secret-change-me`）* | `POST /ingest` 的 HMAC 共享密钥；未配置时该端点返回 `503` |
| `INGEST_TIMESTAMP_TOLERANCE_SECONDS` | `300` | 入站时间戳允许的最大偏差（秒，正负两个方向） |
| `DB_PATH` | `./webhook_relay.db`（容器内 `/data/webhook_relay.db`） | SQLite 数据库路径 |
| `WORKER_COUNT` | `4` | 并发投递工作器数量 |
| `HTTP_TIMEOUT_SECONDS` | `5` | 单次投递 HTTP 超时 |
| `MAX_ATTEMPTS` | `4` | 单任务最大尝试次数 |
| `BACKOFF_BASE_SECONDS` | `1` | 首次重试延迟（之后每次翻倍） |
| `POLL_INTERVAL_SECONDS` | `0.2` | 空闲工作器轮询间隔 |

## 项目结构

```
app/
├── main.py      # FastAPI 应用：API 端点 + 生命周期（启动/停止工作器）
├── worker.py    # 投递工作器：认领任务、HTTP 投递、记录结果
├── db.py        # SQLite 持久层：建表、原子认领、状态更新
├── schemas.py   # Pydantic 请求/响应模型
├── signing.py   # HMAC-SHA256 签名与验签
└── config.py    # 环境变量配置
Dockerfile
docker-compose.yml
requirements.txt
```
