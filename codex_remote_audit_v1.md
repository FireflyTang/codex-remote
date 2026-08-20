# Codex Remote Audit V1

## 1. 目标

Audit V1 用于回答：

> 某个用户动作、请求、响应或事件在哪一侧、哪个处理阶段丢失、出错或被错误解释？

审计首先服务于故障界定和复现：

- 人可以直接用文本编辑器、`rg`、`jq` 阅读；
- 程序可以按稳定 schema 解析、校验和合并；
- Client 与 Host 可以对同一条 C/S 消息自动对账；
- Host 内部可以继续追到 canonical activity、CodexAdapter 和 app-server raw wire；
- append-only，不通过修改旧记录“修正历史”。

审计不追求漂亮展示。V1 的正式格式是一行一个 ProtoJSON 对象的 JSONL。

这是个人 Demo 的开发与用户排错记录，不是防篡改或合规审计系统。V1 不要求 hash chain、法规保留、逐条强制 `fsync`，也不要求从 JSONL 重建 SQLite。客户端不在本仓；`audit.proto` 保留双端共同格式，Host 实现只对 Host 侧证据负责。

## 2. 双端审计

仅记录 Host 日志无法判断以下两种情况：

```text
Client 根本没有发出
vs
Client 发出但 Host 没收到
```

也无法判断：

```text
Host 没有发送事件
vs
Client 收到但没有应用到 UI state
```

因此 V1 要求：

```text
ClientAudit                         HostAudit

UI action                          C/S wire received
  ↓                                RPC started/completed
C/S wire before send        ↔      Host action
  ↓                                canonical activity
C/S wire received           ↔      C/S wire before send
  ↓
frame decoded
  ↓
state apply started/completed
  ↓
UI render observation（可选）
```

Host 继续记录：

```text
Host action
  → app-server raw request
  → app-server raw response/event
  → canonical activity
  → Remote event commit
  → C/S wire send
```

## 3. 格式

`audit.proto` 中的 `AuditRecord` 是唯一结构定义。每条记录编码为单行 ProtoJSON：

```text
<AuditRecord JSON>\n
<AuditRecord JSON>\n
...
```

要求：

- UTF-8；
- 一条记录只占一行；
- 不缩进；
- 字符串内换行按 JSON 转义；
- 64 位整数遵循 ProtoJSON，以十进制字符串编码；
- 文件只能追加；
- 文件轮转后旧文件不可修改；
- 进程崩溃留下的不完整末行可以丢弃，其他完整行仍有效。

## 4. AuditRecord

主要字段：

| 字段 | 含义 |
|---|---|
| `formatVersion` | 审计格式版本，V1 固定为 1 |
| `recordId` | 全局唯一记录 ID |
| `localSeq` | 当前 process run 内严格递增的本地顺序 |
| `observedAtUnixMs` | 本地观察时间 |
| `side` | `CLIENT` 或 `HOST` |
| `processRunId` | 客户端或 Host 本次进程运行 ID |
| `kind` | wire、RPC、state apply、Host action 等 |
| `direction` | inbound、outbound 或 internal |
| `outcome` | started、succeeded、failed、dropped、ignored |
| `component` | 产生记录的组件 |
| `operation` | 稳定操作名 |
| `message` | 简短可读描述，不用于机器判断 |
| `connectionId` | WebSocket 连接 ID |
| `requestId` | RPC/副作用 operation ID |
| `codexId` | Remote Codex ID |
| `eventSeq` | per-Codex C/S event sequence |
| `threadId` | Codex thread ID，仅 Host 侧通常可得 |
| `turnId` / `itemId` | 细粒度关联 |
| `parentRecordId` | 本地 started/completed 或父子操作关联 |
| `frame` | 已成功解码的 C/S Frame |
| `rawText` | 实际收到或将发送的精确 UTF-8 wire 文本 |
| `rawSha256` | `rawText` 原始字节摘要 |
| `rawSizeBytes` | 原始字节数 |
| `rawTruncated` | raw 是否因异常尺寸被截断 |
| `metadata` | 少量开放诊断字段 |

`local_seq` 不是协议字段，不在网络上传输。Client 与 Host 的 local sequence 也不能相互比较。

## 5. Wire 记录

一条有效 C/S wire 记录同时保存：

1. 精确的 `raw_text`；
2. 解码后的结构化 `frame`；
3. 从 Frame 提取到顶层的关联字段。

这种重复是刻意的：

- `raw_text` 证明实际 wire 内容，便于定位编码问题；
- `frame` 便于人阅读和程序查询；
- 顶层 ID 便于不遍历 payload 直接建立索引。

写入器必须校验顶层提取字段与 `frame` 内的值一致；不一致属于审计写入器 bug。读取器应校验 `raw_size_bytes`、`raw_sha256`，并可重新解析 `raw_text` 与 `frame` 做语义对比。

示例（`rawText` 为排版而省略，真实记录必须完整或显式标记 truncated）：

```json
{"formatVersion":1,"recordId":"audit_01J","localSeq":"182","observedAtUnixMs":"1786948611123","side":"AUDIT_SIDE_CLIENT","processRunId":"client_run_01J","kind":"AUDIT_KIND_CS_WIRE_FRAME","direction":"AUDIT_DIRECTION_OUTBOUND","outcome":"AUDIT_OUTCOME_STARTED","component":"transport","operation":"request.start_turn","connectionId":"conn_01J","clientId":"client_01J","clientRunId":"client_run_01J","requestId":"op_01J","codexId":"cdx_01J","frame":{"request":{"requestId":"op_01J","startTurn":{"codexId":"cdx_01J","input":[{"text":{"text":"Run the tests"}}]}}},"rawText":"{\"request\":{\"requestId\":\"op_01J\",...}}","rawSha256":"4f...","rawSizeBytes":"168"}
```

无效 frame 无法填充 `frame`，但仍记录受大小限制的 `raw_text`、hash、解析错误和 `INVALID_FRAME` 关闭动作。

## 6. 发送与接收边界

“准备发送”与“发送成功”不是一回事。append-only 审计不能回写原记录，因此使用两条关联记录：

```text
wire outbound STARTED    record_id=A
  ↓ WebSocket write
wire outbound SUCCEEDED  parent_record_id=A
```

失败则：

```text
wire outbound FAILED     parent_record_id=A
```

定义：

- `STARTED`：raw frame 已构造并进入发送调用前；该记录保存完整 `raw_text` 和 `frame`；
- `SUCCEEDED`：WebSocket library 已接受完整 message write；完成记录可以只保留关联 ID 和 `parent_record_id`，不重复大型 payload；
- `FAILED`：write 返回错误；
- `DROPPED`：因连接关闭、队列上限或策略未尝试 write。

`SUCCEEDED` 不证明远端已收到；必须与远端 inbound 记录对账。

入站同样分两个边界：

```text
wire inbound STARTED    # 保存 raw_text，尚未相信内容
  → decode/validate
wire inbound SUCCEEDED  # 保存 decoded frame，parent 指向 STARTED
  → business handling
```

解析失败时追加 `wire inbound FAILED`，保留错误和父记录。有效 frame 在业务处理前进入审计队列。副作用 Request 在 Host 的 inbound STARTED 与 SUCCEEDED 记录成功追加前不得执行。

## 7. Client 必须记录的阶段

### 7.1 UI action

用户触发会产生网络或本地状态变化的操作时记录：

```text
operation = ui.start_turn
operation = ui.interrupt_turn
operation = ui.respond_approval
operation = ui.open_codex
operation = ui.retry_connection
```

UI action 应在生成 Request 前记录，并在后续 wire record 中通过 `parent_record_id` 或 `request_id` 关联。

不要记录每次鼠标移动、滚动或纯视觉交互。

### 7.2 C/S wire

- outbound STARTED/SUCCEEDED/FAILED/DROPPED；
- inbound SUCCEEDED；
- decode/validation failure；
- connection open/hello/timeout/close。

### 7.3 Client state apply

对于 Response、Watch reset 和 Event，记录：

```text
CLIENT_STATE_APPLY STARTED
CLIENT_STATE_APPLY SUCCEEDED
```

或：

```text
CLIENT_STATE_APPLY FAILED
```

Event state apply 顶层必须携带 `codex_id` 和 `event_seq`。只有 `SUCCEEDED` 后，客户端才可以推进本地 applied cursor。

如果收到重复 event 并正确忽略，记录 `IGNORED`，说明原因 `duplicate_event_seq`。

## 8. Host 必须记录的阶段

### 8.1 C/S wire

- inbound Request 在业务处理前记录；
- outbound Response/Event 在 write 前后记录；
- hello、心跳异常、timeout、slow consumer 和 close；
- frame validation failure。

### 8.2 RPC

每个 Request 记录：

```text
RPC STARTED
RPC SUCCEEDED / FAILED
```

重复副作用请求额外记录：

```text
RPC SUCCEEDED
metadata.deduplicated = true
```

### 8.3 Host action

记录目录创建、session 创建/导入/resume、turn start/interrupt、approval 响应、runtime start/restart/stop。

### 8.4 Adapter 与 app-server

- 每条 app-server raw request/response/event；
- Adapter 翻译出的 canonical activity；
- imported history 的 provenance；
- canonical event 的 `event_seq` 分配与持久化；
- app-server stderr 和 Host application logs。

## 9. 对账键

跨 Client/Host 不使用时间戳作为主要关联。稳定关联键为：

| 场景 | 对账键 |
|---|---|
| WebSocket 连接 | `connection_id` |
| Request/Response | `request_id` |
| Event | `(codex_id, event_seq)` |
| Ping/Pong | `(connection_id, nonce)`，nonce 从 frame 读取 |
| Turn | `codex_id + turn_id` |
| Item | `codex_id + turn_id + item_id` |
| Host ↔ app-server | Host request ID / thread/turn/item ID |

两端时钟可能不同。`observed_at_unix_ms` 用于辅助排序和计算近似延迟，不能单独证明因果关系。

## 10. 问题界定矩阵

| 观察 | 初步界定 |
|---|---|
| Client UI action 有，Client outbound STARTED 无 | Client action/request 构造 |
| Client outbound SUCCEEDED 有，Host inbound 无 | 网络、旧连接或 Host 接收边界 |
| Host inbound 有，RPC STARTED 无 | Host dispatch/validation |
| RPC STARTED 有，无终态 | Host handler 卡死或崩溃 |
| Host outbound SUCCEEDED 有，Client inbound 无 | 网络、连接切换或 Client transport |
| Client inbound 有，state apply STARTED 无 | Client decode/routing |
| state apply STARTED 有，无 SUCCEEDED/FAILED | Client reducer 卡死或进程退出 |
| state apply FAILED | Client state reducer |
| app-server raw 正确，canonical activity 错误 | CodexAdapter translator |
| canonical activity 正确，Remote Event 错误 | RemoteEventStore/Gateway mapping |
| Event 正确，Client state apply 后 UI 错误 | Client state/UI |

这只是自动诊断的初步归因，不替代进一步查看上下文。

## 11. 文件布局

Host：

```text
~/.codex-remote/audit/
├── manifest.json
├── cs-wire/
│   └── 2026-08-17.jsonl
├── rpc/
│   └── 2026-08-17.jsonl
├── activities/
│   └── 2026-08-17.jsonl
├── host-actions/
│   └── 2026-08-17.jsonl
├── app-server-wire/
│   └── 2026-08-17.jsonl
├── runtime/
│   └── app-server-2026-08-17.jsonl
└── host/
    └── 2026-08-17.jsonl
```

app-server stderr 也包装为 `AUDIT_KIND_RUNTIME_LOG` AuditRecord，原始行或 chunk 放在 `raw_text`。这样不会因为纯文本 `.log` 破坏机器统一解析能力。

Client native：

```text
<client-data>/audit/
├── manifest.json
├── cs-wire/
├── ui-actions/
├── state-apply/
└── client/
```

浏览器客户端使用 IndexedDB 追加相同 `AuditRecord`，并提供导出为相同目录结构/JSONL 的诊断包功能。不能只依赖 DevTools console。

## 12. Manifest 与诊断包

每侧 `manifest.json` 至少包含：

```json
{
  "auditFormatVersion": 1,
  "side": "client",
  "processRunId": "client_run_01J",
  "appVersion": "0.1.0",
  "protocolVersion": "1.0",
  "protocolSchemaSha256": "...",
  "startedAt": "2026-08-17T10:00:00.000Z",
  "timezone": "Asia/Shanghai",
  "platform": "web"
}
```

Client 和 Host 可以分别导出诊断包。合并工具按以下步骤工作：

1. 校验 manifest 和 schema hash；
2. 校验每个文件中的 `local_seq` 单调性；
3. 按 connection/request/event 对账；
4. 标记只有发送侧、只有接收侧或缺少 state apply 的记录；
5. 输出时间线和初步问题边界。

## 13. 写入顺序与耐久性

每个 process run 使用单调 `local_seq` 和单写入器，避免多 goroutine/thread 直接交错写 JSONL。

Host 逻辑写入顺序：

```text
allocate local_seq
  → serialize one complete line
  → append or enqueue into the bounded writer
  → dispatch
```

耐久策略：

- side-effect Request inbound、Host action、approval、process lifecycle：必须进入有界诊断队列并保留关联 ID，不要求每条强制同步到稳定存储后才继续；
- 高频 delta/app-server streaming：允许短间隔批量 flush；
- 正常退出：尽力 flush；
- 写入失败：Host 进入 `DEGRADED`，发出 `AUDIT_DEGRADED` warning；
- 审计队列有界，满时不能静默丢弃；必须产生 dropped counter/record 和 incomplete 状态。是否背压由实现决定，不要求合规式阻断全部业务。

Client 至少保证 UI action、outbound side-effect Request、inbound Response/Event 和 state-apply 结果进入本地持久队列。浏览器存储不可用时，UI 必须显示 audit degraded 状态。

## 14. 轮转、保留与完整性

- 文件可按日期或大小轮转；
- `raw_sha256` 基于实际 UTF-8 wire bytes；
- SQLite 保存 Remote 元数据和检索索引；不要求从 JSONL 重建 SQLite；
- Host 尽可能保留完整 Codex/app-server 诊断记录，受显式容量、轮转和 truncation 限制；
- Client 日志保留策略可以受设备容量限制，但清理必须显式记录范围和时间，不能表现为从未发生；
- 导出诊断包时包含文件列表、大小和 SHA-256。

## 15. 可读性约定

为方便直接排查：

- `component`、`operation` 使用稳定短字符串；
- `message` 使用简短自然语言，不把关键信息只放在 message；
- ID 同时放在顶层，不要求人工钻进 `frame` 查找；
- 正常记录保留完整解析 `frame`；
- 大型 raw 只允许在超过安全上限或无效攻击性 frame 时截断，并保留 hash/size；
- 不把多行堆栈直接放进 `message`，堆栈放 metadata 或独立应用日志；
- 不将 ANSI 颜色或终端控制字符写入 JSONL。

常见人工查询：

```bash
rg '"requestId":"op_01J"' audit/
rg '"eventSeq":"1843"' audit/
jq -c 'select(.outcome == "AUDIT_OUTCOME_FAILED")' *.jsonl
jq -c 'select(.kind == "AUDIT_KIND_CLIENT_STATE_APPLY")' *.jsonl
```

## 16. V1 验收标准

1. Client 和 Host 使用同一 `audit.proto` 生成审计类型；
2. 任意 Request 可以从 Client UI action 追到 Host Response；
3. 任意 Event 可以从 Host canonical activity 追到 Client state apply；
4. 有效 C/S frame 同时保留 raw text 和 decoded frame；
5. 进程崩溃后的 JSONL 除最后不完整行外仍可解析；
6. 审计写入失败会显式 degraded，不会静默；
7. 浏览器客户端可以导出持久化审计，而非只查看 console；
8. 合并工具能自动列出 client-only、host-only 和 apply-missing 消息；
9. 人能用普通文本工具定位 request、event、turn 和 item；
10. app-server raw、canonical activity、Remote Event 和 Client apply 可以形成完整因果链。
