# Codex Remote C/S Protocol V1

## 1. 最终选型

Codex Remote V1 使用一条长期 WebSocket 连接承载全部 Client/Host 业务通信。

| 层 | V1 决策 |
|---|---|
| 接口定义 | Protocol Buffers IDL（proto3） |
| 连接 | embedded tsnet listener 上的 plain WebSocket |
| 编码 | ProtoJSON |
| WebSocket message | UTF-8 text frame |
| RPC 与事件 | 复用同一条连接 |
| HTTP 业务 API | 无 |
| Binary Protobuf | 不支持 |
| 消息压缩 | 不协商 `permessage-deflate` |

WebSocket subprotocol 固定为：

```http
Sec-WebSocket-Protocol: codex-remote.v1.protojson
```

正式业务端点：

```text
ws://<tsnet-node>/connect
```

Host 的正式 listener 固定来自 `tsnet.Server.Listen("tcp", ":80")`。V1 不使用 `ListenTLS`、WSS、Tailscale Serve/Funnel、系统 `tailscaled`、宿主机网卡 listener 或公网/LAN fallback；网络保护由 Tailnet/WireGuard 提供。

Host 可以另行提供 `/healthz`、`/readyz` 和静态客户端资源，但它们不属于业务协议。

## 2. 设计原则

V1 只保留四个可靠性机制：

1. Protobuf IDL 是两端类型的唯一真源；
2. 副作用 Request 可以使用同一 `request_id` 安全重试；
3. 每个 Codex 有独立、持久化的 `event_seq`；
4. approval 和 user-input request 是可恢复状态，不是瞬时通知。

协议不暴露 Codex app-server raw wire，也不处理 live TUI 同 session 双控。

## 3. Frame

每个 WebSocket text message 必须包含且只包含一个 `codex.remote.v1.Frame` 的 ProtoJSON：

```proto
message Frame {
  oneof payload {
    ClientHello client_hello = 1;
    ServerHello server_hello = 2;
    Request request = 3;
    Response response = 4;
    Event event = 5;
    Ping ping = 6;
    Pong pong = 7;
    Close close = 8;
  }
}
```

V1 不定义额外 wire sequence。WebSocket 已保证单连接内可靠、有序和无重复传输。

禁止：

- WebSocket binary message；
- 一个 message 中放多个 Frame；
- 一个 Frame 跨多个 messages；
- JSON Lines 套在单条 WebSocket message 内；
- 手写未在 `.proto` 中定义的业务对象。

ProtoJSON 采用官方映射：lowerCamelCase 字段名、64 位整数使用十进制字符串、enum 使用名称。两端必须使用生成类型和 Protobuf JSON codec，不手工拼接或解析正式 wire JSON。

## 4. 握手

WebSocket upgrade 后，客户端第一帧必须是：

```json
{
  "clientHello": {
    "clientId": "client_01J",
    "clientRunId": "client_run_01J",
    "protocolVersion": {"major": 1, "minor": 0},
    "clientName": "Codex Remote Web",
    "clientVersion": "0.1.0"
  }
}
```

Host 返回：

```json
{
  "serverHello": {
    "connectionId": "conn_01J",
    "hostId": "host_01J",
    "hostRunId": "host_run_01J",
    "protocolVersion": {"major": 1, "minor": 0},
    "hostVersion": "0.1.0",
    "hostStatus": "HOST_STATUS_READY",
    "heartbeatIntervalMs": "15000",
    "connectionTimeoutMs": "45000",
    "maxFrameBytes": "8388608"
  }
}
```

`ServerHello` 同时返回 runtime 和 capabilities，不再单独要求一次能力查询。

标识语义：

- `client_id`：稳定安装/设备 ID；
- `client_run_id`：本次客户端进程运行 ID；
- `host_run_id`：本次 Host 进程运行 ID；
- `connection_id`：本次 WebSocket 连接 ID。

这些 ID 用于协议关联和审计，不是安全身份。V1 Demo 依赖 Tailnet 访问边界。

V1 当前只精确接受 `protocol_version={major:1,minor:0}`；其他 major 或 minor 均以 `PROTOCOL_VERSION_UNSUPPORTED` 拒绝。V1.0 不实现多-minor 协商。

## 5. 心跳

正式协议只使用应用层 `Ping/Pong`，不要求第二套 WebSocket control-frame 心跳。

```proto
message Ping {
  uint64 nonce = 1;
  int64 sent_at_unix_ms = 2;
}

message Pong {
  uint64 nonce = 1;
  int64 ping_sent_at_unix_ms = 2;
  int64 pong_sent_at_unix_ms = 3;
}
```

默认规则：

- Host 在连接空闲 15 秒后发送 Ping；
- Client 立即返回相同 nonce 的 Pong；
- 双方收到任意合法 Frame 都刷新 `last_received_at`；
- 45 秒未收到任何合法 Frame，主动关闭连接；
- 客户端重连后通过 `WatchCodex(after_event_seq)` 恢复；
- 心跳不占用 `event_seq`，正常心跳只做采样审计。

手机进入后台导致连接被系统冻结或关闭是正常情况；恢复前台后重新连接，不依赖后台永久保活。

## 6. RPC

所有命令和查询使用异步 multiplexed Request/Response：

```proto
message Request {
  string request_id = 1;
  int64 sent_at_unix_ms = 2;
  int64 deadline_unix_ms = 3;
  oneof request { /* operation */ }
}

message Response {
  string request_id = 1;
  int64 responded_at_unix_ms = 2;
  oneof result { /* result or Error */ }
}
```

同一连接允许多个并发 Request，Response 可以乱序返回，通过 `request_id` 关联。

### 6.1 统一 operation ID

V1 不再区分 `request_id` 和 `command_id`。

`request_id` 必须全局唯一。对副作用操作，网络超时或重连重试时必须复用原 ID：

```text
Request(request_id=op_A)
  → Host 已执行，Response 丢失

reconnect
Request(request_id=op_A)
  → Host 返回第一次结果，不重复执行
```

Host 持久保存副作用 Request 的操作类型、目标和最终 Response。在对应 Codex/session 存在期间不清理这类 dedup record。相同 ID 用于不同操作时返回 `CONFLICT`。

查询 Request 不需要持久化去重；重试时可以复用或生成新 ID。

### 6.2 deadline

`deadline_unix_ms=0` 表示使用 Host 默认值。Deadline 到达只取消 RPC 等待或尚未开始的工作，不等于中断已运行的 Codex turn。中断必须调用 `InterruptTurn`。

业务错误通过 `Response.error` 返回，不关闭整个连接。

## 7. RPC 列表

| RPC | 类型 | 作用 |
|---|---|---|
| `GetHost` | 查询 | 刷新 Host、runtime 和 capabilities |
| `ListDirectories` | 查询 | 受控目录选择器 |
| `ListSessionCandidates` | 查询 | 规范化 cwd 并列出已有 sessions |
| `ListCodexes` | 查询 | 列出 Remote 管理的 Codex |
| `CreateCodex` | 副作用 | 必要时创建目录并启动新 session |
| `ImportSession` | 副作用 | 导入并继续已有 session |
| `WatchCodex` | 查询/订阅 | 原子建立观察并 reset 或 replay |
| `UnwatchCodex` | 查询/订阅 | 停止该连接上的观察 |
| `ListHistory` | 查询 | 分页读取已完成历史 |
| `StartTurn` | 副作用 | 发送用户输入并启动 turn |
| `InterruptTurn` | 副作用 | 中断活动 turn |
| `RespondApproval` | 副作用 | 响应 approval |
| `RespondUserInput` | 副作用 | 响应 Codex 用户输入请求 |

不存在独立的 `ResolveDirectory`：

- `ListSessionCandidates(cwd)` 负责规范化和检查已有目录；
- `CreateCodex(cwd, create_directory_if_missing=true)` 负责创建新目录。

客户端操作主键始终是 `codex_id`，不得使用 `thread_id` 绕过 Host registry。

`StartTurn.options.mode` 表示 Codex collaboration mode（例如 `default` 或 `plan`）；`StartTurn.options.reasoning_effort` 独立表示推理强度。Host 不得将 `mode` 当作 app-server `effort` 转发。

## 8. Watch、reset 与 replay

Watch 是 V1 唯一实时订阅机制。没有 Subscription ID、Ack、ResyncRequired 或独立 Snapshot RPC。

### 8.1 首次打开

客户端不传 `after_event_seq`：

```text
WatchCodex(codex_id)
```

Host 原子完成：

```text
register watcher
  → capture head_event_seq
  → build CurrentView at that boundary
  → send WatchCodexResponse(mode=RESET)
  → send events after head_event_seq
```

`CurrentView` 只包含：

- Codex 当前状态；
- 当前 active turn 和 items；
- pending approvals；
- pending user-input requests；
- `head_event_seq`。

### 8.2 重连

客户端持久保存已经应用的最高 `event_seq` 及产生它的 `ServerHello.host_run_id`：

```text
WatchCodex(codex_id, after_event_seq=1842, after_host_run_id="host_run_01J")
```

如果可以连续 replay：

```text
WatchCodexResponse {
  mode: RESUMED
  replay_from_event_seq: 1843
  head_event_seq: 1850
}
→ events 1843...1850
→ live events
```

如果 cursor 缺失、过期、超前或 Host 无法证明连续性，Host 不发送额外错误流程，而是直接返回：

```text
WatchCodexResponse {
  mode: RESET
  reset_view: CurrentView
  head_event_seq: 9521
}
→ live events from 9522
```

客户端原子替换当前实时状态，然后从新 head 继续。不存在需要额外处理的 `ResyncRequired` 状态机。

`WatchCodexResponse.reset_reason` 解释 reset 原因。Cursor 是 `(after_host_run_id, after_event_seq)` 对；携带 `after_event_seq` 时缺少 `after_host_run_id` 返回 `INVALID_REQUEST`。若 run ID 等于当前 `ServerHello.host_run_id`，Host 才按 replay 可用性选择 RESUMED 或 RESET；若不等，V1 必须返回 `WATCH_RESET_REASON_HOST_RESTARTED` 的 `RESET + CurrentView`，而不是跨运行承诺 event replay。计划重启只发生在无 active turn 时；V1 不做 active-turn 热恢复，也不盲重放状态为 `IN_PROGRESS` 或结果未知的副作用。

### 8.3 流控

Host 对每个连接使用有界发送队列。慢客户端导致队列超过上限时，Host 以 `SLOW_CONSUMER` 关闭连接；客户端使用本地已应用 cursor 重连。

V1 不发送 EventAck。Ack 对当前 Demo 不参与数据清理或业务正确性，保留只会增加状态。

## 9. History 与 CurrentView 分离

已完成的历史通过 `ListHistory` 分页读取；当前变化状态通过 `WatchCodex` 获取。

```text
WatchCodex
  → 立即显示当前状态、active turn、pending request

ListHistory
  → 按需加载 completed turns
```

这避免了大型历史参与一致性 Snapshot 分页。历史页面使用不透明 `page_token`，按 `turn_id` 和 `item_id` 去重。单个超大 command output、diff 或文本必须截断并显式标记，不能生成超过 Frame 上限的响应。

## 10. Event

每个 Codex 独立维护持久化 `event_seq`：

- 严格单调递增；
- 跨连接和 Host 重启保持连续；
- `(codex_id, event_seq)` 唯一标识一个客户端事件；
- 不等于 Host `audit_seq`。

V1 定义且仅定义八类事件：

```proto
message Event {
  string codex_id = 1;
  uint64 event_seq = 2;
  int64 occurred_at_unix_ms = 3;
  string caused_by_request_id = 4;
  Completeness completeness = 5;

  oneof event {
    CodexUpdated codex_updated = 10;
    TurnUpdated turn_updated = 11;
    ItemStarted item_started = 20;
    ItemDelta item_delta = 21;
    ItemUpdated item_updated = 22;
    ItemCompleted item_completed = 23;
    PendingRequestUpdated pending_request_updated = 30;
    WarningRaised warning_raised = 40;
  }
}
```

`Event.completeness` 只描述当前 live Event payload 是否因 Frame 预算被裁剪或因来源缺口而不完整；它不替代嵌套 Item、user-input 等模型自己的完整性，也不能用后续 RESET 的 `CurrentView.completeness` 代替。Host 一旦裁剪 Warning、Approval、UserInput、大 item 或其他 Event 字段，必须在该 Event 自身标记 `truncated=true`、`incomplete=true` 并填写原因。

具体内容类型放在 `Item.content` 中：user message、agent message、reasoning summary、plan、command、tool、file change。

文本和 command output 使用 `ItemDelta`；plan、diff 或其他结构化中间状态使用完整 `ItemUpdated` upsert。客户端按 `item_id` 应用幂等 upsert，按 `chunk_seq` 检测单 item delta 缺口。

V1.0 不发送 IDL 之外的 event/enum；因此不需要 `semantic_type` 和 `state_critical` 兼容字段。

## 11. Approval 与 user input

Approval 和 user-input request 是 `PendingRequest` 状态：

- 创建或状态变化通过 `PendingRequestUpdated` 推送；
- Watch reset 时包含所有 pending requests；
- 客户端离线不会导致请求永久丢失；
- 响应采用 compare-and-set；
- 同一个 `request_id` 重试返回原结果；
- 不同请求对已解决 approval 再操作时返回 `APPROVAL_ALREADY_RESOLVED`。

`Approval.resolved_decision` 保存最终 `ALLOW`、`ALLOW_FOR_SESSION` 或 `DENY`，避免仅凭状态丢失决策语义。User-input 使用 `UserInputQuestion`/`UserInputOption`/`UserInputAnswer`：答案通过稳定 `question_id` 关联，并显式携带 option IDs 或 free-form text，不把结构化问题压成单个 prompt 字符串。

任意可能因 Frame 上限、导入能力或数据缺口而不完整的模型使用通用 `Completeness`，其中 `truncated` 表示因大小裁剪，`incomplete` 表示来源或集合不完整。rename 必须同时填写 `old_path` 和 `new_path`；`path` 保留为显示/兼容主路径。

## 12. Close 与重连

| CloseCode | WebSocket code | 重连 |
|---|---:|---|
| `NORMAL` | 1000 | 否 |
| `SERVER_SHUTDOWN` | 1012 | 是 |
| `PROTOCOL_VERSION_UNSUPPORTED` | 1002 | 升级后 |
| `HELLO_REQUIRED` | 1002 | 修复客户端 |
| `INVALID_FRAME` | 1002 | 修复客户端 |
| `FRAME_TOO_LARGE` | 1009 | 条件性 |
| `CONNECTION_TIMEOUT` | 4000 | 是 |
| `SLOW_CONSUMER` | 4001 | 是 |
| `INTERNAL_PROTOCOL_ERROR` | 1011 | 是 |

重连退避建议：

```text
0.5s → 1s → 2s → 4s → 8s → 15s → 30s
```

每次增加 ±20% jitter；收到 `retry_after_ms` 时不得提前重试。

## 13. 可读审计要求

可读、可对账审计是协议正确性的一部分。Client 和 Host 都必须记录 C/S wire 与本地处理阶段，详细规范见 `codex_remote_audit_v1.md`。

关键要求：

- 审计格式为一行一个 `AuditRecord` 的 ProtoJSON JSONL；
- 有效 Frame 同时保存解析后的 `frame` 和精确 `raw_text`；
- Client 记录 UI action、发送前、接收后和 state apply；
- Host 记录接收后、RPC 处理、事件持久化和发送前；
- 两端用 `connection_id`、`request_id` 或 `(codex_id,event_seq)` 对账；
- 日志中的 `local_seq` 只表示本地观察顺序，不进入正式 wire；
- 心跳可以采样，但连接建立、超时、关闭必须记录；
- Client 和 Host 各自生成稳定 `process_run_id`，进程重启后变化。

因此出现问题时可以判断：

```text
Client UI action 有，wire_out 无       → Client 发送前问题
Client wire_out 有，Host wire_in 无    → 网络/连接问题
Host wire_in 有，RPC result 无         → Host 处理问题
Host wire_out 有，Client wire_in 无    → 网络/连接问题
Client wire_in 有，state_apply 无      → Client 状态处理问题
Host canonical event 本身错误          → Host Adapter/状态转换问题
app-server raw 正确但 canonical 错误   → CodexAdapter 问题
```

## 14. Schema 演进

IDL package 固定为：

```proto
package codex.remote.v1;
```

当前实现冻结为精确 V1.0。在发布 V1.0 后，兼容增加字段或另行设计 minor 演进都必须先形成新的明确决策；当前 Host 不预先实现协商逻辑。禁止修改既有字段编号、复用删除字段、改变既有字段含义或引用 Codex app-server schema。删除字段时必须 `reserved`。

不同语言的 ProtoJSON runtime 对未知 enum 名称处理不完全一致，因此 V1.0 Host 必须拒绝其他版本，不能依赖客户端自动忽略新 enum。

## 15. V1 验收标准

1. 所有业务消息都是 `codex-remote.v1.protojson` WebSocket text frame；
2. Frame 只有 Hello、Request/Response、Event、Ping/Pong、Close；
3. 并发 Request 可乱序返回；
4. 副作用 Request 在丢响应和 Host 重启后仍不会重复执行；
5. Watch 能原子 reset 或连续 replay；
6. 慢客户端不会造成 Host 内存无界增长；
7. approval 在客户端离线期间不会丢失；
8. 历史分页不阻塞实时状态建立；
9. 心跳能发现连接或 Gateway 失活；
10. Client 与 Host 的审计可以对同一个 Request/Event 自动对账；
11. 人可以直接用文本工具阅读审计 JSONL；
12. 客户端完全不依赖 Codex app-server raw wire。
