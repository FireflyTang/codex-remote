# Codex Remote Host Architecture V1

## 1. 文档目的

本文定义 Codex Remote V1 Demo 的 Linux Host 内部架构。重点是 Codex app-server 的生命周期、目录与 session 管理、协议适配、活动分发和全量审计。

客户端—Host 通信采用 WebSocket + ProtoJSON，由独立的 Protobuf IDL 定义，详见 `codex_remote_cs_protocol_v1.md`。Gateway 负责连接、RPC multiplexing、Watch、心跳和断线恢复，但不得把 Codex app-server wire 暴露给客户端。Client/Host 可对账审计见 `codex_remote_audit_v1.md`。Host 使用 embedded `tsnet` 加入公共 Tailscale，详见 `codex_remote_host_tailnet_v1.md`。

## 2. 设计约束

- Host 仅支持 Linux；
- Host 使用 Go 实现，以直接使用官方 `tailscale.com/tsnet`；
- Host 内嵌独立 Tailscale node，不依赖系统 `tailscaled`、系统 VPN 或 root；
- 使用公共 Tailscale control plane，不部署 Headscale；
- 只提供 Tailnet listener，不使用 Funnel、公网监听或 Codex Remote 自建 relay；
- Host 是可直接运行的常驻进程，可以放在 tmux 中运行；
- V1 不依赖 systemd，也不要求注册系统服务；
- Host 管理一个长期运行的 `codex app-server`；
- 一个 app-server 承载多个持久化 thread；
- Host 与 app-server 优先使用 Unix domain socket；
- Host 自己负责 app-server 的启动、就绪检测、监控、重启和退出处理；
- 支持发现、导入和继续开发机上已有的 Codex sessions；
- 运行记录必须可完整审计和排查；
- Demo 暂不设计多租户安全和大规模用户能力；
- 不保证与仍在运行的本地 Codex TUI 对同一 live session 实时双控。

## 3. 总体架构

```text
                 Public Tailscale Control Plane
                    node auth / coordination
                       ┌────┴────┐
                       ▼         ▼
                Client node   embedded tsnet.Server
                       └────┬────┘
                  encrypted Tailnet connection
                            │ Listen("tcp", ":80")
┌───────────────────────────▼───────────────────────────┐
│                 codex-remote-host                     │
│                                                       │
│  TailnetService → HTTP/WebSocket Gateway              │
│                 WebSocket + ProtoJSON V1              │
│                         │                             │
│  CodexManager                                         │
│    ├── DirectoryService                               │
│    ├── SessionDiscovery                               │
│    ├── SessionImporter                                │
│    ├── CapabilityService                              │
│    ├── ActivityHub                                    │
│    ├── RemoteEventStore                               │
│    ├── AuditRecorder                                  │
│    └── CodexAdapter                                   │
│             │                                         │
│       AppServerProcess                                │
│             │ Unix domain socket                      │
└─────────────┼─────────────────────────────────────────┘
              ▼
       codex app-server
         ├── thread A
         ├── thread B
         └── thread C
```

核心原则是：

- Codex 是 session/thread 持久化的真源；
- 文件系统是目录和工作区内容的真源；
- 公共 Tailscale 是节点协调/认证 control plane，embedded tsnet identity 是 Host 网络身份；
- Codex 业务数据不经过 Codex Remote 自建中心 relay；
- append-only JSONL 是 Host 面向开发与用户排错的诊断记录；
- SQLite 保存 Remote 管理元数据、当前状态和检索索引，不取代上述真源；
- 客户端只依赖 Host 的稳定内部能力，不直接依赖原始 Codex 协议。

## 4. 运行时拓扑

V1 使用一个长期 app-server，而不是为每个 Codex 启动独立进程：

```text
codex-remote-host
       │
       └── codex app-server
              ├── thread/session 1
              ├── thread/session 2
              └── thread/session N
```

产品语义中的“一个 Codex”对应 app-server 中的一个 thread/session。Host 内部记录产品 `codex_id` 与底层 `thread_id` 的一一映射。

选择单 app-server 的理由：

- app-server 原生承载多 thread；
- 已有 session 的 list/read/resume 由同一控制面提供；
- Demo 的进程管理、能力协商和事件接收更简单；
- 不妨碍未来在确有隔离或容量需求时引入多 runtime。

V1 不实现多 app-server 调度抽象，但 `CodexAdapter` 与进程管理边界应避免让上层直接依赖进程细节。

## 5. Host 启动与 app-server 生命周期

典型启动方式：

```bash
tmux new -s codex-remote
./codex-remote-host
```

Host 启动顺序：

```text
打开状态数据库
  → 打开 AuditRecorder
  → 并行启动
      ├── TailnetService(tsnet.Server)
      │     ├── 首次运行时显示 Tailscale 登录 URL
      │     └── 等待 Tailnet node + plain TCP listener 就绪
      └── AppServerProcess
            ├── 清理或识别上次遗留的 socket/process 状态
            ├── 启动 codex app-server
            └── 等待 Unix socket、连接并 initialize
  → 刷新 capabilities
  → 开始接收事件
  → Gateway 标记 Ready
```

TailnetService 与 AppServerProcess 在运行时相互独立：Tailnet 暂时断开不得停止 app-server，app-server 重启也不得销毁 tsnet node。只有 Tailnet listener、Gateway 和 app-server 都 Ready，Host 才对远端声明 Ready。

`AppServerProcess` 至少负责：

- 构造并启动 app-server 子进程；
- 创建和管理 Unix socket 路径；
- 就绪探测与 initialize；
- 捕获退出码和 stderr；
- 在异常退出后记录事实并按有限退避策略重启；
- 重启后重新连接、重新协商能力并恢复 Host 的订阅状态；
- Host 正常退出时终止自己启动的 app-server 并完成日志 flush。

Host 只管理自己启动的 app-server，不应误杀用户手动运行的其他 Codex/TUI 进程。

systemd 可作为未来部署增强，但不是 V1 依赖或正确运行的前提。

## 6. Host ↔ app-server 传输

V1 优先采用 Unix domain socket。

```text
CodexAdapter
    │ bidirectional Codex RPC/events
    ▼
UnixSocketTransport
    │
    ▼
codex app-server
```

选择 Unix socket 是 Linux-only、同机进程通信和长期连接约束下的默认方案。socket 路径应位于 Host 自己的运行目录中，启动时明确处理：

- 已存在但无监听者的陈旧 socket；
- 已有健康 app-server 与 Host 自己启动实例的归属；
- 连接断开、半开连接和进程退出；
- socket 就绪早于协议 initialize 完成的情况。

Transport 细节应封装在 `CodexAdapter` 下方；上层模块只操作规范化请求、响应和活动。

## 7. 核心模块

### 7.1 CodexManager

应用核心协调器，负责：

- 创建、打开和查找 Codex 产品记录；
- 维护 `codex_id ↔ thread_id ↔ cwd` 映射；
- 协调目录准备、session discovery/import、start/resume；
- 将用户动作交给 Adapter；
- 把状态变化发布到 ActivityHub 并写入 AuditRecorder。

它不解析原始 wire，也不直接写 socket。

### 7.2 DirectoryService

负责开发机目录的规范化、校验和创建。

对输入路径执行：

1. 展开产品允许的路径表示；
2. 规范化为绝对路径；
3. 若路径存在，验证其为可用目录；
4. 若路径不存在，执行等价于 `mkdir -p` 的递归创建；
5. 返回规范路径或明确错误。

目录创建必须先成功，才能启动新 thread。创建动作及失败原因写入 Host action audit。

### 7.3 SessionDiscovery

负责查询 Codex 所知道的 sessions，并按规范化 cwd 精确筛选。

职责包括：

- 主动覆盖产品需要支持的 session 来源类型，而不是依赖可能只返回部分来源的默认过滤器；
- 返回 thread id、cwd、标题/预览、时间、来源和可用状态；
- 默认过滤 subagent、内部 session 或其他不适合直接交互的记录；
- 将已由 Remote 管理的 thread 与尚未导入的本地 thread 合并展示但不重复。

Session Catalog 的真源是 Codex，而不是 Host 的 SQLite。

### 7.4 SessionImporter

负责把已有 Codex thread 纳入 Remote 管理。

首次导入流程：

```text
选择已有 thread
  → 校验 thread 与 cwd
  → 检查 thread_id 是否已导入
  → 读取完整可得的持久化历史
  → 写入 imported_history 审计记录
  → 创建或关联 Remote CodexRecord
  → resume thread
  → 后续事件按 live_wire 记录
```

导入不修改或复制原 Codex thread。重复导入同一 `thread_id` 必须幂等，复用已有 `codex_id`。

### 7.5 CodexAdapter

CodexAdapter 是 Host 核心与具体 app-server 协议之间的防腐层，负责：

- RPC 请求/响应关联；
- thread list/read/start/resume；
- turn start/interrupt 及审批、用户输入响应；
- 原始 Codex 事件到 canonical activity 的翻译；
- 保留无法立即规范化的 vendor data，避免信息丢失；
- 把每条收发消息先交给 AuditRecorder，再进入翻译或业务处理。

核心模块和 C/S 协议不直接暴露 Codex wire method 或版本特有字段。

### 7.6 ActivityHub

负责 Host 内部实时活动分发：

- 接收 CodexAdapter 产生的 canonical activities；
- 按 codex/thread 路由给当前订阅者；
- 支持多个只读订阅者；
- 不承担长期审计真源职责；
- 不承担 C/S framing；由 Gateway 把 canonical activity 包装成 Remote V1 event。

客户端断线重连采用 `WatchCodex` + per-Codex `event_seq`。ActivityHub 提供实时发布边界，Gateway 和 RemoteEventStore 共同负责原子建立 Watch、连续 replay，或在不能连续恢复时返回 `CurrentView` reset。

### 7.7 CapabilityService

负责在 app-server initialize 后读取并规范化其能力：

- 当前 Codex/app-server 版本；
- 支持的方法、实验性 API 和 session source 类型；
- 可用模型、模式、权限或审批能力；
- Host 可以稳定向上层承诺的能力集合。

对不可用能力应显式降级，不允许客户端仅凭版本号猜测。

### 7.8 AuditRecorder

AuditRecorder 是 V1 的一等组件。任何一次重要动作都应能沿因果链追溯：

```text
Host action
  → raw request
  → raw response/events
  → canonical activities
  → Host state/log outcome
```

AuditRecorder 必须在 Adapter 翻译前捕获 raw wire，避免 Adapter bug 导致原始证据丢失。

C/S 审计不能只存在于 Host。Client 同样记录 UI action、wire send/receive 和 state apply，双方使用同一 `audit.proto` 和 ProtoJSON JSONL 格式。Host 侧记录必须能与 Client 侧通过 `connection_id`、`request_id` 或 `(codex_id,event_seq)` 对账。

### 7.9 RemoteEventStore

负责把 canonical activity 转成客户端可同步的持久化事件流：

- 为每个 `codex_id` 分配严格递增的 `event_seq`；
- 在发布给 ActivityHub/Gateway 前完成持久化；
- 维护 `event_seq → canonical activity journal offset` 索引；
- 按 `after_event_seq` 提供有序 replay；
- 在不能可靠 replay 时构造一致性 `CurrentView` reset；
- 分页提供已完成、基本不再变化的历史 turns。

RemoteEventStore 不复制 app-server raw wire，也不是新的业务真源。其事件内容来自 canonical activity journal，SQLite 只保存游标、当前状态和文件 offset 索引。

同一 Codex 的写入顺序必须是：

```text
canonical activity
  → allocate event_seq
  → append activity/event record durably
  → update index/current state
  → publish to ActivityHub
  → Gateway sends Event
```

`WatchCodex` 必须原子完成 watcher 注册、`head_event_seq` 边界确定和 `CurrentView` 构造，保证 reset 已包含 head 及以前事件的效果。已完成历史不进入 CurrentView，通过 `ListHistory` 独立分页。

## 8. 全量审计设计

### 8.1 必须记录的数据

1. **C/S raw wire**：Client 与 Host 两侧各自观察到的入站和出站消息，包含精确 raw text、解析后的 Frame、方向和关联 ID；正常心跳可采样；
2. **app-server raw wire**：Host 与 app-server 之间所有原始入站和出站消息；
3. **Canonical activity**：翻译后的消息、reasoning、命令、工具、文件修改、审批、用户输入、turn 和状态变化；
4. **Host action/RPC**：目录创建、session 创建/导入/继续、消息发送、中断、审批响应、能力刷新、进程启停与重启等；
5. **Client action/state apply**：用户动作以及 Response/Event 是否成功进入客户端状态；
6. **Runtime stderr 与应用日志**：app-server stderr、Host 日志和 Client 日志；
7. **Tailnet lifecycle/identity**：tsnet 启停、认证状态、listener、WhoIs 结果、断开与恢复，不记录 auth key 或 node private state。

“全量”表示不只存最终对话或 canonical 结果；原始协议事实和进程日志也必须保留。Demo 暂不以敏感数据脱敏或租户隔离作为设计目标，但记录失败不能静默发生。

### 8.2 存储职责

```text
~/.codex-remote/
├── state.db
├── run/
│   └── app-server.sock
└── audit/
    ├── wire/
    │   ├── app-server/
    │   └── client-server/
    ├── rpc/
    ├── activities/
    ├── host-actions/
    ├── runtime/
    └── host/
```

上述为 Host 本地布局。Client audit 存在客户端自己的持久化存储中；浏览器使用 IndexedDB，并可导出为同一 AuditRecord JSONL 诊断包。Host 不假装替 Client 记录其 UI action 或 state apply。

具体按日或按大小滚动均可在实现阶段确定，但职责固定：

- **append-only JSONL 是面向排错的诊断记录**；
- **SQLite 是元数据、当前状态和检索索引**；
- 不要求从 JSONL 重建 SQLite；Codex 持久化 session 仍是对话真相；
- 已写入的 JSONL 记录不得原地修改或覆盖。

### 8.3 共同关联字段

各类审计记录至少应能通过以下标识关联：

- `process_run_id` 和本地严格递增的 `local_seq`；
- `record_id` 与本地 `parent_record_id`；
- `connection_id`、`client_id` 和 `client_run_id`；
- `codex_id`；
- Codex `thread_id`；
- `turn_id` 和 `item_id`（存在时）；
- RPC request id（存在时）；
- timestamp；
- record kind 与 direction；
- provenance。

本地 `local_seq` 仅表示一个 Client 或 Host process run 内的观察顺序，不得用作 C/S replay。C/S replay 使用每个 `codex_id` 独立持久化的 `event_seq`。

### 8.4 写入顺序和失败原则

- 出站 C/S frame 在 WebSocket write 前记录 STARTED，在 write 返回后追加 SUCCEEDED 或 FAILED；
- 入站 raw message 在解码和业务处理前记录；
- 有效 C/S frame 同时保存精确 `raw_text` 和解析后的结构化 `frame`；
- 副作用 Request 的 Host inbound 记录成功追加前不得执行；
- canonical activity 在成功翻译后追加；
- Host action 记录意图、结果和错误；
- 进程 stderr 与 Host logs 保留各自原始顺序，并携带可用于关联的运行实例信息；
- 写入器应串行化同一 journal 的追加，避免记录交错损坏；
- 审计不可写时，Host 必须明确暴露 degraded/unhealthy 状态并记录到仍可用的错误通道；个人 Demo 不要求像合规系统一样阻断全部业务。

V1 使用单写入器和有界队列。记录可批量 flush，退出时尽力 flush；不要求高频逐条 `fsync`、hash chain、防篡改证明或法规级耐久。队列满或存储失败不能静默，Host 进入 degraded 并保留 dropped/incomplete 诊断。完整格式与对账矩阵见 `codex_remote_audit_v1.md`。

## 9. 已有 session 的历史与 provenance

对于首次导入的旧 session，Host 没有它过去真实发生时的 raw wire。因此导入历史与实时捕获必须严格区分。

### 9.1 `imported_history`

通过 thread read、turn/item list 或其他持久化读取接口恢复出的既有历史标记为：

```text
provenance = imported_history
```

它表示“导入时从现有持久化状态观察到的历史”，不能伪装成当时捕获的原始 wire。记录中应保留：

- 导入时间；
- 原 session/thread id；
- 原 session source；
- 可得的原始创建/更新时间；
- 读取 API/方式和 app-server 版本；
- 历史是否完整、是否分页完成，以及任何缺口。

### 9.2 `live_wire`

Host 建立管理关系后，实际从 app-server 实时收到或向其发送的消息标记为：

```text
provenance = live_wire
```

由这些消息翻译得到的 canonical activity 应能回链到对应 raw wire 记录。

导入边界必须显式记录，确保排查时可以回答“哪些是恢复得到的历史，哪些是 Host 当时亲自观察到的事实”。

## 10. 元数据模型

SQLite 至少保存以下逻辑实体；字段可在实现时细化。

```text
CodexRecord
  id                 # Remote codex_id
  thread_id          # Codex thread/session identity
  cwd                # normalized absolute path
  title
  origin             # remote_created | local_existing
  imported_at
  created_at
  last_opened_at
  last_known_status
  runtime_version

AuditIndex
  record_id
  process_run_id
  local_seq
  journal_kind
  file_path
  byte_offset
  timestamp
  connection_id
  codex_id
  event_seq
  thread_id
  turn_id
  item_id
  request_id
  provenance

RemoteEventIndex
  codex_id
  event_seq
  journal_file
  byte_offset

RequestDedup
  request_id
  operation
  target_id
  final_response
  completed_at
```

`thread_id` 应具有唯一约束，保证已有 session 重复发现或重复导入时不会产生多个 CodexRecord。`request_id` 对副作用操作具有唯一约束，Host 重启后仍能返回第一次执行结果。

## 11. 关键流程

### 11.1 已有目录

```text
Client selects path
  → DirectoryService.normalize/validate
  → SessionDiscovery.listByCwd
  → return existing sessions + Start New option
```

选择已有 session：

```text
SessionImporter.importOrOpen
  → read persisted history if first import
  → record imported_history + provenance
  → CodexAdapter.resume
  → subscribe live events
```

选择新建：

```text
CodexManager.create
  → CodexAdapter.threadStart(cwd)
  → create CodexRecord(origin=remote_created)
  → subscribe live events
```

### 11.2 不存在目录

```text
Client enters path
  → DirectoryService.normalize
  → mkdir -p
  → audit Host action
  → CodexAdapter.threadStart(cwd)
  → create CodexRecord
```

若 `mkdir -p` 失败，不得调用 thread start。

### 11.3 app-server 异常退出

```text
process exits
  → capture exit status and remaining stderr
  → audit runtime/Host action
  → mark runtime unavailable
  → bounded backoff restart
  → wait socket + initialize
  → refresh capabilities
  → restore subscriptions/current metadata
  → mark runtime ready
```

Codex thread 历史由 Codex 持久化；Host 不通过重放旧 raw requests 来恢复 session。

计划重启只允许在所有 Codex 都没有 active turn 时执行。重启后必须复用 tsnet node identity，重新发现 thread 并恢复稳定 `codex_id` 映射、history/session 可见性、pending approval/user-input 和可继续状态。新的 `host_run_id` 使原运行内 replay 连续性失效，因此 `WatchCodex` 返回 `RESET + CurrentView`。V1 不承诺 active turn 跨 Host/runtime 进程热恢复，也不盲重放 dedup 表中的 `IN_PROGRESS` 请求或结果未知的副作用。

## 12. live TUI 同 session 双控边界

V1 可以发现并继续已持久化的本地 session，但不保证与一个仍在运行的本地 Codex TUI 对同一 live thread 可靠共存。

因此架构上：

- 不假设 app-server 会把实时事件可靠 fan-out 给多个独立前端；
- 不实现两个控制者之间的输入、审批或中断仲裁；
- 不把另一前端当前的临时状态视为 Host 可完整重建的状态；
- 若能检测到潜在 live 占用，可向客户端报告风险；无法检测时也不得宣称双控安全。

已停止本地 TUI 后，通过持久化 thread resume 继续属于 V1 支持路径。

## 13. Demo 安全与规模边界

V1 Demo 暂不设计：

- 多租户数据隔离；
- 复杂用户、角色和权限模型；
- 面向公网的零信任应用认证；
- 审计数据脱敏、加密归档和法规保留策略；
- 多 Host 集群、水平扩容或集中式数据库；
- 大量同时在线客户端、海量 session 或无限期高吞吐日志。

这不意味着实现可以任意越权操作本机。目录范围、进程归属和错误处理仍应保持最小且明确，但不把生产级安全体系作为 Demo 验收条件。

## 14. 建议代码边界

```text
internal/
├── codex/
│   ├── manager
│   ├── registry
│   └── model
├── directory/
│   └── service
├── session/
│   ├── discovery
│   └── importer
├── adapter/codex/
│   ├── client
│   ├── translator
│   └── transport_unix
├── runtime/
│   └── app_server_process
├── activity/
│   ├── hub
│   └── remote_event_store
├── capability/
│   └── service
├── audit/
│   ├── recorder
│   ├── wire
│   ├── activity
│   ├── host_action
│   ├── rpc
│   └── log_capture
├── persistence/
│   └── sqlite
├── tailnet/
│   ├── service
│   ├── tsnet_server
│   ├── listener
│   ├── identity
│   └── audit
└── gateway/
    ├── websocket
    ├── protojson_codec
    ├── rpc_dispatcher
    ├── watch
    └── heartbeat
```

目录只是建议，模块职责和依赖方向比具体包名更重要。

## 15. C/S 通信协议

V1 使用 embedded tsnet listener 上的一条 plain WebSocket 长连接承载全部业务通信：

- Protobuf IDL 是接口语义和生成类型的唯一真源；
- 线上编码固定为 ProtoJSON；
- 每个 WebSocket text message 包含一个完整 `Frame`；
- RPC request/response、server-push event、Watch、心跳和关闭通知复用同一连接；
- 不提供平行业务 HTTP API，也不支持 binary Protobuf 双轨；
- WebSocket subprotocol 为 `codex-remote.v1.protojson`；
- 副作用 Request 重试复用全局唯一 `request_id`，Host 持久化结果保证幂等；
- 断线恢复使用原子 `WatchCodex`，可连续时 replay，不可连续时直接返回 `CurrentView` reset；
- completed history 通过 `ListHistory` 独立分页；
- 正式 wire 不包含 `connection_seq`，审计本地顺序使用 `local_seq`；
- 只使用协议 Ping/Pong 检测连接和 Gateway 活性；
- 不使用独立 Subscribe/Ack/Resync/Snapshot 状态机。

Gateway 由 TailnetService 的 `tsnet.Server.Listen("tcp", ":80")` listener 承载，只能访问 `CodexManager`、`ActivityHub`、`CapabilityService`、CurrentView/history 查询和 RemoteEventStore，不得把 app-server raw wire 暴露给客户端。它不建立宿主机、LAN 或公网 listener。完整规范见 `codex_remote_cs_protocol_v1.md`、`codex_remote_audit_v1.md`、`codex_remote_host_tailnet_v1.md` 和 `protocol/codex/remote/v1/`。

多客户端 controller/observer 仲裁仍不属于 V1；在加入该能力前，协议不承诺多个客户端同时发出相互冲突命令的产品语义。

## 16. V1 架构验收点

1. Host 可在 Linux tmux 中独立常驻，不依赖 systemd；
2. Host 可启动、监控并恢复单个长期 app-server；
3. Host 通过 Unix socket 完成 initialize 和双向事件处理；
4. 一个 app-server 可管理多个 Remote Codex/thread；
5. 已有目录能发现本地 sessions，并能导入、读取历史和继续；
6. 不存在目录能通过 `mkdir -p` 创建后启动新 thread；
7. 重复导入同一 thread 不产生重复产品记录；
8. raw wire、canonical activity、Host action、runtime stderr 和 Host logs 均完整落盘；
9. JSONL 保留 append-only 排错记录，SQLite 承担 Remote 元数据、状态和索引；不要求从 JSONL 重建 SQLite；
10. imported history 与后续 live wire 有明确 provenance 和导入边界；
11. app-server 异常退出后有可审计的重启和恢复过程；
12. 架构与产品均不承诺 live TUI 同 session 实时双控；
13. Gateway 实现单 WebSocket + ProtoJSON 协议，包含握手、RPC、心跳、原子 Watch/reset/replay 和幂等 Request；
14. Client/Host 使用同一可读 AuditRecord JSONL，任意 Request/Event 能跨两端对账；
15. Host 在没有系统 `tailscaled` 和 root 的情况下通过 embedded tsnet 加入公共 Tailscale；
16. Host 只使用 Tailnet `Listen("tcp", ":80")` plain WS，不启用 ListenTLS、WSS、Serve、Funnel、公网/LAN fallback 或自建 relay；
17. WhoIs 身份、Tailnet 生命周期和断线恢复可审计，auth key/node state 不进入审计；
18. 核心模块不依赖 WebSocket framing 或 tsnet 具体类型，也不向客户端泄漏 app-server raw wire。
