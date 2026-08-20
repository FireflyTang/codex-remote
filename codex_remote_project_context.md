# Codex Remote 项目上下文与既定决策

> 本文用于把完整项目背景交接给后续 Codex。若早期讨论、旧草稿与本文冲突，以本文列出的“当前已定案”内容和现有 V1 文档为准。不要把历史探索方案重新当成当前方案。

## 1. 一句话定义

Codex Remote 是一个面向个人私有部署的、Codex-first 的远程结构化客户端与本地控制平面：它运行在用户自己的 Linux 开发机上，通过内嵌的 Tailscale 节点安全接入用户的 Tailnet，并通过 `codex app-server` 结构化地创建、发现、继续、观察和控制 Codex sessions。

它要让用户从手机或另一台设备远程使用开发机上的 Codex，同时继续使用开发机本地的：

- 代码与工作目录；
- shell、工具和 sandbox；
- 凭据与账号配置；
- MCP 和其他本地集成；
- Codex 原生 session 历史。

最重要的边界是：

> Codex 负责 agent 能力、工具执行和 session 真相；Codex Remote 负责远程接入、生命周期管理、稳定的产品协议、状态恢复和全链路审计。

## 2. 项目为什么存在

项目源于对 Happy Coder 一类远程 Codex 产品稳定性和可排查性的不满。目标不是简单复刻 Happy Coder，而是利用现在已经存在的两项结构化能力重新做一套更干净的实现：

1. Codex 已提供 `codex app-server`，可以结构化控制 thread、turn、item、approval、user input、interrupt 等，不必拦截终端 TUI；
2. Tailscale 提供 `tsnet`，可以把 Tailnet node 直接嵌入 Host 进程，不要求 Linux 主机预装系统级 Tailscale。

因此，项目真正要攻克的是：

- 多个本地 Codex sessions 的清晰管理；
- 远程连接断开后的正确恢复；
- Host 与 app-server 的可靠生命周期；
- approval、user input 和运行中状态不会因客户端暂时离线而丢失；
- Codex 协议变化被 Adapter 隔离，不直接扩散到客户端；
- 出现问题后能准确判断故障发生在 Client、网络、Host、Adapter 还是 app-server。

项目的差异化不只是“用了 Tailscale”，而是：

> 内嵌 Tailnet 接入 + Codex 结构化控制 + 持久状态恢复 + 可读、可对账的全链路审计。

## 3. 它明确不是什么

Codex Remote 不是：

- 之前讨论过的“远端模型 + 本地瘦执行器”；两者没有关系；
- HivemindOS 一类通用多 Agent 编排、协作或调度平台；
- 一个试图同时抽象 Claude Code、OpenCode、Codex 等产品的统一 Agent 平台；
- 远程终端、终端模拟器或 Codex TUI 的 PTY/ANSI/键盘注入代理；
- Codex agent loop、tool runtime、sandbox 或 session 存储的重实现；
- 由产品方托管业务数据的中心化云服务；
- 自建 Headscale control plane；
- 为大企业、多租户和海量并发设计的平台。

项目定位始终是：

> 先把一个用户自己 Linux 开发机上的原生 Codex，远程使用得可靠、清楚、可恢复、可排查。

## 4. 当前产品语义

### 4.1 核心对象

产品中：

> 一个 Codex = 一个底层 Codex session/thread。

用户说“本地有多个 Codex”，含义是 Host 管理多个可独立选择和交互的 sessions，而不是默认启动多个 app-server 进程，也不是再增加一层 workspace/instance/session 的复杂层级。

一个 Codex 至少绑定：

- 一个稳定的产品 `codex_id`；
- 一个 Codex `thread_id`；
- 一个规范化工作目录 `cwd`；
- 来源：由 Remote 创建，或从本地已有 session 导入；
- 当前状态、能力和待处理请求。

### 4.2 已有目录

用户选择一个已存在目录时，系统不能立即新建 session。正确流程是：

1. 规范化目录路径；
2. 列出该目录下本地已有、可继续的 Codex sessions；
3. 让用户选择继续其中一个，或者在该目录新建一个；
4. 隐藏不适合直接交互的内部或 subagent sessions；
5. 若该 thread 已导入过，复用原来的 `codex_id`，不能重复建立产品记录。

### 4.3 不存在的目录

用户输入一个不存在的新目录时：

1. Host 执行等价于 `mkdir -p` 的安全创建；
2. 创建成功后在该目录启动一个新 session；
3. 创建失败时明确返回失败，不能表现为 Codex 已创建。

### 4.4 导入已有 session

“导入”只表示 Codex Remote 开始展示和管理该 thread：

- 不复制 thread；
- 不重写原始历史；
- 不更换其 Codex 身份；
- 继续交互仍追加到同一 thread；
- 导入操作按 `thread_id` 幂等；
- 保留 session 的来源和导入 provenance。

首次导入时读入的历史标记为 `imported_history`；Host 接管后从 app-server 实时观察到的新活动标记为 `live_wire`。两者都能展示，但证据来源不能混淆。

### 4.5 live TUI 并发边界

V1 不承诺 Codex Remote 与一个仍在运行的本地 Codex TUI 对同一 live session 实时双控。不保证：

- 两端收到完全相同的增量事件；
- 两端同时输入不会竞争；
- approval、interrupt 和状态变化在两端一致；
- Host 能可靠判断另一个前端是否仍占用 thread。

可以继续已经持久化的本地 session，但必要时应提示用户先停止本地 TUI 的实时操作。除非 Codex 官方以后提供明确的多前端订阅和控制语义，否则不要把双控写成已支持能力。

## 5. 当前服务器端范围

当前设计与实现阶段只处理服务器端 Host。V1 约束为：

- Host 仅支持 Linux；
- Demo/个人自用优先；
- 暂不围绕生产级安全、多租户、RBAC、高并发和集中运维设计；
- Host 可以直接放在 `tmux` 中常驻，不依赖 systemd；
- 客户端具体形态和客户端如何加入 Tailnet 暂不属于服务器端方案。

这不代表客户端不重要，而是当前边界刻意收敛：服务器端先提供稳定、可生成类型、可恢复、可审计的接口。

## 6. Tailscale/tsnet 是核心方案，不是附属功能

### 6.1 已定案

- Host 使用 Go，主要原因是官方 `tailscale.com/tsnet`；
- 使用公共 Tailscale control plane，不自建 Headscale；
- `tsnet.Server` 直接嵌入 Host 进程；
- Host 在 Tailnet 中是一个拥有独立身份的 node；
- Linux 宿主机不需要预装或运行系统 `tailscaled`；
- 不需要 root，不创建系统级 VPN/TUN；
- `tsnet.Server.Dir` 持久化 node 身份，Host 重启后复用；
- 首次启动可以在 tmux/console 输出 Tailscale 登录 URL；无人值守时可选 auth key；
- Host 通过 `Listen("tcp", ":80")` 提供 Tailnet-only plain WebSocket 入口；
- 可以通过 `LocalClient().WhoIs()` 获得来访 Tailnet 用户/设备信息；
- 不使用 Funnel，不额外开放公网或 LAN fallback；
- 不建设 Codex Remote 自己的业务 relay。

Tailscale control plane 用于节点认证和协调。业务流量能直连时直连，不能直连时可能经过 Tailscale DERP；它不经过 Codex Remote 自己运营的中心服务器。

### 6.2 服务器端网络拓扑

```text
                 Public Tailscale control plane
                    auth / coordination / ACL
                               │
                               │
Client ═══════ user's Tailnet ═╪══════ embedded tsnet node
          direct or Tailscale DERP     inside Linux Host
                                             │
                                     plain WebSocket Gateway
                                             │
                                      Application Core
                                             │
                                        CodexAdapter
                                             │
                                         Unix socket
                                             │
                                      codex app-server
```

Tailscale 网络生命周期与 Codex runtime 生命周期相互独立：Tailnet 暂时失联不应停止正在运行的 Codex；app-server 重启也不应销毁 tsnet node 身份。

## 7. Host 与 Codex runtime 架构

### 7.1 当前拓扑

V1 Demo 使用：

```text
1 Linux Host process
  └── 1 long-lived codex app-server process
        ├── thread A = Codex A
        ├── thread B = Codex B
        └── thread C = Codex C
```

Host 自己负责 app-server 的：

- 启动、初始化和就绪判断；
- Unix socket 文件管理；
- stderr 收集；
- 异常退出检测和有限重启；
- 重连、thread rediscovery 和状态重建；
- 有序关闭。

Host 到 app-server 优先使用 WebSocket over Unix domain socket。此前讨论过的 stdio 只是历史备选，不是当前 V1 主方案。

### 7.2 Host 必须是常驻的 app-server client

远程 Client 不能直接成为 app-server 的临时连接。正确关系是：

```text
Remote Client  ←→  persistent Host  ←→  codex app-server
```

Client 离线后，Host 与 app-server 的连接、Codex 当前运行、事件接收和待处理 approval/user input 仍继续存在。Client 重连后，从 Host 的当前状态和持久事件序列恢复。

进程重启边界与普通 Client 断线不同：只允许无 active turn 时计划重启；重启后复用 tsnet node identity 并恢复 `codex_id` 映射、session/history 可见性、pending 状态和继续能力，Watch 以 `RESET + CurrentView` 重新建立状态。V1 不做 active-turn 热恢复，也不盲重放 `IN_PROGRESS` 请求或结果未知的副作用。

### 7.3 核心模块

- `TailnetService`：内嵌 tsnet node、登录、listener、peer identity 和网络状态；
- `Gateway`：plain WebSocket、协议握手、Frame 编解码和连接生命周期；
- `CodexManager`：产品 Codex 的创建、打开、恢复和状态协调；
- `DirectoryService`：路径规范化、存在性检查、`mkdir -p` 和目录错误；
- `SessionDiscovery`：按 cwd 发现本地可继续 threads，并过滤内部 session；
- `SessionImporter`：幂等导入、历史读取、产品记录与 provenance；
- `CodexAdapter`：隔离具体 Codex app-server 版本和 raw wire；
- `ActivityHub`：维护 canonical activity/current view，并分发给远程连接；
- `RemoteEventStore`：按 Codex 保存可重放的持久事件序列；
- `CapabilityService`：把当前 Host/Codex 能力明确暴露给 Client；
- `AuditRecorder`：记录两条协议边界、Host 行为和运行日志。

## 8. Source of truth 边界

| 数据 | 真源 |
|---|---|
| Codex 对话、turn、item、持久 thread | Codex/app-server 的原生 session 存储 |
| 工作目录和文件 | Linux 文件系统 |
| Host 网络身份和 Tailnet 连通性 | `tsnet` 持久状态与公共 Tailscale |
| 远程协议可恢复事件序列 | Host `RemoteEventStore` |
| 全链路审计事实 | append-only JSONL audit journal |
| 产品映射、当前状态、索引、幂等结果 | Host SQLite |

SQLite 不是 Codex 对话真源，也不是审计真源。不要复制一套“自己的完整 conversation database”来替代 Codex。

## 9. C/S 协议的当前最终决策

C/S 协议已经从 TBD 收敛为以下方案：

- 接口定义：Protocol Buffers proto3 IDL；
- 传输：embedded tsnet listener 上的一条长期 plain WebSocket；
- 编码：ProtoJSON；
- WebSocket 使用 UTF-8 text frame；
- subprotocol：`codex-remote.v1.protojson`；
- RPC、事件和心跳复用同一连接；
- 没有平行的 HTTP 业务 API；
- 不支持 binary protobuf；
- 不使用 gRPC、Connect、SSE 或 JSON-RPC 作为远程产品协议；
- Codex app-server raw protocol 不暴露给 Client。

选择 Protobuf 是为了 IDL、生成类型和演进纪律；选择 ProtoJSON text 是为了让人工和机器都容易抓包、审计和复现。正式消息必须通过 Protobuf 生成类型和 ProtoJSON codec 读写，不能手拼 JSON。

### 9.1 Frame 只保留必要类型

```text
ClientHello / ServerHello
Request / Response
Event
Ping / Pong
Close
```

### 9.2 可靠性规则

- `request_id` 同时是 RPC 关联 ID 和副作用操作的幂等键；断线重试复用原 ID；
- 每个 Codex 有独立、持久化、单调递增的 `event_seq`；
- `WatchCodex(after_event_seq)` 原子返回：可连续恢复时 replay，无法连续恢复时 RESET + `CurrentView`；
- 历史分页通过 `ListHistory` 获取，当前运行态通过 `WatchCodex` 获取；
- approval 和 user input 是可恢复的 pending state，不是只发一次的瞬时通知；
- WebSocket 已保证单连接内有序，因此不再自造 `connection_seq`、Ack 等复杂层；
- 客户端重连是正常路径，不依赖连接永久在线。

### 9.3 心跳

协议有应用层心跳：

- 连接空闲 15 秒后由 Host 发送 `Ping`；
- Client 返回相同 nonce 的 `Pong`；
- 收到任何合法 Frame 都刷新接收时间；
- 45 秒没有收到合法 Frame 时关闭并重连；
- 心跳不占用 `event_seq`，正常心跳只做采样审计。

## 10. 审计与问题界定是一等产品能力

只记录 Host 日志不够，因为它无法证明 UI 是否发出了动作、Client 是否成功写入连接、Client 是否正确应用了 Host 事件。因此 Client 和 Host 都必须审计。

### 10.1 格式

- Client 和 Host 共用 `audit.proto`；
- 每条 `AuditRecord` 使用 ProtoJSON；
- 一行一个完整 JSON 对象，形成 append-only JSONL；
- 字段命名、枚举和 64 位整数遵守 ProtoJSON；
- 既保存足够可读的解码字段，也保存精确的 `raw_text`、长度和 hash；
- 不把 auth key、Tailscale node state 等秘密写入审计。

### 10.2 Host 必须覆盖的证据

- Client ↔ Host 的原始 C/S wire 和 decoded Frame；
- Host RPC 的接收、dispatch、完成和失败；
- Host action，例如建目录、导入、启动、中断和审批决策；
- Host ↔ app-server 的完整 raw wire；
- Adapter 产生的 canonical activity；
- app-server stdout/stderr 与进程生命周期；
- Host 自身日志、状态应用和异常。

### 10.3 Client 必须覆盖的证据

- UI action；
- outbound Frame 的准备、发送开始、成功或失败；
- inbound raw Frame 的收到、解码成功或失败；
- Client reducer/state apply 的开始、完成或失败；
- 用户最终看到的错误和诊断包导出信息。

### 10.4 对账键

- 单次连接：`connection_id`；
- 一次操作：`request_id`；
- 一个 Codex 的一个事件：`(codex_id, event_seq)`；
- 单进程本地顺序：`local_seq`，但它不是跨端 wire 顺序。

目标是让开发者按证据回答：问题在 Client UI、Client transport、Tailnet/连接、Host Gateway、Host state、CodexAdapter 还是 app-server，而不是凭猜测归责。

## 11. 当前明确不做

- macOS/Windows Host；
- 多 app-server runtime、runtime pool 或版本隔离；
- 通用多 Agent 支持与 Agent 编排；
- 多租户、企业 RBAC、大规模高可用；
- 公网服务、Funnel、产品自建 relay；
- Headscale 或私有 Tailscale control plane；
- 远程终端/TUI 控制；
- 与本地 live TUI 对同一 thread 的可靠实时双控；
- 重新实现 Codex session 历史；
- binary protobuf wire；
- 为协议再叠一套 HTTP 业务 API；
- 生产级 retention、合规和安全体系。

## 12. 仍待决定的问题

这些问题尚未定案，不应自行假设答案：

- 首个 Client 的具体形态和平台；
- Client 如何加入或访问用户 Tailnet；
- 多个远程 Client 同时连接同一 Codex 时的 controller/observer 规则；
- 是否以及如何检测一个本地 TUI 正在占用 live session；
- SessionDiscovery 对不同 Codex source kinds 的最终过滤规则；
- 生产阶段的 retention、安全、权限和规模设计；
- 实现时锁定的 Codex app-server 版本及能力兼容矩阵。

## 13. 已讨论但已经废弃或被替代的方向

| 历史方向 | 当前结论 |
|---|---|
| 与“瘦执行器”项目合并 | 明确无关 |
| 做 HivemindOS 式通用多 Agent 控制面 | 不做，保持 Codex-first |
| 拦截 TUI、PTY 或终端输出 | 不做，直接使用 app-server |
| Host 依赖系统 `tailscaled` | 不依赖，使用内嵌 `tsnet` |
| 自建 Headscale 或产品 relay | 不做，使用公共 Tailscale |
| stdio 作为 Host↔app-server 主 IPC | 已由 Unix socket 优先方案替代 |
| Remote 协议原样透传 Codex raw wire | 已由 canonical Remote Protocol + CodexAdapter 替代 |
| Runtime/Instance/Session 多层产品模型 | V1 收敛为一个 Codex = 一个 thread |
| 多 app-server runtime | Demo 不做，单长期 app-server + 多 thread |
| binary protobuf | 不做，Proto IDL + ProtoJSON text |
| HTTP + WebSocket 两套业务接口 | 不做，单 WebSocket 承载业务 |
| 复杂 Subscribe/Ack/多层 snapshot 协议 | 收敛为 `WatchCodex` + replay 或 RESET/CurrentView |
| 只靠临时 ring buffer 或 console 排错 | 不足，使用持久事件状态与双端 append-only 审计 |

除非用户明确要求重新评估，或出现足以改变架构的官方能力变化，不要重新打开这些选型。

## 14. 后续 Codex 的工作原则

1. 先判断改动属于产品、Host 架构、Tailnet、C/S 协议还是 Audit，保持文档边界；
2. 产品文档只描述用户能力和范围，不塞入 tmux、Unix socket、SQLite 或 wire format；
3. 不把 `tsnet` 忘成外围部署细节，它是 Host 网络接入与私有部署体验的核心；
4. 不把 Codex Remote 描述成通用 Agent 平台；
5. 不复制 Codex 的对话真源；
6. 不把客户端断线等同于 Codex 运行停止；
7. 协议改动必须同步 `.proto`、协议文档和审计字段；
8. 所有可靠性设计都应能被审计证据验证；
9. Demo 不做生产级过度设计，但不能牺牲协议边界、状态恢复和排障能力；
10. 用户可以接受较新、尚不完全稳定但架构更好的技术；若选型不确定且会改变产品方向，应先向用户说明取舍。

## 15. 当前文档集合

- `codex_remote_v1_product_definition.md`：产品定位、用户流程、范围和非目标；
- `codex_remote_host_architecture_v1.md`：Linux Host、app-server 生命周期、模块和本地状态；
- `codex_remote_host_tailnet_v1.md`：只针对服务器端的 tsnet/Tailnet 方案；
- `codex_remote_cs_protocol_v1.md`：WebSocket + ProtoJSON 协议；
- `codex_remote_audit_v1.md`：Client/Host 双端审计与问题界定；
- `protocol/`：当前 proto3 IDL 真源。

后续工作应在这些文档上继续演进，不要再从早期聊天中的临时架构重新起步。
