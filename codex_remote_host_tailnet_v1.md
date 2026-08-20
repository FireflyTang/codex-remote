# Codex Remote Host Tailnet Architecture V1

## 1. 文档目的

本文定义 Codex Remote V1 Linux Host 的网络接入方案，只讨论服务器端。

客户端如何加入 Tailnet 不在本文范围内。本文假定 Client 已经能够访问用户自己的 Tailnet，然后通过 Codex Remote C/S 协议连接 Host。

## 2. 已确定的选择

| 项目 | V1 决策 |
|---|---|
| Host 语言 | Go |
| Tailnet 实现 | 官方 `tailscale.com/tsnet` |
| Tailscale control plane | 公共 Tailscale 服务 |
| 自建 Headscale | 不使用 |
| 系统 `tailscaled` | 不依赖 |
| 系统级 VPN/TUN | 不创建 |
| root 权限 | 不要求 |
| Host 网络身份 | 独立 tsnet Tailnet node |
| 业务入口 | `tsnet.Server.Listen("tcp", ":80")` |
| C/S 协议 | Tailnet-only plain WebSocket + ProtoJSON |
| 公网暴露 | 不使用 Tailscale Funnel |
| 产品自建 relay | 不使用 |
| Tailscale 设备状态 | `tsnet.Server.Dir` 持久化 |
| 首次认证 | Tailscale 登录 URL |
| 无人值守认证 | 可选 auth key |
| 来访身份 | `LocalClient().WhoIs()` |

`tsnet` 会把一个 Tailscale node 直接嵌入 Go 进程，使用 userspace 网络栈，不要求额外运行 `tailscaled` 或修改系统网络配置。[Tailscale tsnet 文档](https://tailscale.com/docs/features/tsnet)、[`tsnet` Go package](https://pkg.go.dev/tailscale.com/tsnet)

## 3. 总体拓扑

```text
                  Public Tailscale Control Plane
                     coordination / node auth
                                │
                                ▼
┌──────────────────── Linux development host ────────────────────┐
│                                                                │
│  codex-remote-host / Go                                        │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ TailnetService                                           │  │
│  │                                                          │  │
│  │ tsnet.Server                                             │  │
│  │   ├── independent node identity                          │  │
│  │   ├── userspace TCP/IP                                   │  │
│  │   ├── persistent state directory                         │  │
│  │   ├── LocalClient / WhoIs                                │  │
│  │   └── Listen("tcp", ":80")                             │  │
│  └──────────────────────────┬───────────────────────────────┘  │
│                             │ net.Listener                     │
│                             ▼                                  │
│                    HTTP / WebSocket Gateway                    │
│                             │                                  │
│                 codex.remote.v1.protojson                      │
│                             │                                  │
│                     Application Core                           │
│                             │                                  │
│                       CodexAdapter                             │
│                             │ Unix socket                      │
│                             ▼                                  │
│                     codex app-server                           │
│                                                                │
└────────────────────────────────────────────────────────────────┘
                                ▲
                                │ Tailnet connection
                                │ direct when possible,
                                │ Tailscale DERP when needed
                                │
                              Client
```

Tailscale control plane 负责节点认证、协调和网络策略。Codex 消息、命令输出、diff 和审批等业务数据不经过 Codex Remote 自己运营的中心服务器。

V1 只调用 embedded node 的 `Listen("tcp", ":80")`。它返回的 listener 仅存在于该 tsnet node 的 userspace Tailnet 网络栈中；不调用 `ListenTLS` 或 `ListenFunnel`，也不在宿主机网卡建立 fallback listener。业务传输使用 plain WebSocket，网络机密性由 Tailnet/WireGuard 提供。

## 4. 为什么 Host 选择 Go

Go 不是因为 Agent 逻辑天然更适合 Go，而是因为 V1 明确选择官方 `tsnet`：

- `tsnet.Server` 是官方嵌入式 Tailscale node；
- 返回标准 `net.Listener` 和 `net.Conn`，可以直接接 Go `net/http`；
- Host 不需要额外 FFI、sidecar 或系统 daemon；
- node state、LocalAPI 和 listener 生命周期都在同一进程管理；
- 单个 Host binary 加一个 Codex app-server 子进程即可运行。

因此 V1 不使用 Rust + C ABI/libtailscale，也不把 Tailscale 做成独立 sidecar。

## 5. TailnetService 职责

`TailnetService` 是 Host 内的一等组件，负责：

- 构造和持有唯一的 `tsnet.Server`；
- 设置稳定 Hostname 和专用 state directory；
- 处理首次登录 URL 和可选 auth key；
- 等待 node 加入 Tailnet；
- 建立 Tailnet-only plain TCP listener；
- 通过 LocalClient 查询来访 node/user identity；
- 将 `net.Listener` 交给 HTTP/WebSocket Gateway；
- 暴露连接、认证、listener 和 node 状态；
- 记录 Tailnet 生命周期及连接身份审计；
- Host 退出时关闭 listener 和 `tsnet.Server`。

它不负责：

- Codex C/S Frame 编解码；
- session、turn 或 approval；
- app-server 进程管理；
- 替代 Tailscale ACL/Grants；
- 建立公网入口；
- 提供通用 SOCKS5、subnet router 或 exit node。

建议接口边界：

```text
TailnetService
  Start(ctx)
  WaitReady(ctx)
  Listener()
  WhoIs(ctx, remoteAddr)
  Status()
  Close()
```

上层 Gateway 只接收标准 listener 和规范化的 Tailnet peer identity，不依赖 `tsnet` 其他内部类型。

## 6. `tsnet.Server` 配置

逻辑配置：

```text
TailnetConfig
  hostname
  state_dir
  listen_addr
  auth_key          optional, secret
  verbose_logging   default false
```

建议默认值：

```text
hostname    codex-remote-<linux-hostname>
state_dir   ~/.codex-remote/tailscale
listen_addr :80
```

对应构造：

```go
srv := &tsnet.Server{
    Hostname: cfg.Hostname,
    Dir:      cfg.StateDir,
    AuthKey:  cfg.AuthKey,
}
```

`Server` 字段必须在第一次 Start/Listen/LocalClient 调用前设置完成。`Server.Dir` 保存 node 的身份与状态，使 Host 重启后能够重新加入原 Tailnet node，而不要求每次重新登录。[`tsnet.Server` API](https://tailscale.com/docs/reference/tsnet-server-api)

V1 不设置自定义 `ControlURL`，因此使用公共 Tailscale control plane。更换为 Headscale 不属于 V1 配置项。

## 7. 状态目录

```text
~/.codex-remote/
├── state.db
├── tailscale/          # tsnet.Server.Dir
├── run/
│   └── app-server.sock
└── audit/
```

要求：

- `tailscale/` 与 Host SQLite、Codex session 和 audit 分开；
- 目录权限限制为当前 Linux 用户；
- 不把 node state 复制进 audit 或诊断包；
- 不在多个 Host 实例间共享同一个 `Server.Dir`；
- 删除 state directory 等价于丢弃该嵌入 node 身份，应视为显式重新注册操作。

## 8. 首次认证与重新认证

### 8.1 交互式首次启动

没有现有 state、没有 auth key 时：

```text
./codex-remote-host
  → start tsnet.Server
  → TAILNET_AUTH_REQUIRED
  → tmux/console 输出 Tailscale authentication URL
  → 用户在浏览器完成登录
  → 如 Tailnet 开启 device approval，等待管理员批准
  → node becomes ready
  → create Tailnet-only plain TCP listener
```

官方文档说明：没有 auth key 或已有凭据时，`Server.Start` 会产生并显示认证 URL；auth key 可以作为无人值守替代方案。[tsnet device creation and authentication](https://tailscale.com/docs/features/tsnet#device-creation-and-authentication)

登录 URL 需要显示给用户，但不应长期写入可导出的完整审计包。审计只记录 `auth_required`、`auth_succeeded` 或 `auth_failed` 等状态。

### 8.2 Auth key

V1 允许从进程环境或受保护配置读取 auth key，并传给 `Server.AuthKey`。不得：

- 把 auth key 放在普通命令行参数中；
- 写入日志、AuditRecord 或 SQLite；
- 通过 C/S 协议返回给 Client。

已有有效 node state 时优先复用 state，不主动强制重新登录。

### 8.3 Node 失效

当 node key 过期、设备被删除或需要重新批准时：

- TailnetService 进入 auth-required/unavailable；
- Host 本地进程与 app-server 不必退出；
- 正在运行的 Codex 继续由 Host 管理和审计；
- console 输出重新认证指引；
- 恢复后重建 listener，Gateway 接受新连接。

## 9. Listener 与 Gateway

默认启动：

```go
ln, err := srv.Listen("tcp", ":80")
httpServer.Serve(ln)
```

`Listen` 返回标准 `net.Listener`，只在 embedded tsnet node 上发布，并会在需要时启动 `tsnet.Server`。V1 不管理证书，也不使用 WSS。[官方 Go API](https://pkg.go.dev/tailscale.com/tsnet#Server.Listen)

HTTP server 只提供：

```text
GET /connect     WebSocket upgrade
GET /healthz     optional
GET /readyz      optional
GET /             optional static client
GET /assets/*     optional static assets
```

业务 RPC 不提供独立 HTTP API，仍全部通过 `/connect` 上的 WebSocket + ProtoJSON。

V1 不同时监听：

- `0.0.0.0` 系统网卡；
- 公网 TCP port；
- `ListenFunnel`；
- 系统 `tailscaled` 提供的 Tailscale IP。

因此 Host 应用服务只通过嵌入 node 的 Tailnet identity 暴露。

## 10. 来访身份

Gateway 接受 WebSocket upgrade 前，通过：

```text
tsnet.Server.LocalClient()
  → WhoIs(ctx, request.RemoteAddr)
```

读取 Tailnet node、用户和 tag 信息。官方文档将 `LocalClient().WhoIs()` 作为识别 tsnet listener 来访者的标准方式。[LocalClient and WhoIs](https://tailscale.com/docs/reference/tsnet-server-api#serverlocalclient)

V1 Demo 不建立应用账号、JWT、refresh token 或复杂 RBAC。访问边界为：

```text
Tailnet membership
  + Tailscale ACL/Grants
  + successful WhoIs
```

WhoIs 结果用于：

- 连接审计；
- 显示连接来自哪个 Tailnet node；
- 未来简单 allowlist 或 controller policy；
- 区分同一用户的不同设备。

WhoIs 失败时拒绝 upgrade，而不是建立一个身份未知的正式连接。

审计可以保存稳定 node/user 标识和必要显示名称，但不能保存 node private key、auth key 或其他 Tailscale 内部秘密。

## 11. Host 生命周期

TailnetService 和 AppServerProcess 是两个相互独立的长期组件：

```text
Host start
  ├── open state.db and AuditRecorder
  ├── start TailnetService
  └── start AppServerProcess

Host READY
  = Tailnet listener ready
  + Gateway ready
  + app-server initialized
```

运行时遵循：

- Tailnet 暂时断开，不停止 app-server 或当前 Codex；
- app-server 重启，不销毁 tsnet node 或 Tailnet listener；
- Client 断线，Host 与 app-server 的长期连接继续；
- Tailnet 恢复后 Client 重新连接并通过 `WatchCodex` reset/replay；
- Host 正常退出时依次停止接收新连接、关闭 Gateway、关闭 tsnet listener、停止 app-server、flush audit。

## 12. 状态模型

TailnetService 内部至少区分：

```text
STOPPED
STARTING
AUTH_REQUIRED
CONNECTING
READY
DEGRADED
STOPPING
```

关键状态变化写入 Host audit：

```text
tailnet.start
tailnet.auth_required
tailnet.auth_succeeded
tailnet.ready
tailnet.listener_started
tailnet.connection_accepted
tailnet.whois_succeeded
tailnet.whois_failed
tailnet.disconnected
tailnet.reconnected
tailnet.stopped
```

普通 Tailscale 内部 debug 日志可以在 verbose 模式开启，但不替代这些稳定、结构化的 Host action records。

## 13. 故障行为

| 故障 | Host 行为 |
|---|---|
| 公共 Tailscale control plane 暂不可达 | TailnetService 重试；app-server 继续运行 |
| 首次 node 未登录 | console 输出登录 URL；Host 未对远端 Ready |
| device 等待 approval | 保持等待并显示状态 |
| node state 损坏 | 记录错误，要求显式重新注册，不静默删除 state |
| `Listen` 失败 | Host degraded；不回退到公网或系统网卡监听 |
| WhoIs 失败 | 拒绝该 WebSocket upgrade并记录审计 |
| Tailnet 连接中断 | 现有 Client 断线；Codex 继续运行；恢复后重连 |
| DERP 被使用 | 对 C/S 协议透明，只影响延迟 |
| audit 不可写 | 按 Audit V1 进入 degraded，不静默运行副作用 |

不允许为了“自动恢复”而：

- 启动 Funnel；
- 监听公网；
- 自动切换到未认证的 LAN listener；
- 删除 tsnet state 重新创建设备。

## 14. 审计要求

Tailnet 记录使用共享 `AuditRecord` JSONL：

```text
kind       CONNECTION_LIFECYCLE / HOST_ACTION
component  tailnet
operation  tailnet.*
```

连接身份至少能关联：

- `host_run_id`；
- `connection_id`；
- Tailnet node stable ID；
- user login/display identity（存在时）；
- node computed name；
- tags；
- remote address；
- WhoIs outcome。

这些信息进入 C/S 连接建立记录，使后续 ClientHello、Request 和 Event 可以追溯到 Tailnet peer。

不得进入审计：

- auth key；
- node private key；
- 未脱敏的临时登录凭据；
- tsnet state 文件内容。

## 15. 代码边界

```text
internal/tailnet/
├── service.go          # lifecycle and stable interface
├── tsnet_server.go     # tsnet.Server construction
├── listener.go         # Listen + HTTP server handoff
├── identity.go         # LocalClient / WhoIs normalization
├── status.go
└── audit.go
```

依赖方向：

```text
Gateway
  → TailnetService interface
      → tailscale.com/tsnet
```

Application Core、CodexManager 和 CodexAdapter 不得直接 import `tailscale.com/tsnet`。

## 16. V1 验收标准

1. Linux 机器未安装 `tailscaled` 时 Host 仍可加入 Tailnet；
2. Host 不需要 root，也不创建系统 TUN/VPN；
3. 首次运行可以通过 console 登录 URL 完成 node 注册；
4. 重启后通过 `Server.Dir` 复用同一 node identity；
5. auth key 可以支持无人值守加入，且不会写入审计；
6. `Listen("tcp", ":80")` 只在 embedded node 的 Tailnet 内提供 HTTP/plain WebSocket；
7. 未启用 Funnel，也没有公网/LAN fallback listener；
8. Gateway 可以通过 WhoIs 得到来访 node/user identity；
9. Tailnet 暂时断开时 app-server 和 Codex sessions 继续运行；
10. 恢复后 Client 可重连并通过 `WatchCodex` 恢复状态；
11. Tailnet 生命周期和连接身份进入可读 AuditRecord；
12. Codex 业务数据不经过 Codex Remote 自建中心 relay；
13. Host 仍然是单 Go binary + 一个本地 Codex app-server 子进程。

## 17. 官方参考

- [Tailscale：tsnet](https://tailscale.com/docs/features/tsnet)
- [Tailscale：tsnet.Server API](https://tailscale.com/docs/reference/tsnet-server-api)
- [Go package：tailscale.com/tsnet](https://pkg.go.dev/tailscale.com/tsnet)
