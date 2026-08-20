# Codex Remote 当前决策基线

本文件记录用户最后确认的实现边界。它覆盖旧文档中的冲突表述，但不删除旧文档已经列出的产品功能。协议、实现、测试和审计均以此为准。

1. **功能保留**：现有 Markdown 已列出的产品功能默认全部保留，只有用户明确批准后才能删除。此项覆盖任何因“Demo 简化”而擅自删减 session、turn、审批、user-input、Watch、history、诊断等功能的解释。
2. **Host-only**：当前仓库只实现 Linux Host；客户端实现不在本仓。协议 IDL 仍完整定义 Client/Host 边界，以便真实客户端生成类型和黑盒验收。此项覆盖旧文档中可能让人误以为本仓同时交付 Client 的表述。
3. **embedded tsnet 是核心**：Host 直接嵌入 `tailscale.com/tsnet`，使用公共 Tailscale control plane；不依赖系统 `tailscaled`，不使用 Headscale、Tailscale Serve、Funnel、自建 relay、宿主机 Tailscale IP 或宿主机 listener。此项覆盖早期的系统 Tailscale、Serve 或普通网卡绑定方案。
4. **唯一正式入口**：正式 listener 是 `tsnet.Server.Listen("tcp", ":80")`，业务端点是 Tailnet-only plain `ws://<tsnet-node>/connect`。不使用 `ListenTLS`、WSS、证书、LAN 或 public fallback。Tailnet/WireGuard 是网络保护层。此项覆盖所有 WSS、`ListenTLS`、证书终止和宿主机监听表述。
5. **个人 Demo 优先级**：这是个人使用 Demo，功能正确和可排错优先；不做生产安全体系、多租户、合规和高吞吐优化。但不能以简化为由牺牲 session 身份/历史/继续语义、断线重连、pending approval/user-input 或正式协议边界。此项覆盖旧文档中的生产级加固和严格规模目标。
6. **诊断审计**：保留 C/S raw/decoded、app-server raw、canonical activity、Host action/RPC、runtime、Tailnet 生命周期及共同关联 ID，用于开发和用户排错。采用易读 JSONL、基本轮转/导出和明确的 incomplete/truncated 标记；不做 hash chain、防篡改证明、法规保留、合规耐久或高频逐条强制 `fsync`。审计失败必须可见，但不要求按合规系统方式阻断所有业务。此项覆盖旧文档中“审计真源可重建一切”“关键副作用必须强同步落盘”等过严表述。
7. **重启边界**：只允许在没有 active turn 时计划重启。Host/runtime 重启后，复用 tsnet node identity，重新发现并恢复 `codex_id` 映射、session/history 可见性、pending 状态及可继续状态；Watch 必须返回 `RESET + CurrentView`。不做 active-turn 热恢复，不盲重放 `IN_PROGRESS` 操作或未确认副作用。此项覆盖任何宣称运行中 turn 能跨进程无缝续跑的旧表述。
   Watch cursor 必须同时保存 `after_event_seq` 与产生它的 `after_host_run_id`；缺少 run ID 不得仅凭 event sequence 猜测是否同一次 Host 运行，不同 run 必须 `HOST_RESTARTED RESET`。
8. **协议冻结**：允许修订 V1 proto 来补齐明确契约缺口；当前只精确支持协议 `1.0`，不实现多-minor 协商兼容。V1 保持 8 类 Event，并包含 canonical/provenance、结构化 user-input、通用 completeness、approval resolved decision 与 rename old/new path。此项覆盖旧的“协商最低 minor”和缺字段表述。
9. **客户端边界**：客户端不在本仓；Host 仍必须提供足够稳定、可生成且可由外部黑盒客户端验证的 WS + ProtoJSON 契约。此项覆盖客户端 UI、IndexedDB 或客户端交付要求。
10. **开发验收机制**：实质实现由有明确文件所有权的 implementation subagents 完成；独立黑盒测试只能通过正式 WS + ProtoJSON 接口；实现完成后由未参与实现的独立 agent 审计，修复阻塞项后重跑相关测试和完整验收。不得以实现者自己的单元测试直接宣布完成。此项覆盖任何省略独立测试/审计阶段的流程。

若其他设计文档与本文件冲突，应先同步修正文档和 `.proto`，不得用旧表述绕过以上基线。
