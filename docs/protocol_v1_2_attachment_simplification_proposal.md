# Codex Remote Protocol v1.2 图片附件语义简化提案

> 状态：已由重新发布的协议 `v1.2.0` 采纳。本文保留为设计背景；实现与对外语义以协议仓当前 `v1.2.0` 为准。

## 1. 背景与目标

Codex Remote 当前定位为个人使用、功能优先的 Demo：它应优先保证图片附件主流程完整、重启后数据仍然可用，并避免为极窄崩溃窗口引入复杂的事务状态机。

v1.2 图片附件的基础目标是：

- 上传并持久化图片；
- 在 `StartTurn` 中按用户输入顺序混合发送文本与图片；
- 在实时事件与历史记录中返回一致的图片描述符；
- Host 重启以及 Codex 经历 Unmanage、Forget/re-import 后，附件仍可读取和再次使用。

当前附件设计中的 durable intent、附件 pin、上游结果对账和启动时全量扫描，对跨崩溃 exactly-once 语义提出了较高要求。建议将这些要求降级为可选增强能力，同时保留所有用户可见的主线功能。

## 2. 降级跨崩溃 exactly-once 要求

### 2.1 建议从基础合规要求中移除

以下机制建议不再作为 v1.2 基础实现的强制要求：

- 调用 native app-server 前持久化 pending `StartTurn` intent；
- 为 pending intent 建立附件 pin，并在完成后执行 pin/unpin 状态迁移；
- Host 重启后自动判断 app-server 是否已经接受结果不确定的 turn；
- 对结果不确定的请求自动完成、回滚或重放；
- 跨 Host 崩溃保证 `StartTurn` exactly-once。

### 2.2 建议保留的基础语义

基础实现仍应满足：

1. `UploadImageAttachment` 返回成功前，Host 必须持久化图片内容及其 immutable descriptor。
2. 正常执行 `StartTurn` 前，Host 必须校验附件存在、归属正确，并验证协议要求的 size/hash 等信息。
3. 正常成功路径中，Host 必须持久化 user message/turn 与附件 descriptor 的关联，使实时事件和历史记录能够返回相同附件。
4. 如果 Host 在 app-server 接受 `StartTurn` 前后崩溃，且重启后无法确认上游结果：
   - Host 不得自动重放该 `StartTurn`；
   - Host 可以将请求标记为 outcome unknown；
   - 客户端以相同 request ID 重试时，Host 可以返回 `CONFLICT` 或专门的 unknown-result 错误；
   - 相关附件必须继续保留，不得因为结果未知而立即删除。
5. 自动 reconcile、pending intent 和跨崩溃 exactly-once recovery 可以由 Host 自愿实现，但不影响基础协议合规性。

建议规范加入以下英文措辞：

> Protocol v1.2 guarantees durable uploaded attachments and normal-path turn/history association. It does not require exactly-once `StartTurn` recovery across a Host crash occurring between native app-server acceptance and Host-side finalization. A Host may conservatively report an unknown outcome and must not automatically replay the request.

## 3. 将附件 pin 与 GC 设为可选能力

v1.2 不应强制 Host 实现附件垃圾回收。Host 可以永久保留所有已成功上传的附件；在这种实现中，不需要单独的 pending pin/unpin 状态机。

如果 Host 选择实现 GC，则应满足：

- 已被历史记录引用的附件不得删除；
- 与 outcome unknown 请求关联的附件不得删除；
- 未引用附件至少保留 `unreferenced_retention_ms`；
- pin/unpin 可以作为 Host 内部实现细节，不需要暴露为协议状态。

`unreferenced_retention_ms` 应表示最短保留时间，而不是强制删除期限。Host 始终可以选择永久保留附件，因此无需为此修改现有 protobuf。

建议规范加入以下英文措辞：

> A Host may retain attachments indefinitely. Implementations that do not perform attachment garbage collection do not need a separate pending pin state. If garbage collection is implemented, referenced attachments and attachments associated with an unknown request outcome must not be collected.

## 4. 将启动全量扫描降级为按需校验

不建议要求 Host 每次启动前同步扫描、解码并验证全部历史附件。附件数量增长后，全量扫描可能显著拖慢启动，单个异常附件甚至可能让整个 Host 无法进入 READY。

建议改为：

- Host 启动时只检查 attachment store 与基本配置是否可访问；
- 在 `StartTurn`、`DownloadImageAttachment` 或其他实际读取路径中，按需校验附件的 size/hash；
- 单个历史附件超过当前 frame 限制或无法读取时，仅对应请求返回明确错误；
- Host 不应仅因单个历史附件暂时无法通过当前 frame 发送，就让整个服务保持 NOT_READY；
- 启动时全量验证可以作为可选严格模式。

建议规范加入以下英文措辞：

> A Host is not required to decode or validate every retained attachment before becoming ready. Attachment integrity and frame eligibility may be validated on access. Failure of an individual retained attachment must not, by itself, prevent the Host from becoming ready.

## 5. 明确 stable native-session identity

### 5.1 当前歧义

协议将附件归属于“stable native-session identity”，但尚未明确该 identity 的具体定义。对于尚未物化的 remote-created thread，这会产生真实歧义：

1. `CreateCodex` 在 app-server 中创建 thread；
2. 用户在首条消息发送前上传图片；
3. 此时 thread 尚无 rollout，尚未 materialize；
4. Host 重启后，app-server 可能返回 `no rollout found` 或 `thread not found`；
5. Host 为同一逻辑会话重新创建 native thread；
6. 新 thread ID 与上传图片时的 thread ID 不同。

如果附件 owner 直接等于 app-server thread ID，重建后原附件会失去归属。附件 owner 同样不能直接等于 `codex_id` 或 workspace path，因为 Forget/re-import 可能更换 `codex_id`，workspace 也不是附件生命周期边界。

### 5.2 建议定义 Host 持久化的 logical session owner

“Stable native-session identity”应定义为 Host 持久化的逻辑会话 owner，并满足：

- owner 独立于 `codex_id`、workspace path 和当前 app-server thread ID；
- Host 在 `CreateCodex` 或 `ImportSession` 时建立并持久化 owner；
- 对已物化 native session，Host 可以基于规范化的 `(source, session_id)` 建立 owner；
- 对尚未物化的 remote-created session，Host 生成并持久化自己的 owner key；
- 如果重启后必须创建新的 app-server thread，新 thread 必须重新绑定到原 logical session owner；
- Unmanage 不改变 owner；
- Forget 保留 owner 与附件；
- re-import 即使生成新的 `codex_id`，也必须重新绑定到原 owner；
- 附件始终归属于 logical session owner，因此在上述生命周期变化后仍可下载并用于 `StartTurn`。

logical session owner 可以完全作为 Host 内部持久化字段，不需要新增客户端可见的 protobuf 字段。

建议规范加入以下英文措辞：

> “Stable native-session identity” means a Host-persisted logical session owner. It is independent of `codex_id`, workspace path, and the current native app-server thread ID. For an unmaterialized remotely-created session, the Host must preserve the logical owner when replacing a lost native thread after restart. Unmanage and Forget/re-import must not change attachment ownership.

## 6. 应保留的协议能力

以下功能与约束应继续保留：

- `UploadImageAttachment` 和 `DownloadImageAttachment` RPC；
- `Capabilities.image_attachments`；
- `StartTurn` 中有序的文本/图片混合输入；
- 历史与实时事件中的完整 `ImageAttachment` descriptor；
- 附件不属于 workspace；
- filename 只用于展示，不作为文件路径；
- Upload request ID 去重；
- MIME、size 和 SHA-256 基本校验；
- Host 重启以及 Unmanage、Forget/re-import 后继续访问附件。

## 7. 不作为 v1.2 基础要求的能力

以下能力可以实现，但不应成为 v1.2 基础合规要求：

- 跨 Host 崩溃的 `StartTurn` exactly-once；
- pending intent 的自动上游对账；
- 复杂的附件 pin/unpin 状态机；
- 强制附件 GC；
- 启动时全量扫描、解码和验证全部保留附件。

## 8. 推荐的最小实现路径

```text
上传并持久化图片
→ StartTurn 按需校验附件并转换为 native localImage 输入
→ 正常成功后保存历史关联
→ 重启、Unmanage、Forget/re-import 后仍可读取和使用
```

对于 Host 恰好在 app-server 接受请求与 Host 本地完成持久化之间崩溃的极窄窗口，允许保守返回 outcome unknown，并禁止自动重放。该限制不影响正常流程，也不删除任何用户可见功能，更符合当前个人 Demo 的开发目标。
