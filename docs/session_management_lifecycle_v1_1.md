# Session / Codex Manage 生命周期 V1.1 需求基线

> 状态：**协议修改前需求稿，后端尚未实现**。
>
> 目标协议版本建议：`v1.1.0`。本文用于交付协议仓进行 schema 设计，并作为后续 Host 与黑盒验收的共同基线；字段编号、最终命名和兼容性检查由协议变更阶段确定。

## 1. 目标与非目标

本需求为 Host 管理的 Codex 引入可过期租约，避免长期无有效活动的 session 一直保持 managed，同时不丢失身份和历史。

- 每个 managed Codex 的租约为 2 小时。
- 连续 90 分钟没有有效活动后进入临期状态；此时距离租约到期还有 30 分钟。
- 到期且满足安全条件后，Host 自动将其转为 unmanaged。
- managed 与 unmanaged Codex 始终在同一个 `ListCodexes` 结果中保留显示。
- unmanage 不是删除、归档或 app-server unregister。
- 当前不要求显式 `ManageCodex` RPC；对 unmanaged Codex 调用 `StartTurn` 是唯一自动恢复管理的入口。

## 2. 两个正交状态维度

新增的管理状态与现有 `CodexStatus` 运行状态正交，不得互相替代或编码进同一个枚举。

### 2.1 管理状态

建议新增 `ManagementState`：

- `MANAGEMENT_STATE_UNSPECIFIED`
- `MANAGEMENT_STATE_MANAGED`
- `MANAGEMENT_STATE_EXPIRING_SOON`
- `MANAGEMENT_STATE_UNMANAGED`

### 2.2 运行状态

现有 `CodexStatus`（例如 `IDLE`、`RUNNING`、`WAITING_FOR_APPROVAL`、`WAITING_FOR_USER_INPUT`、`INTERRUPTING`、`UNAVAILABLE`、`ERROR`）继续描述运行事实。

例如，`EXPIRING_SOON + IDLE`、`MANAGED + RUNNING` 和 `UNMANAGED + IDLE` 都是合法组合。实现和 Client 不得用 `UNMANAGED` 推断 session 已删除，也不得用 `IDLE` 推断管理租约仍有效。

## 3. 租约与状态机

### 3.1 时间定义

- 租约长度：`120 分钟`。
- 临期阈值：自最近一次有效活动起 `90 分钟`，等价于 `managed_until_unix_ms - 30 分钟`。
- `managed_until_unix_ms` 是绝对 Unix 毫秒时间，不能因 Host 重启重新起算。
- 新建或导入并进入 managed 的 Codex，初始 deadline 为该操作生效时间加 2 小时。
- 一次有效续期把 deadline 更新为该活动生效时间加 2 小时，并开始新的租约周期。

### 3.2 状态转换

| 当前管理状态 | 触发条件 | 下一状态 | 必须行为 |
|---|---|---|---|
| `MANAGED` | 距 deadline 还有 30 分钟，且本周期尚无有效续期 | `EXPIRING_SOON` | 发出本租约周期唯一一次临期预警 |
| `MANAGED` | 白名单中的有效活动 | `MANAGED` | deadline 延后 2 小时 |
| `EXPIRING_SOON` | 白名单中的有效活动 | `MANAGED` | deadline 延后 2 小时，并开启新的预警周期 |
| `MANAGED` 或 `EXPIRING_SOON` | 合法手动 `UnmanageCodex`，且不 busy | `UNMANAGED` | 保留 Codex 身份、session 与 history |
| `EXPIRING_SOON` | deadline 到达，且 idle、无 active turn、无 pending | `UNMANAGED` | 自动安全 unmanage |
| `EXPIRING_SOON` | deadline 到达，但存在 active turn 或 pending | `EXPIRING_SOON` | 不 interrupt、不丢 pending；保持过期 deadline，条件解除后再次判断 |
| `UNMANAGED` | 合法 `StartTurn` 被接受 | `MANAGED` | 使用原 `codex_id` 自动恢复管理并建立新 2 小时租约 |

自动扫描允许存在很小的调度延迟，但语义判断必须以持久化的绝对 deadline 为准。测试应使用可控时钟，不依赖真实等待 2 小时。

### 3.3 自动 unmanage 的安全条件

自动 unmanage 只能在下列条件同时满足时发生：

1. 租约已到期；
2. 运行状态为 idle；
3. 没有 `active_turn_id`；
4. 没有 pending approval；
5. 没有 pending user-input request；
6. 没有正在完成的 interrupt 或其他等价 busy 状态。

若 deadline 到期时任一条件不满足，Host 不得 interrupt turn、自动回答 pending request 或伪造完成状态。它必须保留 `EXPIRING_SOON` 和原 deadline；条件解除后再安全转为 `UNMANAGED`。如果期间发生有效续期，则回到 `MANAGED` 并建立新 deadline。

## 4. 有效活动与续期白名单

只有以下行为可以续期：

1. `StartTurn`
2. `InterruptTurn`
3. `RespondApproval`
4. `RespondUserInput`
5. `Pong.foreground_codex_ids` 中列出的 Codex

其余读取、观察或连接保活一律不续期，包括但不限于：

- `WatchCodex` / `UnwatchCodex`
- `ListCodexes`
- `ListHistory`
- `GetHost`、目录与 session discovery/list
- 不带 foreground Codex 的普通 Ping/Pong 心跳
- 连接建立、Hello、重连和 Watch replay/RESET

续期只针对通过协议校验并被 Host 接受的有效操作。校验失败、找不到 Codex、冲突、busy 拒绝或 side-effect dedup 的历史响应重放不得产生第二次续期。`Pong.foreground_codex_ids` 只能续期当前已是 `MANAGED` 或 `EXPIRING_SOON` 的已知 Codex；它不能把 `UNMANAGED` Codex 重新变为 managed。

## 5. 手动 UnmanageCodex

建议新增 side-effect RPC `UnmanageCodex`，请求至少携带 `codex_id`，响应返回更新后的 `Codex` 和正常的 dedup 信息。

手动 unmanage 必须是显式且保守的：

- 若 Codex 有 active turn、pending approval、pending user-input、正在 interrupt 或处于其他 busy 状态，立即以新增的 `CODEX_BUSY` 错误拒绝。
- busy 拒绝不得隐式调用 `InterruptTurn`，不得排队等待 turn 结束，也不得清除或回答 pending request。
- 成功后保留原 `codex_id`、上游 session/thread 标识、CurrentView 与全部可用 history。
- 成功后仍能通过同一个 `ListCodexes` 查询到该 Codex，并明确显示 `UNMANAGED`。
- 本操作只改变 Host 的管理生命周期；不得宣称或假设存在 app-server unregister，也不要求调用未定义的上游注销能力。
- 对已经 `UNMANAGED` 的同一请求应遵循现有 side-effect dedup 规则，不产生新的身份或删除数据。

## 6. StartTurn 自动恢复

对一个已知的 `UNMANAGED` Codex 发起合法 `StartTurn` 时：

1. Host 使用持久化映射恢复同一个 session；
2. 管理状态转为 `MANAGED`；
3. 保持原 `codex_id`，不得创建替代 Codex 或要求 Client 重新 import；
4. 既有 history 保持连续可见；
5. 建立从本次有效活动起算的 2 小时租约；
6. 按正常 `StartTurn` 语义提交 turn，并通过已有 Response/Event 关联规则报告结果。

请求在校验或 dispatch 前失败时不得恢复管理或续期。请求已被 Host 接受并提交给 runtime 后，即使上游随后返回 turn 失败，管理恢复和本次有效活动仍成立；不得因业务 turn 失败换用新的 `codex_id`。

当前不新增显式 `ManageCodex` RPC。除 `StartTurn` 外，Watch、List、history、Pong foreground 以及其他 RPC 都不能把 `UNMANAGED` 恢复为 managed。

## 7. 预警语义

每个租约周期最多发出一次临期预警：

- 预警在进入 `EXPIRING_SOON` 时产生。
- 预警必须携带该周期的 `managed_until_unix_ms`。
- 建议新增明确的 warning reason/code，例如 `WARNING_CODE_MANAGEMENT_EXPIRING_SOON`，不得仅依赖自由文本判断。
- 预警通过现有 Warning/Event/CurrentView 机制可见，不新增第九种 Event variant。
- 同一 deadline 的定时扫描、重连、Watch、RESET 或 Host 重启不得重复产生预警。
- 有效续期产生新 deadline 和新租约周期；若新周期再次进入临期，可以再发一次预警。

实现必须持久化“当前 deadline 对应的预警是否已发出”或等价 generation，不能只依赖进程内存去重。

## 8. 持久化与重启

下列数据必须随 Codex 映射持久化，并跨 Host 重启恢复：

- `management_state`
- `managed_until_unix_ms`
- 当前租约周期/预警去重标记
- 原 `codex_id` 与 session/thread 映射
- CurrentView、history、active/pending 相关现有状态

重启期间的墙上时间计入租约。Host 恢复后：

- deadline 未到：恢复原 `MANAGED` 或 `EXPIRING_SOON` 状态，不延长租约；
- 已越过临期阈值但未到 deadline：进入或保持 `EXPIRING_SOON`，且同周期预警仍至多一次；
- deadline 已到且满足安全条件：转为 `UNMANAGED`；
- deadline 已到但仍 busy/pending：保持 `EXPIRING_SOON`，不得破坏运行状态。

状态、deadline 与预警去重信息的更新应与现有 Codex 持久化/事件提交保持一致，避免重启后出现状态回退、重复预警或丢失身份。

## 9. List、Watch 与可见性

- `ListCodexes` 继续返回 managed、expiring-soon 和 unmanaged Codex；本需求不拆分第二个列表。
- 每个返回的 `Codex` 明确携带管理状态。处于 managed/expiring-soon 时携带 `managed_until_unix_ms`；处于 unmanaged 时该值为 `0`。
- 原有排序和分页契约不得因为 management state 被静默改变；如未来新增筛选，必须是可选且默认包含全部状态。
- management state 或 deadline 改变时，应使用现有 `CodexUpdated` 与 CurrentView/RESET 机制让 watcher 收敛。
- Watch、replay、RESET 和 List 只传播状态，不续期。

## 10. 协议 V1.1 建议变更

建议在协议仓以 `v1.1.0` 作为 Protobuf schema 的 additive change，并通过协议仓的 breaking gate。这里的 additive 只表示字段编号和类型层面的 Protobuf 兼容，不表示当前 strict ProtoJSON 传输具备跨 minor 运行时兼容性。Host 与 Client 必须同时升级，并精确使用 `{major: 1, minor: 1}`；旧 V1.0 Client 不应连接 V1.1 Host，V1.1 也不保证旧 Client 能忽略新增 ProtoJSON 字段。

1. 新增 `ManagementState` enum。
2. 在 `Codex` 新增：
   - `management_state`
   - `managed_until_unix_ms`
3. 新增 `UnmanageCodexRequest` / `UnmanageCodexResponse`，并加入现有 Request/Response oneof。
4. 在 `Pong` 新增 `repeated string foreground_codex_ids`。
5. 在错误码中新增明确的 `CODEX_BUSY`。
6. 在 warning code/reason 中新增明确的 management-expiring-soon 原因；预警数据必须带 `managed_until_unix_ms`，可使用 typed field，或在保持现有 Warning 结构时使用规范化 metadata key。
7. 保持现有 8 类 Event，不为本需求增加新 Event variant。
8. 不新增显式 `ManageCodex` RPC。

最终字段编号必须只使用尚未占用的编号，通过协议仓的 Buf lint/build/breaking gate，并同步生成同一 tag 下的 Go artifact 与 descriptor。

## 11. 边界与失败语义

- `UNMANAGED` 不等于 session 不可恢复、不等于删除，也不改变 `codex_id`。
- management state 不代表 runtime 健康；`UNAVAILABLE`/`ERROR` 等运行状态仍独立报告。
- 时钟回拨不得让已经自动 unmanage 的 Codex自行回到 managed；只有合法 `StartTurn` 可以恢复。
- 批量 foreground ID 中的未知、重复或 unmanaged ID 不得恢复管理。协议阶段需固定是忽略单个无效 ID 还是拒绝整个 Pong；无论选择哪种，都不得部分产生未披露的管理恢复。
- `managed_until_unix_ms` 只由 Host 决定，Client 提供的时间戳不能覆盖 deadline。
- Client 断线本身不立即 unmanage；只按有效活动和绝对 deadline 判断。
- 自动/手动 unmanage 均不得删除 audit、history、CurrentView 或 side-effect dedup 记录。

## 12. 验收标准

后续实现至少满足以下确定性测试：

1. 新建/导入 Codex 后为 `MANAGED`，deadline 约为生效时间加 2 小时。
2. 可控时钟推进到 89:59 不预警；到 90:00 转为 `EXPIRING_SOON`，预警包含准确 deadline。
3. 同一租约周期多次扫描、Watch、重连和 Host 重启只产生一次预警。
4. 在临期阶段执行每一种白名单有效活动都会续期并回到 `MANAGED`；新 deadline 从活动生效时间起算。
5. Watch、ListCodexes、ListHistory、普通心跳和 replay 不改变 deadline。
6. `Pong.foreground_codex_ids` 只续期其中已 managed/expiring-soon 的已知 Codex；不恢复 unmanaged Codex。
7. idle、无 active turn、无 pending 的 Codex 到 2 小时后自动变为 `UNMANAGED`。
8. active turn、approval、user-input 或 interrupt 中的 Codex 到期后保持 `EXPIRING_SOON`，不被 interrupt；busy 解除后才自动 unmanage。
9. 对 busy Codex 手动 `UnmanageCodex` 返回 `CODEX_BUSY`，turn/pending 和 deadline 不被破坏。
10. 成功手动或自动 unmanage 后，原 Codex 仍出现在同一 `ListCodexes` 中，`codex_id`、session 映射和 history 不变。
11. 对 unmanaged Codex 执行合法 `StartTurn` 后，以相同 `codex_id` 恢复为 `MANAGED`，history 连续且 turn 正常关联。
12. 对 unmanaged Codex执行 Watch/List/ListHistory/Pong foreground 不得恢复管理。
13. Host 在 `MANAGED`、`EXPIRING_SOON`、已过期但 busy、`UNMANAGED` 各状态重启后，state/deadline/预警去重和身份均正确恢复，且 downtime 不赠送新租期。
14. List 分页同时包含不同 management state，排序和 token 语义保持现有契约。
15. 所有新增 schema 通过协议仓 lint/build/breaking/generate-clean；Host 与 Client 同时升级并以 exact minor `1.1` 完成握手。V1.0 Client 连接 V1.1 Host 必须按版本不匹配拒绝，不作为向后兼容验收路径。

验收必须覆盖持久化重启和正式 WebSocket + ProtoJSON 黑盒路径；仅有内存级单元测试不能证明本生命周期完成。
