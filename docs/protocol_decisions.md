# Protocol V1.1 frozen decisions

- Package: `codex.remote.v1`; Go import: `github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1` from [codex-remote-protocol](https://github.com/FireflyTang/codex-remote-protocol) `v1.1.0`. The exact tag, peeled source commit, and descriptor hash are pinned in the Host root [`protocol.lock`](../protocol.lock).
- Transport: one ProtoJSON `Frame` per UTF-8 WebSocket text message, subprotocol `codex-remote.v1.protojson`.
- Endpoint: embedded `tsnet.Server.Listen("tcp", ":80")` at Tailnet-only `ws://<tsnet-node>/connect`; no WSS, `ListenTLS`, Serve, Funnel, host-network or public fallback.
- Version: accept exactly `{major:1, minor:1}`. Any other version closes with `CLOSE_CODE_PROTOCOL_VERSION_UNSUPPORTED`.
- RPCs: the Host supports all 20 V1.1 request variants: 18 backend RPCs plus connection-scoped WatchCodex and UnwatchCodex. The six workspace RPCs are GetWorkspace, ListWorkspaceEntries, ReadWorkspaceTextFile, WriteWorkspaceTextFile, UploadWorkspaceEntry, and DownloadWorkspaceEntry.
- Events: the Host produces all nine V1.1 event variants: CodexUpdated, TurnUpdated, ItemStarted, ItemDelta, ItemUpdated, ItemCompleted, PendingRequestUpdated, WarningRaised, and WorkspaceAccessStateUpdated.
- Workspace: `Capabilities.workspace` is present with five strictly positive enforced limits. With the default 4 MiB frame they are 512 KiB text, 2 MiB inline upload, 2 MiB inline download, 32 MiB expanded archive data, and 1,000 archive entries; inline/text limits are reduced for smaller configured frames. Management state does not gate workspace access, including for `UNMANAGED` Codexes.
- Workspace mutation: GetWorkspace, RESET CurrentView, and WorkspaceAccessStateUpdated expose the same generation-based state. `generation` is persisted and monotonic; `active_agent_count` includes the parent and every subagent; a fresh opaque `quiescence_token` is required for mutation. Write/upload are rejected until all agents stop, while list/read/download remain available.
- Completeness: `Completeness` is the common contract for truncated bytes and incomplete sources/collections. It is attached at Event, item, turn, session candidate, CurrentView, HistoryPage, user-input and audit/provenance boundaries. `Event.completeness` describes truncation of that live event payload itself; clients must not infer event-payload completeness only from a nested model or a later RESET.
- User input: questions, options and answers are structured and keyed by stable IDs. `RespondUserInputRequest.answers` is authoritative.
- Approval: resolved approvals preserve `resolved_decision`, including the distinction between ALLOW and ALLOW_FOR_SESSION.
- Rename: renamed file changes carry both `old_path` and `new_path`; `path` is a display/compatibility primary path.
- Provenance: item/turn snapshots carry a provenance kind, and diagnostic `AuditRecord` can carry typed `Provenance` plus `CanonicalActivity`, linking imported history, live wire, or Host-synthesized facts to source record IDs.
- Watch restart: a cursor is the pair `(after_host_run_id, after_event_seq)`. A request carrying `after_event_seq` must also carry the `ServerHello.host_run_id` that produced it; a missing run ID is `ERROR_CODE_INVALID_REQUEST`. A different run ID always produces `WATCH_MODE_RESET` plus `CurrentView` and `WATCH_RESET_REASON_HOST_RESTARTED`. A matching run ID follows same-run replay availability.
- Runtime restart: planned restart is allowed only without an active turn. V1 does not hot-recover an active turn or blindly replay `IN_PROGRESS`/unknown-result side effects.
- Audit: human-readable diagnostic evidence, not a compliance or tamper-proof log; no hash chain or per-record forced fsync requirement.

The authoritative `.proto` files and generated artifacts are changed and released together in the public protocol repository. The Host consumes the pinned release and does not regenerate protocol code locally.

## Deterministic behavior required by implementations

- Side-effect dedup compares a canonical fingerprint of the complete normalized operation payload, not only operation type/target. This includes WriteWorkspaceTextFile and UploadWorkspaceEntry; the other four workspace RPCs are queries. A reused ID with a different fingerprint returns `ERROR_CODE_CONFLICT`. Terminal success and error responses may be replayed; an `IN_PROGRESS`/unknown-result record is never blindly executed again after restart.
- A request whose deadline is already expired is rejected before dispatch. Once a side effect or turn has been submitted, only its single terminal Response is sent; deadline never implies `InterruptTurn` and never produces a second Response.
- A Watch Response is sent before replay/live Events. Rewatching the same Codex on one connection replaces that watch. RESET response head equals `reset_view.head_event_seq`; a matching-run cursor RESUMED replays `(after_event_seq, head_event_seq]` then live events.
- `chunk_seq` starts at 1 and is independent/contiguous per item across all delta variants. Duplicate chunks are ignored; a gap requires a Watch RESET. `ItemUpdated` is a complete item upsert, not a field merge.
- Response and causally related Event may arrive in either order; clients correlate using `caused_by_request_id` and must not infer success only from arrival order.
- `page_size=0` uses the Host default; values above the advertised maximum are clamped. Tokens are opaque and bound to the operation plus normalized query; malformed or mismatched tokens return `ERROR_CODE_INVALID_REQUEST`.
- `ImportSessionRequest.source` plus `session_id` identify a candidate; session IDs need not be globally unique across sources.
- Before ClientHello, any application Frame is invalid. Missing/wrong WebSocket subprotocol is rejected during upgrade. Duplicate hello, empty oneof, unknown fields/enums, binary messages, noncanonical 64-bit JSON representation, or oversized messages are protocol errors.
