# Protocol V1.0 frozen decisions

- Package: `codex.remote.v1`; Go import: `github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1`.
- Transport: one ProtoJSON `Frame` per UTF-8 WebSocket text message, subprotocol `codex-remote.v1.protojson`.
- Endpoint: embedded `tsnet.Server.Listen("tcp", ":80")` at Tailnet-only `ws://<tsnet-node>/connect`; no WSS, `ListenTLS`, Serve, Funnel, host-network or public fallback.
- Version: accept exactly `{major:1, minor:0}`. Any other version closes with `CLOSE_CODE_PROTOCOL_VERSION_UNSUPPORTED`.
- Events: exactly eight V1 event variants: CodexUpdated, TurnUpdated, ItemStarted, ItemDelta, ItemUpdated, ItemCompleted, PendingRequestUpdated, WarningRaised.
- Completeness: `Completeness` is the common contract for truncated bytes and incomplete sources/collections. It is attached at Event, item, turn, session candidate, CurrentView, HistoryPage, user-input and audit/provenance boundaries. `Event.completeness` describes truncation of that live event payload itself; clients must not infer event-payload completeness only from a nested model or a later RESET.
- User input: questions, options and answers are structured and keyed by stable IDs. `RespondUserInputRequest.answers` is authoritative.
- Approval: resolved approvals preserve `resolved_decision`, including the distinction between ALLOW and ALLOW_FOR_SESSION.
- Rename: renamed file changes carry both `old_path` and `new_path`; `path` is a display/compatibility primary path.
- Provenance: item/turn snapshots carry a provenance kind, and diagnostic `AuditRecord` can carry typed `Provenance` plus `CanonicalActivity`, linking imported history, live wire, or Host-synthesized facts to source record IDs.
- Watch restart: a cursor is the pair `(after_host_run_id, after_event_seq)`. A request carrying `after_event_seq` must also carry the `ServerHello.host_run_id` that produced it; a missing run ID is `ERROR_CODE_INVALID_REQUEST`. A different run ID always produces `WATCH_MODE_RESET` plus `CurrentView` and `WATCH_RESET_REASON_HOST_RESTARTED`. A matching run ID follows same-run replay availability.
- Runtime restart: planned restart is allowed only without an active turn. V1 does not hot-recover an active turn or blindly replay `IN_PROGRESS`/unknown-result side effects.
- Audit: human-readable diagnostic evidence, not a compliance or tamper-proof log; no hash chain or per-record forced fsync requirement.

Generated code must only be changed through `.proto` plus `make proto-generate`.

## Deterministic behavior required by implementations

- Side-effect dedup compares a canonical fingerprint of the complete normalized operation payload, not only operation type/target. A reused ID with a different fingerprint returns `ERROR_CODE_CONFLICT`. Terminal success and error responses may be replayed; an `IN_PROGRESS`/unknown-result record is never blindly executed again after restart.
- A request whose deadline is already expired is rejected before dispatch. Once a side effect or turn has been submitted, only its single terminal Response is sent; deadline never implies `InterruptTurn` and never produces a second Response.
- A Watch Response is sent before replay/live Events. Rewatching the same Codex on one connection replaces that watch. RESET response head equals `reset_view.head_event_seq`; a matching-run cursor RESUMED replays `(after_event_seq, head_event_seq]` then live events.
- `chunk_seq` starts at 1 and is independent/contiguous per item across all delta variants. Duplicate chunks are ignored; a gap requires a Watch RESET. `ItemUpdated` is a complete item upsert, not a field merge.
- Response and causally related Event may arrive in either order; clients correlate using `caused_by_request_id` and must not infer success only from arrival order.
- `page_size=0` uses the Host default; values above the advertised maximum are clamped. Tokens are opaque and bound to the operation plus normalized query; malformed or mismatched tokens return `ERROR_CODE_INVALID_REQUEST`.
- `ImportSessionRequest.source` plus `session_id` identify a candidate; session IDs need not be globally unique across sources.
- Before ClientHello, any application Frame is invalid. Missing/wrong WebSocket subprotocol is rejected during upgrade. Duplicate hello, empty oneof, unknown fields/enums, binary messages, noncanonical 64-bit JSON representation, or oversized messages are protocol errors.
