# Development status

Last updated: 2026-08-17 (Asia/Shanghai)

## Milestone status and scope

- Status: Linux Host personal Demo functionality is complete for the confirmed V1.0 scope.
- Acceptance: implementation, launched-Host formal-wire black-box validation, and independent read-only audit/remediation cycles are complete; no blocking finding remains for this milestone.
- Scope authority: `docs/decision_baseline.md`.
- Product boundary: embedded `tailscale.com/tsnet`, Tailnet-only plain WebSocket + ProtoJSON, Host-only repository. The client, production security/compliance/high-throughput work, WSS, and host-network/public fallback remain out of scope.

## Agent and file ownership used for the milestone

| Role | Responsibility | Final state |
|---|---|---|
| primary `/root` | decomposition, integration, acceptance | milestone accepted |
| protocol / external black-box owner | V1 proto, generated types, protocol decisions, deterministic external fixture and formal-wire suite | complete |
| Host / Gateway owner | Host composition, persistence, activity, audit, capability, Gateway and embedded tsnet | complete |
| runtime / adapter owner | Codex app-server UDS lifecycle and protocol adapter | complete |
| independent reviewers | protocol review plus read-only implementation audits | complete; findings remediated and retested |

The shared-worktree agents did not commit, push, or intentionally revert another owner's changes.

## Stable interfaces and dependencies

- Module: `github.com/kylin1993/codex-remote`.
- Protocol dependency: [codex-remote-protocol](https://github.com/FireflyTang/codex-remote-protocol) `v1.0.0`; Go import `github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1`. The exact source commit and descriptor hash are recorded in the Host root [`protocol.lock`](../protocol.lock).
- Direct dependencies: `tailscale.com v1.98.10`, `github.com/coder/websocket v1.8.15`, `modernc.org/sqlite v1.38.2`, `google.golang.org/protobuf v1.36.11`.
- Host project-local toolchain: Go 1.26.5 under ignored `.tools/`; no system configuration was changed. Buf 1.47.2 and protoc-gen-go 1.36.11 were used during the historical in-repository protocol milestone, before schema ownership moved to the public protocol repository.
- Production endpoint: embedded `tsnet.Server.Listen("tcp", ":80")`, exposed only as `ws://<tsnet-node>/connect` inside the Tailnet.

## Delivered functionality

- Frozen V1.0 ProtoJSON protocol with exactly 13 RPCs, eight canonical Event variants, paired HostRunID/event cursors, structured approval/user-input, provenance, diagnostic audit types, and common truncation/incompleteness contracts. A clipped live payload is marked on `Event.completeness` itself.
- Runnable Linux Host with embedded-tsnet production entry and an explicit loopback-only development seam used by external tests.
- Directory/session discovery, source-scoped import, Codex creation/listing, history, turn start/interrupt, approval and structured user-input responses.
- SQLite-backed Codex mapping, CurrentView, side-effect deduplication, resolved-pending tombstones, and atomic event-sequence/CurrentView commits.
- Watch initial RESET, same-run replay/live delivery, rewatch replacement, Host-restart RESET, heartbeat, explicit frame-limit close, and deterministic slow-consumer close.
- Stable synthetic identities for real plan/diff notifications that omit vendor item IDs; repeated plan updates upsert and plan/diff coexist.
- Bounded item, collection, warning, approval, user-input, history, CurrentView and live Event payloads with explicit completeness. Actionable question/option IDs, labels and multi-select semantics survive feasible clipping.
- One owned Codex app-server process over WebSocket/UDS, same-process adapter reconnect, bounded process restart, and no blind replay of unknown-result or in-progress side effects.
- Human-readable diagnostic JSONL for C/S wire, app-server wire, canonical activities, Host actions, runtime and Tailnet lifecycle with common correlation IDs. It is intentionally not a compliance or tamper-proof audit system.

## Test inventory

Static source inventory on 2026-08-17:

- 108 top-level `Test...` functions across the repository.
- 35 top-level tests under `tests/blackbox`; table-driven subtests are not included in this declaration count.
- The self-contained script launches the real Host plus a separately built deterministic fake app-server and exercises only the public WebSocket + ProtoJSON boundary.

The 35 external black-box tests are:

```text
TestAllThirteenRPCsHaveFormalWireCases
TestApprovalLifecycleAndDedup
TestApprovalPendingAppearsInReconnectReset
TestAuditWriteFailureDoesNotBlockBusiness
TestConcurrentRequestsCorrelateByRequestID
TestDiscoverImportAndContinueUnmanagedSession
TestEarlyLargeUpdatesSurviveStartResponseAndRemainActionable
TestExpiredDeadlineDoesNotDispatch
TestFailedTurnPreservesStatusErrorAndTime
TestHandshakeRejectsUnsupportedMinor
TestHandshakeRequiredBeforeRequest
TestHandshakeV10AndGetHost
TestHeartbeatPingPongKeepsConnectionUsable
TestHeartbeatTimeoutSendsProtocolClose
TestHostDiagnosticAuditContainsCorrelatedWireAndCanonicalEvidence
TestInboundOversizeGetsFormalFrameTooLargeClose
TestInterruptLifecycleAndDedup
TestInvalidAndBinaryFramesCloseConnection
TestLargeVendorOutputIsExplicitlyBounded
TestMultipleLargeItemsBoundCollectionsAndKeepConnectionUsable
TestNormalHostVerticalSlice
TestPageTokensAreBoundToOperationAndNormalizedQuery
TestPendingRestartCreate
TestPendingRestartDoesNotPretendRequestIsActionable
TestRestartCreateCompletedSession
TestRestartRestoresAndResetsWithoutReplay
TestRewatchResponsePrecedesReplacementStream
TestRuntimeRecoversAfterAppServerSocketDisconnect
TestSlowConsumerGetsExplicitProtocolClose
TestStructuredItemsDeltasAndHistory
TestStructuredUserInputLifecycle
TestSyntheticPlanDiffIDsAreStableAndUpserted
TestUpgradeRejectsMissingAndWrongSubprotocol
TestWatchValidatesCodexRequestIDAndDeadline
TestWrongPathDoesNotUpgrade
```

## Final validation evidence

The commands below are historical milestone evidence from 2026-08-17. The first `make check` predates extraction of the authoritative schema and generated artifacts into the public protocol repository; current Host builds do not generate protocol code locally.

```bash
make check
# buf lint + buf build + buf generate + go test ./...

.tools/go/bin/go test -count=1 -race ./...
.tools/go/bin/go vet ./...

bash scripts/blackbox.sh
# all normal and scenario-specific runs, restart create/verify, and pending-restart create/verify passed

CODEX_REMOTE_BLACKBOX_SCENARIOS='synthetic-upsert early-large' bash scripts/blackbox.sh
# passed three consecutive runs after Event.completeness and bounded multi-select remediation

CODEX_REMOTE_BLACKBOX_SCENARIOS='early-large' bash scripts/blackbox.sh
# passed with 800 plan steps, 1,200 file changes, large diff/warning, 6,000 approval argv,
# and 20 questions x 40 options under a 64 KiB formal frame budget
```

The installed Codex CLI 0.147.0 also passed the opt-in read-only smoke for real process startup, UDS WebSocket initialization and `thread/list`. No real user turn, approval or write operation was performed by that smoke.

## Independent audit result

Independent read-only reviews found protocol/run-cursor, source-scoped session, replay atomicity, lost-update, bounded-event, structured-item and external-test gaps during development. The owners fixed those findings, the external black-box owner independently reproduced the corrected behavior, and the final repository-wide check/race/vet/full-black-box gates are green. There is no blocking audit finding for the Demo milestone.

## Honest limits and deferred validation

- No live Tailscale control-plane login, `WhoIs`, or remote peer data-plane E2E was performed. Production wiring is implemented and unit-tested; the launched-Host black-box suite intentionally uses the explicit loopback development seam.
- Real Codex CLI coverage is read-only smoke only. The deterministic fake drives turn, approval, user-input, failure and restart behavior without touching a user's real session.
- Active turns and pending app-server RPC channels are not recovered across a Host process restart. Planned restart is allowed only when there is no active turn; stale pending state is cleared and disclosed as incomplete rather than pretending it is actionable.
- Persisted identity is source-scoped, but real-time upstream notifications carry only `threadId`. If two sources expose the same `threadId`, live notification routing cannot be unambiguously source-scoped without upstream support.
- If the minimum identities required to represent all actionable pending questions/options already exceed the negotiated frame limit, retaining every identity and emitting a conforming frame are mathematically incompatible. The Gateway's final behavior is explicit `FRAME_TOO_LARGE`; the tested large-but-feasible case retains all identities.
- Capability reporting covers the frozen baseline and observed source kinds, but available models, collaboration modes and approval policies are not yet a complete dynamically discovered catalog.
- Diagnostic audit provides basic rotation and correlation for troubleshooting. Rotation polish and compliance-grade retention/durability are non-core; no hash chain, tamper-proof guarantee, or per-record forced fsync is claimed.
- The client is not implemented in this repository.

## Blockers and remaining work

- Blockers: none for the confirmed Demo milestone.
- Optional follow-up: run the documented live-Tailnet/real-peer and more invasive real-Codex acceptance checks only when credentials, peers and disposable sessions are intentionally provided.
