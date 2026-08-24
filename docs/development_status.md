# Development status

Last updated: 2026-08-24 (Asia/Shanghai)

## Milestone status and scope

- Status: the Linux Host personal Demo now implements the V1.1 management lifecycle and bounded workspace access locally. Final integration acceptance for the expanded workspace scope is left to the primary agent; this document does not claim a Host release or live deployment.
- Acceptance: one complete self-contained launched-Host formal-wire black-box run passed on 2026-08-24 for the pre-workspace lifecycle/restart scope. It used isolated loopback test Hosts and did not restart or exercise a live Tailnet Host. A dedicated workspace formal-wire scenario is now present but is not retroactively included in that earlier evidence.
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
- Protocol dependency: [codex-remote-protocol](https://github.com/FireflyTang/codex-remote-protocol) `v1.1.0`; Go import `github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1`. The exact tag, peeled source commit, and descriptor hash are recorded in the Host root [`protocol.lock`](../protocol.lock).
- Direct dependencies: `tailscale.com v1.98.10`, `github.com/coder/websocket v1.8.15`, `modernc.org/sqlite v1.38.2`, `google.golang.org/protobuf v1.36.11`.
- Host project-local toolchain: Go 1.26.5 under ignored `.tools/`; no system configuration was changed. Buf 1.47.2 and protoc-gen-go 1.36.11 were used during the historical in-repository protocol milestone, before schema ownership moved to the public protocol repository.
- Production endpoint: embedded `tsnet.Server.Listen("tcp", ":80")`, exposed only as `ws://<tsnet-node>/connect` inside the Tailnet.

## Delivered functionality

- Published and pinned protocol V1.1.0 with 20 Host-supported RPCs, paired HostRunID/event cursors, structured approval/user-input, provenance, diagnostic audit types, and common truncation/incompleteness contracts. The Host now produces all nine Event families, including WorkspaceAccessStateUpdated.
- Persistent `MANAGED` / `EXPIRING_SOON` / `UNMANAGED` state, two-hour default leases, a typed warning 30 minutes before expiry, conservative manual/automatic `UnmanageCodex`, targeted foreground-Pong renewal, and same-`codex_id` restoration through `StartTurn`. Lease deadline and warning-cycle deduplication persist across Host restart.
- Lifecycle timing is configurable with `--lease-duration`, `--lease-warning-before`, and `--lease-sweep-interval` (defaults `2h`, `30m`, and `1m`).
- Six bounded workspace RPCs cover workspace discovery, paged listing, complete UTF-8 text reads, atomic text writes, inline regular-file/ZIP upload, and inline regular-file/ZIP download. `Capabilities.workspace` advertises five executable hard limits: by default 512 KiB text, 2 MiB inline upload, 2 MiB inline download, 32 MiB expanded archives, and 1,000 archive entries; inline/text limits contract conservatively with a smaller configured formal frame.
- Workspace mutation state is consistent across GetWorkspace, CurrentView RESET, and WorkspaceAccessStateUpdated. Persisted monotonic `generation`, `active_agent_count`, and opaque `quiescence_token` prevent mutation until the parent agent and every subagent have stopped. Read/list/download remain usable while busy, and `UNMANAGED` Codexes retain workspace access.
- Runnable Linux Host with embedded-tsnet production entry and an explicit loopback-only development seam used by external tests.
- Directory/session discovery, source-scoped import, Codex creation/listing, history, turn start/interrupt, approval and structured user-input responses.
- SQLite-backed Codex mapping, CurrentView, side-effect deduplication, resolved-pending tombstones, and atomic event-sequence/CurrentView commits.
- Watch initial RESET, same-run replay/live delivery, rewatch replacement, Host-restart RESET, heartbeat, explicit frame-limit close, and deterministic slow-consumer close.
- Stable synthetic identities for real plan/diff notifications that omit vendor item IDs; repeated plan updates upsert and plan/diff coexist.
- Bounded item, collection, warning, approval, user-input, history, CurrentView and live Event payloads with explicit completeness. Actionable question/option IDs, labels and multi-select semantics survive feasible clipping.
- One owned Codex app-server process over WebSocket/UDS, same-process adapter reconnect, bounded process restart, and no blind replay of unknown-result or in-progress side effects.
- Human-readable diagnostic JSONL for C/S wire, app-server wire, canonical activities, Host actions, runtime and Tailnet lifecycle with common correlation IDs. It is intentionally not a compliance or tamper-proof audit system.

## Test inventory

Static source inventory on 2026-08-24:

- 165 top-level `Test...` functions across the repository.
- 42 top-level tests under `tests/blackbox`; table-driven subtests are not included in this declaration count.
- The self-contained script launches the real Host plus a separately built deterministic fake app-server and exercises only the public WebSocket + ProtoJSON boundary.

The 42 external black-box tests are:

```text
TestAllTwentyRPCsHaveFormalWireCases
TestApprovalLifecycleAndDedup
TestApprovalPendingAppearsInReconnectReset
TestAuditWriteFailureDoesNotBlockBusiness
TestConcurrentRequestsCorrelateByRequestID
TestDiscoverImportAndContinueUnmanagedSession
TestEarlyLargeUpdatesSurviveStartResponseAndRemainActionable
TestExpiredDeadlineDoesNotDispatch
TestFailedTurnPreservesStatusErrorAndTime
TestHandshakeRejectsUnsupportedMinors
TestHandshakeRequiredBeforeRequest
TestHandshakeV11AndGetHost
TestHeartbeatPingPongKeepsConnectionUsable
TestHeartbeatTimeoutSendsProtocolClose
TestHostDiagnosticAuditContainsCorrelatedWireAndCanonicalEvidence
TestInboundOversizeGetsFormalFrameTooLargeClose
TestInterruptLifecycleAndDedup
TestInvalidAndBinaryFramesCloseConnection
TestLargeVendorOutputIsExplicitlyBounded
TestManagementLifecycleOverFormalWire
TestMultipleLargeItemsBoundCollectionsAndKeepConnectionUsable
TestNormalHostVerticalSlice
TestPageTokensAreBoundToOperationAndNormalizedQuery
TestPendingRestartCreate
TestPendingRestartDoesNotPretendRequestIsActionable
TestRestartAutomaticThreadNameSurvivesReset
TestRestartAutomaticThreadNameUpdatesAndPersists
TestRestartCreateCompletedSession
TestRestartLifecycleCreate
TestRestartLifecyclePreservesDeadlineUnmanagedAndWarningDedup
TestRestartRestoresAndResetsWithoutReplay
TestRewatchResponsePrecedesReplacementStream
TestRuntimeRecoversAfterAppServerSocketDisconnect
TestSlowConsumerGetsExplicitProtocolClose
TestStructuredItemsDeltasAndHistory
TestStructuredUserInputLifecycle
TestSyntheticPlanDiffIDsAreStableAndUpserted
TestUnmaterializedCreatedThreadHasEmptyHistoryUntilFirstUserMessage
TestUpgradeRejectsMissingAndWrongSubprotocol
TestWatchValidatesCodexRequestIDAndDeadline
TestWorkspaceFormalWireScenario
TestWrongPathDoesNotUpgrade
```

## Final validation evidence

Pre-workspace lifecycle/restart evidence from 2026-08-24:

```bash
make check
bash scripts/blackbox.sh
# one complete self-contained run passed, including lifecycle and restart phases
```

The black-box command launches disposable loopback test Hosts and fake app-server processes. It does not demonstrate that an already-running live Tailnet Host was restarted or upgraded. The workspace scenario was added after the complete run cited above; final expanded-scope integration evidence must come from a new run rather than from this historical result.

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

Independent read-only reviews found protocol/run-cursor, source-scoped session, replay atomicity, lost-update, bounded-event, structured-item and external-test gaps during the earlier milestone. The owners fixed those findings and the external black-box owner independently reproduced the corrected lifecycle behavior. Final audit and expanded full-black-box acceptance for workspace remain with the primary agent.

## Honest limits and deferred validation

- No live Tailscale control-plane login, `WhoIs`, or remote peer data-plane E2E was performed. Production wiring is implemented and unit-tested; the launched-Host black-box suite intentionally uses the explicit loopback development seam.
- Real Codex CLI coverage is read-only smoke only. The deterministic fake drives turn, approval, user-input, failure and restart behavior without touching a user's real session.
- Active turns and pending app-server RPC channels are not recovered across a Host process restart. Planned restart is allowed only when there is no active turn; stale pending state is cleared and disclosed as incomplete rather than pretending it is actionable.
- Persisted identity is source-scoped, but real-time upstream notifications carry only `threadId`. If two sources expose the same `threadId`, live notification routing cannot be unambiguously source-scoped without upstream support.
- If the minimum identities required to represent all actionable pending questions/options already exceed the negotiated frame limit, retaining every identity and emitting a conforming frame are mathematically incompatible. The Gateway's final behavior is explicit `FRAME_TOO_LARGE`; the tested large-but-feasible case retains all identities.
- Capability reporting covers the frozen baseline and observed source kinds, but available models, collaboration modes and approval policies are not yet a complete dynamically discovered catalog.
- Diagnostic audit provides basic rotation and correlation for troubleshooting. Rotation polish and compliance-grade retention/durability are non-core; no hash chain, tamper-proof guarantee, or per-record forced fsync is claimed.
- V1.1 workspace transfer is deliberately bounded and inline. It has no chunking, resumability, multipart transport, or large-file confirmation; operations over the advertised limits return typed errors.
- After a deterministic workspace state/event commit failure that occurs after the filesystem mutation, the Host best-effort restores the original content/tree, but the opaque revision may change; clients must re-read before retrying. An indeterminate commit or crash window is conservatively reported as outcome unknown.
- The client is not implemented in this repository.

## Blockers and remaining work

- Blockers: none for the confirmed Demo milestone.
- Optional follow-up: run the documented live-Tailnet/real-peer and more invasive real-Codex acceptance checks only when credentials, peers and disposable sessions are intentionally provided.
