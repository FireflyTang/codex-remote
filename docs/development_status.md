# Development status

Last updated: 2026-08-25 (Asia/Shanghai)

## Milestone status and scope

- Status: the Linux Host personal Demo implements the V1.1.2 management lifecycle, Rename/Forget, and bounded workspace access, with expanded integration acceptance complete.
- Acceptance: one complete self-contained launched-Host formal-wire `bash scripts/blackbox.sh` run exited 0 on 2026-08-25. It uses disposable loopback test Hosts; no claim is made that a live Tailnet Host was upgraded or restarted.
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
- Protocol dependency: [codex-remote-protocol](https://github.com/FireflyTang/codex-remote-protocol) exact `v1.1.2`; Go import `github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1`. ClientHello and ServerHello use exact `{major:1, minor:1, patch:2}` with no fallback. The exact tag, peeled source commit, and descriptor hash are recorded in the Host root [`protocol.lock`](../protocol.lock).
- Direct dependencies: `tailscale.com v1.98.10`, `github.com/coder/websocket v1.8.15`, `modernc.org/sqlite v1.38.2`, `google.golang.org/protobuf v1.36.11`.
- Host project-local toolchain: Go 1.26.5 under ignored `.tools/`; no system configuration was changed. Buf 1.47.2 and protoc-gen-go 1.36.11 were used during the historical in-repository protocol milestone, before schema ownership moved to the public protocol repository.
- Production endpoint: embedded `tsnet.Server.Listen("tcp", ":80")`, exposed only as `ws://<tsnet-node>/connect` inside the Tailnet.

## Delivered functionality

- Published and pinned protocol V1.1.2 with 22 Host-supported RPCs, paired HostRunID/event cursors, structured approval/user-input, provenance, diagnostic audit types, and common truncation/incompleteness contracts. The Host produces all 10 Event families, including CodexForgotten and WorkspaceAccessStateUpdated.
- Persistent `MANAGED` / `EXPIRING_SOON` / `UNMANAGED` state, two-hour default leases, a typed warning 30 minutes before expiry, conservative manual/automatic `UnmanageCodex`, targeted foreground-Pong renewal, and same-`codex_id` restoration through `StartTurn`. Lease deadline and warning-cycle deduplication persist across Host restart.
- Lifecycle timing is configurable with `--lease-duration`, `--lease-warning-before`, and `--lease-sweep-interval` (defaults `2h`, `30m`, and `1m`).
- RenameCodex changes only the persistent Host display title and establishes a manual override against later native automatic-title updates, including after restart. ForgetCodex is valid only for `UNMANAGED`; it removes the Host mapping, CurrentView/list-title state, and workspace registration while retaining native rollout/CWD, diagnostic audit, request dedup, and a persisted forgotten candidate. Discovery merges that candidate with runtime results as `RESUMABLE` with no `managed_codex_id`. A materialized reimport gets a new `codex_id` while retaining the native thread/history. Same-run unmaterialized reimport reuses the old thread and `StartTurn` materializes it without writing a fabricated rollout; after restart, a genuine upstream thread-not-found result instead creates a new native thread in the stored CWD.
- Six bounded workspace RPCs cover workspace discovery, paged listing, complete UTF-8 text reads, atomic text writes, inline regular-file/ZIP upload, and inline regular-file/ZIP download. `Capabilities.workspace` advertises five executable hard limits: by default 512 KiB text, 2 MiB inline upload, 2 MiB inline download, 32 MiB expanded archives, and 1,000 archive entries; inline/text limits contract conservatively with a smaller configured formal frame.
- Workspace mutation state is consistent across GetWorkspace, CurrentView RESET, and WorkspaceAccessStateUpdated. Persisted monotonic `generation`, `active_agent_count`, and opaque `quiescence_token` prevent mutation until the parent agent and every subagent have stopped. Read/list/download remain usable while busy, and `UNMANAGED` Codexes retain workspace access.
- Runnable Linux Host with embedded-tsnet production entry and an explicit loopback-only development seam used by external tests.
- Directory/session discovery, source-scoped import, Codex creation/listing, history, turn start/interrupt, approval and structured user-input responses.
- SQLite-backed Codex mapping, CurrentView, 11 side-effect dedup paths, resolved-pending tombstones, and atomic event-sequence/CurrentView commits.
- Watch initial RESET, same-run replay/live delivery, rewatch replacement, Host-restart RESET, heartbeat, explicit frame-limit close, and deterministic slow-consumer close.
- Stable synthetic identities for real plan/diff notifications that omit vendor item IDs; repeated plan updates upsert and plan/diff coexist.
- Bounded item, collection, warning, approval, user-input, history, CurrentView and live Event payloads with explicit completeness. Actionable question/option IDs, labels and multi-select semantics survive feasible clipping.
- One owned Codex app-server process over WebSocket/UDS, same-process adapter reconnect, bounded process restart, and no blind replay of unknown-result or in-progress side effects.
- Human-readable diagnostic JSONL for C/S wire, app-server wire, canonical activities, Host actions, runtime and Tailnet lifecycle with common correlation IDs. It is intentionally not a compliance or tamper-proof audit system.

## Test inventory

Static source inventory on 2026-08-25, calculated with `go test -list '^Test'`:

- 190 top-level `Test...` functions across the repository.
- 47 top-level tests under `tests/blackbox`; table-driven subtests are not included in this declaration count.
- The self-contained script launches the real Host plus a separately built deterministic fake app-server and exercises only the public WebSocket + ProtoJSON boundary.

The current black-box inventory includes exact V1.1.2 handshake, all-22-RPC constructibility, Rename/Forget, manual-title restart, lifecycle restart, unmaterialized lifecycle, and workspace responsiveness coverage.

## Final validation evidence

Current V1.1.2 lifecycle/workspace evidence from 2026-08-25:

```bash
make check
bash scripts/blackbox.sh
# one complete self-contained run exited 0, including Rename/Forget, workspace, lifecycle, and restart phases
```

The black-box command launches disposable loopback test Hosts and fake app-server processes. It is the complete 2026-08-25 formal-wire acceptance evidence and does not assert a live Tailnet Host upgrade or restart.

The follow-up `CODEX_REMOTE_BLACKBOX_SCENARIOS='rename-forget' bash scripts/blackbox.sh` scope passed. This is scoped Rename/Forget evidence, not a second claim that the complete suite was repeated.

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

Independent read-only reviews found protocol/run-cursor, source-scoped session, replay atomicity, lost-update, bounded-event, structured-item and external-test gaps during development. The owners fixed those findings, and the complete expanded lifecycle/workspace black-box run passed.

## Honest limits and deferred validation

- Full remote-peer mutation flows and invasive real-Codex workspace writes have not been exercised on a live Tailnet Host; the comprehensive deterministic suite uses the explicit loopback development seam.
- Real Codex CLI coverage is read-only smoke only. The deterministic fake drives turn, approval, user-input, failure and restart behavior without touching a user's real session.
- Active turns and pending app-server RPC channels are not recovered across a Host process restart. Planned restart is allowed only when there is no active turn; stale pending state is cleared and disclosed as incomplete rather than pretending it is actionable.
- Persisted identity is source-scoped, but real-time upstream notifications carry only `threadId`. If two sources expose the same `threadId`, live notification routing cannot be unambiguously source-scoped without upstream support.
- If the minimum identities required to represent all actionable pending questions/options already exceed the negotiated frame limit, retaining every identity and emitting a conforming frame are mathematically incompatible. The Gateway's final behavior is explicit `FRAME_TOO_LARGE`; the tested large-but-feasible case retains all identities.
- Capability reporting covers the frozen baseline and observed source kinds, but available models, collaboration modes and approval policies are not yet a complete dynamically discovered catalog.
- Diagnostic audit provides basic rotation and correlation for troubleshooting. Rotation polish and compliance-grade retention/durability are non-core; no hash chain, tamper-proof guarantee, or per-record forced fsync is claimed.
- V1.1.2 workspace transfer is deliberately bounded and inline. It has no chunking, resumability, multipart transport, or large-file confirmation; operations over the advertised limits return typed errors.
- After a deterministic workspace state/event commit failure that occurs after the filesystem mutation, the Host best-effort restores the original content/tree, but the opaque revision may change; clients must re-read before retrying. An indeterminate commit or crash window is conservatively reported as outcome unknown.
- Dispatcher currently invokes a side-effect Backend before recording its terminal dedup response. A Host crash after Backend success and before `CompleteRequest` leaves that request outcome unknown; this Demo does not claim exactly-once recovery for that narrow interval.
- Forgotten candidates are merged only onto the terminal native-discovery page. Because the cursor covers the runtime source only, that final page can exceed the requested `page_size`; this Demo deliberately does not implement a composite cursor.
- The client is not implemented in this repository.

## Blockers and remaining work

- Blockers: none for the confirmed Demo milestone.
- Optional follow-up: run more invasive live-Tailnet/real-peer and real-Codex workspace mutation checks only when credentials, peers, and disposable sessions are intentionally provided.
