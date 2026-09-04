# Host black-box test matrix

Boundary rule: tests use only the formal WebSocket + ProtoJSON endpoint and generated public protocol types. They do not import `internal/**`, call Go implementation functions, or inspect SQLite. The deterministic app-server is a separately built external process. The launch script starts the real Host through its loopback-only development listener; production remains embedded-tsnet-only.

Current V1.2.0 evidence (2026-09-04): one complete self-contained `bash scripts/blackbox.sh` run exited 0. It passed the image attachment normal-path create and restarted-Host verify phases plus every prior scenario. The 49 top-level black-box tests use disposable loopback test Hosts; live Tailnet deployment was verified separately with health 200, WebSocket Upgrade 101, and a real V1.2.0 ServerHello.

Follow-up scoped evidence: `CODEX_REMOTE_BLACKBOX_SCENARIOS='rename-forget' bash scripts/blackbox.sh` passed. It is only scoped Rename/Forget evidence, not a repeated complete-suite claim.

| Area | Exercised behavior | Current result |
|---|---|---|
| Upgrade/Hello | correct, missing, and wrong subprotocol; wrong path; exact 1.2.0 including required patch; unsupported versions; request before Hello; Host/HostRun/connection identities | pass |
| ProtoJSON | text-only; invalid JSON; empty frame; unknown field; binary frame; inbound oversized frame receives formal `FRAME_TOO_LARGE` Close | pass |
| 24 Host RPCs | every Host-supported request variant is constructible; formal-wire cases include RenameCodex, ForgetCodex, all six workspace RPCs, UploadImageAttachment, and DownloadImageAttachment | pass |
| Multiplex | 32 concurrent outstanding request IDs, arbitrary response/event ordering, expired deadline | pass |
| Directory/session | pagination clamp/token/error; tokens bound to RPC and normalized query/cwd; discover real unmanaged `exec` sessions; reject source mismatch; import by `(source, session_id)` and continue the imported thread | pass |
| Turn/items | all 10 Event families produced by the Host, including CodexForgotten and WorkspaceAccessStateUpdated; structured user/reasoning/plan/command/tool/file-change/agent items; ordered text/image user-message parts; command stderr and reasoning/file deltas; failed/interrupted/completed status; Unix-ms times, failure, provenance, complete/incomplete history | pass |
| Synthetic plan/diff | fixture uses the real notification shape with no vendor `itemId`; generated IDs are nonempty, stable across repeated plan updates, distinct by type, upsert to one plan, and preserve plan plus diff together in RESET | pass; three consecutive formal-WS runs |
| Pre-response state | fixture emits plan, diff, file change, warning, approval, and user input before the app-server `turn/start` response; a fresh RESET retains the active turn and both actionable pending identities | pass; guards the StartTurn lost-update window |
| Approval | pending CurrentView across reconnect; resolved decision; cross-Codex response rejected; same-payload dedup; changed-payload conflict; terminal CAS loser | pass |
| User input | stable structured question/option IDs and answers; resolved state retains original questions and authoritative answers; cross-Codex response rejected; pending CurrentView; dedup/conflict/CAS | pass |
| Side-effect dedup | all 12 mutations—CreateCodex, ImportSession, StartTurn, InterruptTurn, RespondApproval, RespondUserInput, UnmanageCodex, RenameCodex, ForgetCodex, WriteWorkspaceTextFile, UploadWorkspaceEntry, UploadImageAttachment—use the shared request-ID dedup route; formal-wire image coverage includes same-payload replay and changed-payload conflict | pass |
| Management lifecycle | orthogonal management/runtime state; two-hour production default; 30-minute warning threshold; manual/automatic unmanage; busy rejection without interrupt; passive calls do not renew; targeted foreground Pong renews eligible Codexes only; StartTurn restores the same Codex/session/history | pass over formal wire with accelerated test timing |
| Rename / forget | persistent manual title wins over later native automatic title, including restart; Forget rejects managed Codexes, emits terminal CodexForgotten, removes Host listing/view, and replays by request ID. Persisted forgotten candidates merge with runtime discovery as `RESUMABLE` without a managed ID; materialized reimport has a new Codex ID with the same native thread/history, while same-run unmaterialized reimport reuses the old thread and `StartTurn` materializes it without a fabricated rollout. After restart, genuine upstream thread-not-found creates a new native thread in the stored CWD. | pass |
| Watch/reconnect | initial RESET; unknown Codex error; empty request ID and expired Watch/Unwatch rejected; live/unwatch; paired cursor; replacement Response-first with no stale event; restart RESET | pass |
| Restart | external fake JSON persistence; stable Host/Codex/history/dedup; new HostRunID; old paired cursor RESET; lease deadlines are not gifted time; unmanaged state and per-deadline warning dedup survive; unrecoverable pending is cleared and disclosed incomplete | pass |
| Heartbeat | advertised timing; Ping/Pong keeps connection usable; timeout sends application Close and allows reconnect | pass |
| Audit | parse ProtoJSON JSONL; validate record/run/sequence/time and CS/app-server source IDs on canonical records; force journal rotation failure and require business RPCs continue while degradation is logged | pass |
| Pagination/completeness | one ~260 KiB item and eight independently large items under a 64 KiB budget; require Item/Turn/History/CurrentView completeness, collection trimming, and usable connection | pass |
| Large structured events | 800 plan steps, 1,200 file changes, a large diff and vendor warning, 6,000 approval command arguments, and 20 questions with 40 options each; every observed Event stays within the advertised 64 KiB frame and plan/diff/file/warning/approval/input each carry their own explicit `Event.completeness` | pass; nested completeness and RESET completeness are checked independently |
| Bounded structured input | the truncated live Event and RESET retain all 20 question IDs, all 800 option IDs and nonempty selectable labels; the final question retains `allows_multiple`, two late option IDs can be selected, and the resolved response echoes that exact multi-select answer | pass; proves actionability beyond first-question free-form fallback |
| Slow consumer | create/unwatch, retain 500 ~65 KiB deltas, then replay from a paired cursor with send queue 1; pause reader and require application `Close=SLOW_CONSUMER` plus reconnect | pass; deterministic replay pressure avoids depending on runtime/SQLite production speed |
| Runtime recovery | fixture closes the app-server WebSocket after StartTurn while its HTTP/UDS process remains; after supervisor detection require READY and a successful new app-server RPC | pass |
| Workspace | six workspace RPCs; five nonzero advertised/enforced limits; paged listing; complete UTF-8 read; atomic create/replace/upsert; strong revision and quiescence preconditions; regular-file/ZIP upload/download; typed path/archive/size errors; generation consistency across Get/RESET/Event; parent-agent busy rejection; read-only access while busy; mutation after the parent stops; unmanaged access; write/upload dedup | pass |
| Image attachments V1.2.0 | advertised PNG/JPEG/GIF and frame-aware limit; immutable upload/download descriptor and bytes; owner isolation; ordered text/image/text StartTurn; realtime/history descriptor parity without private paths; upload dedup/conflict; persistence through Unmanage, Forget/re-import, and Host restart | pass in focused two-phase scenario |

The formal workspace scenario exercises the parent-agent busy/terminal transition. Independent parent and multiple-subagent contribution to `active_agent_count`, including restart restoration of the latest child states, is covered by implementation-level Codex tests because the deterministic external fixture does not spawn real collaboration children.

Forgotten candidates are appended only to the terminal runtime-discovery page. Because there is no composite cursor across runtime and persisted candidates, that terminal response can exceed the requested `page_size`; this is a Demo known limit.

Run the complete self-contained matrix:

```bash
bash scripts/blackbox.sh
```

Run only limit/backpressure scenarios:

```bash
CODEX_REMOTE_BLACKBOX_SCENARIOS='sessions structured failed rewatch multi-large audit-failure runtime-disconnect' bash scripts/blackbox.sh
```

Run only the workspace scenario:

```bash
CODEX_REMOTE_BLACKBOX_SCENARIOS='workspace' bash scripts/blackbox.sh
```

Run the image normal-path and restarted-Host phases:

```bash
CODEX_REMOTE_BLACKBOX_SCENARIOS='image-attachments' bash scripts/blackbox.sh
```

The script builds the Host and fake fixture, creates isolated state/audit/workspace directories, waits for `/healthz` with proxy bypass, exports the formal WS URL, and cleans up every child process. A nonzero exit from any scenario is retained while later scenarios continue, so one blocker does not hide the rest.

The event-sequence/CurrentView persistence failure boundary requires an injected persistence implementation and is therefore covered by implementation-level activity/persistence tests rather than this external fixture. The black-box suite still verifies recovery after audit-journal failure, an app-server UDS disconnect, heartbeat timeout, and explicit frame/slow-consumer closes, with a successful business RPC after each recoverable failure.
