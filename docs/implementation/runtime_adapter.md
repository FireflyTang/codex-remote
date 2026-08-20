# Runtime / Adapter implementation status

## Scope and dependency boundary

This module owns one long-running Codex app-server process and its Unix-socket RPC connection. It has no dependency on tsnet, Gateway HTTP/WebSocket framing, Gateway protobufs, persistence, or audit storage. Raw wire observation, stderr lines, runtime state changes, and normalized adapter events are exposed as callbacks/channels so the Host can connect its own audit and state layers.

## Codex CLI 0.147.0 protocol

The implementation was aligned against schemas generated locally with:

```text
codex app-server generate-json-schema --experimental --out <temporary-directory>
```

The runtime launches:

```text
codex app-server --listen unix://PATH
```

The Unix listener requires a standard HTTP Upgrade handshake and then carries one JSON-RPC 2.0 message per WebSocket text frame. Startup calls `initialize` with `experimentalApi: true`, waits for its response, then sends `initialized`. Implemented client calls are `thread/list`, `thread/read`, `thread/start`, `thread/resume`, `turn/start`, and `turn/interrupt`. Server requests handled are `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`, and `item/tool/requestUserInput`. User-input questions and their typed options are parsed into internal structs, including `allowsMultiple`, `isOther`, and `isSecret`; answers remain a map from question ID to a list of selected/free-form strings as required by 0.147.0. The adapter explicitly sets a 64 MiB inbound WebSocket message limit by default so large command output, diffs, and vendor events are not rejected by coder/websocket's much smaller library default. `DialWithOptions` and `runtime.Config.AppServerReadLimit` allow an override without requiring Host configuration.

`thread/list` supports the exact cwd filter, pagination cursor/limit, and explicit source kinds. Session discovery also normalizes cwd, filters subagent threads, and compares normalized cwd exactly. Import reads persisted turns before resume and preserves that history when resume returns no turns.

Adapter notifications are classified into the eight Host-facing activity families (`codex_updated`, `turn_updated`, `item_started`, `item_delta`, `item_updated`, `item_completed`, `pending_request_updated`, and `warning_raised`). The original method and complete vendor params remain attached for the Host translator/audit path; unrecognized notifications are explicitly classified as `vendor` rather than discarded.

Turn decoding preserves Codex 0.147.0 `startedAt`, `completedAt`, `durationMs`, structured `error`, raw items, and `itemsView`. `itemsView=full/summary/notLoaded` is additionally normalized to full/partial/unknown completeness without discarding the original value. Real camelCase notifications such as `item/commandExecution/outputDelta`, `command/exec/outputDelta`, `item/fileChange/outputDelta`, and `process/outputDelta` are recognized. Events expose command/process stream, text or base64 encoding, plan steps, diff text, and explicit semantics while retaining raw params.

Turn options keep collaboration mode and reasoning effort separate. Protocol field `mode` is forwarded as Codex `collaborationMode`; `reasoning_effort` is forwarded as top-level `effort` when there is no collaboration mode, or as `collaborationMode.settings.reasoning_effort` when there is one. Codex 0.147.0 requires `collaborationMode.settings.model`, so a collaboration mode without an explicit model returns a clear adapter error instead of silently treating the mode as effort.

## Runtime behavior

`runtime.Manager` owns only the child it starts. It rejects a live socket, removes a stale socket, captures stderr, initializes readiness, multiplexes adapter events, and exposes current state/adapter access. Unexpected process exits use bounded retry/backoff. If the adapter WebSocket closes while the child remains alive, the manager immediately leaves Ready, reconnects and initializes against the same process, then publishes a new Ready adapter; retry budgets reset after every successful connection. Only exhausted reconnects fall back to an owned-process restart. A requested process restart executes immediately while idle or waits for the active-turn count to reach zero. In-flight RPCs and active turns are never replayed. Higher Host layers must re-read persisted mappings, histories, and pending/current views after each Ready transition.

## Host integration dependencies

- Attach `Config.WireObserver` to app-server raw-wire audit before starting the manager.
- Attach `Config.Stderr`, `States()`, and `Events()` to diagnostic audit/activity translation.
- Reacquire `Manager.Adapter()` after every Ready transition; adapter pointers are instance-scoped.
- On restart Ready, the Host must rebuild session/history/mapping/pending views from Codex plus Host persistence and force Gateway Watch reset. Runtime intentionally does not own those stores.
- Map protocol approval decisions to adapter strings: `accept`, `acceptForSession`, `decline`, or `cancel`. Permission-profile accept echoes the requested profile with turn/session scope; decline/cancel grants an empty profile.

## Verification

Tests cover directory normalization/create/error paths, exact-cwd session discovery, explicit interactive source coverage, subagent filtering, import read-before-resume, out-of-order RPC responses, RPC errors/disconnect, a 260 KiB vendor message over a real Unix WebSocket, initialize, full Turn schema preservation, collaboration-mode/reasoning-effort wire separation, camelCase output/plan/diff notification semantics, command/permission approval, structured user input, owned subprocess startup, same-process recovery from repeated Unix WebSocket disconnects, live-socket ownership rejection, planned idle restart, unexpected exit restart, and deferred restart after turn completion.

Package tests are run with the repository-local Go toolchain (`.tools/go/bin/go`). `CODEX_REMOTE_REAL_APPSERVER=1 go test ./internal/adapter -run TestRealCodexAppServerReadOnlySmoke -v` passed against the installed Codex CLI 0.147.0 and exercised real process startup, WebSocket-over-UDS initialize, and read-only `thread/list`. Repository-wide `go test ./...`, `go test -race ./...`, `go vet ./...`, protocol lint/build/generation, and the launched-Host external black-box suite all pass. The black-box runtime-disconnect scenario closes the app-server WebSocket while leaving its process alive, then verifies automatic same-process recovery and a successful subsequent RPC.
