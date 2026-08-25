# Codex Remote

Codex Remote is a Linux Host-only personal demo for controlling local Codex sessions from another device on the same Tailnet. The Host embeds [`tsnet`](https://pkg.go.dev/tailscale.com/tsnet), exposes a plain WebSocket only inside the Tailnet, and implements the Host-supported portion of the V1.1.2 ProtoJSON protocol.

This repository contains the Host only; a Client is not included. It is functionality-first demo software, not a production security or compliance solution.

## Requirements

- Linux amd64
- A Tailscale account/Tailnet
- A working `codex` CLI with `codex app-server`
- `curl`, `tar`, and `sha256sum` for the pinned project-local toolchain

## Build and test

```bash
make tools
.tools/go/bin/go build -o codex-remote-host ./cmd/codex-remote-host
make test
```

Run the full Host checks with `make check`. Protocol generation is not part of this repository's build.

## Management lifecycle

Managed Codexes have a persistent two-hour inactivity lease and enter `EXPIRING_SOON` 30 minutes before expiry. Manual and safe idle-time automatic unmanage preserve the Codex/session/history; a later `StartTurn` restores the same `codex_id`. Accepted control activity and `Pong.foreground_codex_ids` renew eligible leases, while passive List/Watch/history traffic does not. Deadlines and per-cycle warning deduplication survive Host restart.

The defaults can be changed for development and testing:

```text
--lease-duration=2h
--lease-warning-before=30m
--lease-sweep-interval=1m
```

## Workspace access

The Host implements 22 RPCs in V1.1.2, including the six workspace RPCs—GetWorkspace, ListWorkspaceEntries, ReadWorkspaceTextFile, WriteWorkspaceTextFile, UploadWorkspaceEntry, and DownloadWorkspaceEntry. `Capabilities.workspace` advertises five strictly positive hard limits. With the default 4 MiB formal frame they are 512 KiB text files, 2 MiB inline upload, 2 MiB inline download, 32 MiB expanded archives, and 1,000 archive entries; inline/text limits are conservatively reduced when a smaller frame limit is configured.

Workspace state is exposed consistently through GetWorkspace, Watch RESET snapshots, and WorkspaceAccessStateUpdated. Its persisted monotonic `generation`, `active_agent_count`, and opaque `quiescence_token` gate mutations: writes and uploads are accepted only after the parent agent and every subagent have stopped. Listing, reading, and downloading remain available while agents are active. Only regular files have revisions; Upload is token-gated unconditional create-or-replace, including regular/directory cross-kind replacement, while WriteText remains CAS. Management is independent of workspace access, so an `UNMANAGED` Codex keeps the same workspace operations and state.

## Rename and forget

`RenameCodex` changes only the Host display title. Its manual override is persistent and prevents later native automatic-title updates, including after Host restart. It is valid for every management state.

`ForgetCodex` is allowed only after `UNMANAGED`. It removes Host-owned mapping, CurrentView, listing/title state, and workspace registration, but preserves the native rollout, CWD, diagnostic audit, request-dedup records, and a Host-persisted forgotten candidate. Discovery merges that candidate with runtime results as `RESUMABLE`, with no `managed_codex_id`. A materialized native session can be reimported with a new `codex_id` while retaining its native thread and history. In the same run, an unmaterialized session reuses its old thread and is materialized by `StartTurn` without fabricating a rollout; after restart, if upstream genuinely reports that thread as not found, import creates a new native thread in the stored CWD.

## First start

Start the Host and open the Tailscale authentication URL printed in its console:

```bash
./codex-remote-host
```

For unattended login, provide a suitable Tailscale auth key:

```bash
TS_AUTHKEY=tskey-auth-... ./codex-remote-host
```

After the Host is ready, it prints its connection URL. With the default hostname, the Client connects through MagicDNS at:

```text
ws://codex-remote-<linux-hostname>/connect
```

Both devices must be on the same Tailnet. Traffic is plain WebSocket inside that private network; there is no LAN, public-network, WSS, Serve, or Funnel fallback.

The authoritative language-neutral schema and generated Go artifact are published by [codex-remote-protocol](https://github.com/FireflyTang/codex-remote-protocol). This Host consumes exact `v1.1.2`: both ClientHello and ServerHello use `{major:1, minor:1, patch:2}` with no version fallback. [`protocol.lock`](protocol.lock) records the exact tag, peeled source commit, and descriptor hash. See [development status and known limits](docs/development_status.md#honest-limits-and-deferred-validation) for current boundaries.
