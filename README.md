# Codex Remote

Codex Remote is a Linux Host-only personal demo for controlling local Codex sessions from another device on the same Tailnet. The Host embeds [`tsnet`](https://pkg.go.dev/tailscale.com/tsnet), exposes a plain WebSocket only inside the Tailnet, and implements the Host-supported portion of the V1.1 ProtoJSON protocol.

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

The Host implements all six V1.1 workspace RPCs—GetWorkspace, ListWorkspaceEntries, ReadWorkspaceTextFile, WriteWorkspaceTextFile, UploadWorkspaceEntry, and DownloadWorkspaceEntry—bringing the Host total to 20 RPCs. `Capabilities.workspace` advertises five strictly positive hard limits. With the default 4 MiB formal frame they are 512 KiB text files, 2 MiB inline upload, 2 MiB inline download, 32 MiB expanded archives, and 1,000 archive entries; inline/text limits are conservatively reduced when a smaller frame limit is configured.

Workspace state is exposed consistently through GetWorkspace, Watch RESET snapshots, and the ninth Event family, WorkspaceAccessStateUpdated. Its persisted monotonic `generation`, `active_agent_count`, and opaque `quiescence_token` gate mutations: writes and uploads are accepted only after the parent agent and every subagent have stopped. Listing, reading, and downloading remain available while agents are active. Management is independent of workspace access, so an `UNMANAGED` Codex keeps the same workspace operations and state.

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

The authoritative language-neutral schema and generated Go artifact are published by [codex-remote-protocol](https://github.com/FireflyTang/codex-remote-protocol). This Host currently consumes `v1.1.0`; [`protocol.lock`](protocol.lock) records the exact tag, peeled source commit, and descriptor hash. See [development status and known limits](docs/development_status.md#honest-limits-and-deferred-validation) for current boundaries.
