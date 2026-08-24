# Codex Remote

Codex Remote is a Linux Host-only personal demo for controlling local Codex sessions from another device on the same Tailnet. The Host embeds [`tsnet`](https://pkg.go.dev/tailscale.com/tsnet), exposes a plain WebSocket only inside the Tailnet, and implements the V1 ProtoJSON protocol with 13 RPC routes.

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

The authoritative language-neutral schema and generated Go artifact are published by [codex-remote-protocol](https://github.com/FireflyTang/codex-remote-protocol). This Host currently consumes `v1.0.0`; [`protocol.lock`](protocol.lock) records the exact source commit and descriptor hash. See [development status and known limits](docs/development_status.md#honest-limits-and-deferred-validation) for current boundaries.
