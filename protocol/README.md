# Codex Remote Protocol V1 IDL

`codex/remote/v1/` is the source of truth for the Client/Host boundary and shared diagnostic-audit format. This repository implements the Linux Host only; the complete boundary remains in the IDL for generated external clients and black-box tests.

The wire format is ProtoJSON carried in one UTF-8 WebSocket text message per `codex.remote.v1.Frame`. Binary Protobuf is not part of V1. The formal endpoint is Tailnet-only plain `ws://<tsnet-node>/connect`, served by embedded `tsnet.Server.Listen("tcp", ":80")`.

From the repository root, validate and generate with the pinned project-local toolchain:

```bash
make proto-lint
make proto-compile
make proto-generate
```

Generated clients must use their Protobuf runtime's canonical JSON codec. They must not hand-build or hand-parse wire JSON.

V1 currently accepts exactly protocol version `1.0`; minor-version negotiation is not implemented.
