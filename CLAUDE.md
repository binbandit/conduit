# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`github.com/crazywolf132/conduit`: a small Go library for Unix-domain-socket IPC between local processes. It uses only the standard library and has no external dependencies. `go.mod` requires Go 1.27, and the release workflow runs Go 1.27.3.

## Commands

```bash
go build ./...
go vet ./...
go test ./...                                   # all tests live in ./test
go test -race -count=1 ./test                   # tests are concurrent; use -race when touching client/server
GOEXPERIMENT=nojsonv2 go test -race ./test      # classic encoding/json decoder (CI runs both)
go test ./test -run TestClientReconnect -v      # single test
go run ./examples/pingpong server               # then: go run ./examples/pingpong client
go run ./examples/chat server                   # then: go run ./examples/chat client <username>
```

There's no Makefile or linter config. CI (`.github/workflows/release.yml`) is a manual `workflow_dispatch` on Go 1.27.3 that runs `go test -v ./...` twice (default jsonv2 decoder, then `GOEXPERIMENT=nojsonv2`) and then tags a GitHub release. Nothing runs on PRs.

## Architecture

- **Root package `conduit`** holds the shared pieces: `Message` (`types.go`), `ServerConfig`/`ClientConfig` and their `Default*Config` constructors (`config.go`), the `Logger` interface with `DefaultLogger`/`NoopLogger` (`logger.go`), and `LimitedReader` (`io.go`). `server/` and `client/` both import the root package. They never import each other.
- **Wire format:** a stream of JSON objects `{"type": "...", "payload": <raw JSON>}`, written with `json.Encoder` and read with `json.Decoder`. There's no length prefix. `Payload` is a `json.RawMessage`, and handlers decode it with `msg.UnmarshalPayload(&v)`.
- **Dispatch:** both sides keep a `map[string]Handler` keyed by message type and filled by `Handle()`. Each connection has one read goroutine, and handlers run synchronously inside it, so a slow handler blocks further reads on that connection. Messages with an unknown type are logged at Warn and skipped.
- **Server** (`server/server.go`): `Start()` deletes any existing socket file, listens, `chmod`s the socket to `SocketPermissions`, then spawns the accept loop. Each accepted conn becomes a `*Connection`, which has an ID, per-connection `Get/SetContext`, and `Send`. `Broadcast` sends to every conn while holding the server's RLock. `Stop()` is idempotent through `sync.Once`. It closes the listener and all conns, then removes the socket file.
- **Client** (`client/client.go`): `Connect()` dials and spawns `handleMessages`. When that read loop exits and `Reconnect` is true (the default) and the client isn't closed, its deferred func calls `ConnectWithRetry()`. That call blocks until it connects or `Close()` is called, and the new `Connect` starts a fresh read goroutine. The client's own errors are `client.ErrNotConnected` and `client.ErrClientClosed`.

## Non-obvious behavior

- **The server and client read loops must stay in sync.** Before each `Decode`, both `handleConnection` and `handleMessages` call `LimitedReader.Reset()`, clear the read deadline, and block in `decoder.More()`. That's how `MaxMessageSize` and `ReadTimeout` end up applying per message while idle connections stay open.
- **`LimitedReader` must return `(0, err)` once the limit is used up, never data together with an error.** The jsonv2-backed `encoding/json` (the default in recent toolchains) ignores read errors that come back with bytes. Run tests against both decoders: `GOEXPERIMENT=nojsonv2 go test ./...` uses the classic decoder, which builds that opt out of jsonv2 still get.
- **The README has drifted from the code.** It mentions `conduit.ErrConnectionClosed`/`conduit.ErrTimeout`, which don't exist, and gives a 10MB default size where the code uses 32MB. When they disagree, trust the code.

## Tests

- All tests are black-box in `package test` under `test/`. There are no `_test.go` files next to the source.
- Every test uses its own hard-coded `/tmp/conduit_*.sock` path. Give a new test its own unique path so tests don't collide.
- Tests silence logging with `cfg.Logger = conduit.NewLogger(conduit.LogError, nil)`, and they synchronize with buffered channels plus `time.After` timeouts. Some, like reconnect and broadcast, rely on `time.Sleep`, so they can be timing-sensitive.
