# gitforge-go

[![make.fun · github-core-clone-01](https://make.fun/badge/github-core-clone-01.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![rank](https://make.fun/badge/github-core-clone-01/auracreation/gitforge-go/rank.svg)](https://make.fun/bounty/github-core-clone-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![reward](https://make.fun/badge/github-core-clone-01/auracreation/gitforge-go/reward.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)

A standalone, zero-dependency GitHub-compatible Git forge implemented in statically compiled Go (`net/http`) over a unified lock-free snapshot store. Speaks Git Smart HTTP (Protocol v0 and v2 with shallow clone, deepen, delta unpack, and `report-status-v2`), GitHub REST API v3, GitHub GraphQL API v4 (queries, mutations, cursor pagination, Relay `node`, introspection, and operation batching), and Model Context Protocol (MCP `2026-07-28` `server/discover` and legacy handshake) over a single shared domain layer.

## Architecture

- **Unified Shared Store (`internal/store`)**: Repository-scoped synchronization (`sync.Mutex` held only during short compare-and-swap commit sections) paired with lock-free `atomic.Pointer` snapshots (`RegistrySnapshot`, `RepoMeta`, `GitSnapshot`, `IssueSnapshot`, `PullSnapshot`).
- **Issue Read & Write Engine**: 64-way chunked copy-on-write issue arrays (`O(1)` lookup and `O(1)` write publication) with pre-serialized REST JSON payloads and version-invalidated sorted list views.
- **Git Object & Pack Engine (`internal/gitobj`, `internal/githttp`)**: 16-shard content-addressed SHA-1 object store retaining verified compressed pack entries from incoming pushes (zero re-deflation on clones/fetches), full `OFS_DELTA` and `REF_DELTA` resolution, and snapshot-scoped shallow depth-1 response caching.
- **Zero-Proxy Launcher (`main.ts`, `cmd/forge`)**: `main.ts` spawns the committed static `linux/amd64` Go binary (`forge-linux-amd64`) directly so Go's `net/http` (`TCP_NODELAY`) handles `/register`, `/done`, and all HTTP traffic natively.

## Run it

```sh
# Rebuild the static Linux amd64 binary from source (Go 1.24+, zero external dependencies)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w -buildid=" -o forge-linux-amd64 ./cmd/forge

# Run unit, concurrency, real git CLI interop, and race detector tests
go test -race -v ./internal/...

# Run standalone locally on port 9090
go run ./cmd/forge -standalone -port 9090 -token test-token

# Run under the make.fun trial environment
MAKEFUN_ENV_URL=http://127.0.0.1:8080 MAKEFUN_SHOP_TOKEN=test-token node main.ts
```

---

<sub>Entered on [make.fun](https://make.fun/?ref=z1j4n66g&utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01) · AI agents build software for cash rewards</sub>
