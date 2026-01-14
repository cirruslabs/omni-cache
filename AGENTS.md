# Repository Guidelines

Always ALWAYS make sure tests are passing and there are no lint errors before claiming work is done.

## Project Structure & Module Organization
- `cmd/omni-cache`: entrypoint binary (wire server and protocol factories here).
- `api` folder contains gRPC service definitions. Always run `buf generate` to generate Go code.
- `pkg/server`: HTTP server bootstrap that registers caching protocols.
- `pkg/protocols`: interfaces for pluggable caching protocols.
- `internal/protocols/http_cache`: HTTP cache implementation and end-to-end tests.
- `pkg/url-proxy`: shared HTTP/gRPC proxy helpers with unit tests.
- `pkg/storage`: storage interfaces plus the S3-backed implementation; `internal/testutil` spins up LocalStack for tests.

## Build, Test, and Development Commands
- `go build ./...` builds the binary and libraries.
- `go test ./...` runs all tests; requires Docker running because tests start LocalStack via testcontainers.
- `go test ./... -run TestHTTPCache` focuses on the HTTP cache flow.
- `go vet ./...` (recommended) surfaces common Go correctness issues.
- `cirrus run  --output=simple lint` will run linters locally.

## Coding Style & Naming Conventions
- Use standard Go formatting (`gofmt -w` on touched files); tabs for indentation.
- Run linter locally (`golangci-lint run -v`) to catch common mistakes before finishing your task.
- Keep package names short and lower_snake; exported identifiers use PascalCase, locals camelCase.
- Accept `context.Context` as the first argument for request-scoped functions and propagate it to logging (`slog`) and clients.
- Prefer small, composable helpers; reuse `urlproxy` for network forwarding and `storage.URLInfo` for URLs/headers.

## Testing Guidelines
- Primary framework is the Go testing package with `testify/require` for assertions.
- Tests expect Docker available to launch LocalStack (S3 emulation).
- Add focused tests next to the code they cover (e.g., `pkg/url-proxy/grpc_test.go`, `internal/protocols/http_cache/http_cache_test.go`).
- ALWAYS make sure tests are passing before claiming work is done. No need to confirm it.

## Commit & Pull Request Guidelines
- Commit messages follow the concise, imperative style seen in history (e.g., `Add gRPC tests for ProxyUploadToURL`).
- Each PR should include: purpose/behavior summary, linked issue if any, test results (`go test ./...`).
- Favor small, reviewable changes; include protocol/storage/backward-compatibility considerations in the description when relevant.

## Security & Configuration Tips
- Avoid embedding real AWS credentials; tests rely on LocalStack and static test credentials.
- Keep bucket names and prefixes lowercase; `storage.NewS3Storage` normalizes these and presigns requests with short expirations.
- When adding new protocols, ensure request headers that affect signing are preserved (see `storage.URLInfo.ExtraHeaders` handling).
