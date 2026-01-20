# Omni Cach Sidecar

Sidecar for your caching needs along CI workloads.

Note: project is very WIP but it extracts logic from caching used in [Cirrus Runners](https://cirrus-runners.app/) and [Cirrus CI](https://cirrus-ci.org/) which handle millions of caches monthly. Subscribe to releases and wait for version 1.0.0.

## Protocols

Omni-cache is built around pluggable caching protocols. Each protocol provides a `protocols.Factory` and registers its HTTP and/or gRPC handlers via `protocols.Registrar`.

Built-in protocol factories live in `pkg/protocols/builtin` (`builtin.Factories()`).

## Development

Run omni-cache with a LocalStack S3 backend (Docker required):

```sh
go run ./cmd/omni-cache dev
```
