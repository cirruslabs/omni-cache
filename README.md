# Omni Cache Sidecar

Omni Cache is a sidecar daemon that exposes multiple cache protocols on a local endpoint while
storing blobs in an S3-compatible storage. Run it next to a CI runner or build job so cache traffic stays
on the host network and tools do not need direct S3 credentials.

## Installation

- [Homebrew](INSTALL.md#homebrew)
- [Debian-based distributions](INSTALL.md#debian-based-distributions) (Debian, Ubuntu, etc.)
- [RPM-based distributions](INSTALL.md#rpm-based-distributions) (Fedora, CentOS, etc.)
- [Prebuilt Binary](INSTALL.md#prebuilt-binary)
- [Golang](INSTALL.md#golang)

## Sidecar mode

- Listens on a local TCP address (default `localhost:12321`) and, on Unix, a unix socket at
  `~/.cirruslabs/omni-cache.sock`.
- Serves HTTP and gRPC (h2c) on the same port.
- All built-in protocols are enabled by default.

Run against your S3 bucket:

```sh
export OMNI_CACHE_BUCKET=ci-cache
export OMNI_CACHE_PREFIX=my-repo
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
omni-cache sidecar
```

Quick local test with LocalStack (requires Docker):

```sh
omni-cache dev
```

## Configuration

- `OMNI_CACHE_BUCKET` (required): S3 bucket to store cache blobs.
- `OMNI_CACHE_PREFIX` (optional): prefix for cache objects.
- `OMNI_CACHE_S3_ENDPOINT` (optional): override the S3 endpoint URL (must include scheme, e.g. `https://s3.example.com` or `http://localhost:4566`).
  When set, Omni Cache uses path-style S3 requests for compatibility with S3-compatible endpoints.
- `OMNI_CACHE_HOST` (optional): listen address. Accepts `host`, `host:port`, or `http(s)://host:port`.
  Default: `localhost:12321`. This address is also embedded into GitHub Actions cache v2
  upload/download URLs, so set it to something your clients can reach.
- S3 credentials and region are resolved via the AWS SDK default chain (`AWS_REGION`,
  shared config/credentials files, instance roles).

CLI flags override env values:
- `omni-cache sidecar --bucket ... --prefix ... --s3-endpoint ...`
- `omni-cache dev --bucket ... --prefix ... --localstack-image ...`

## Stats endpoint

Omni Cache exposes a lightweight stats endpoint on the same host as the sidecar.

- `GET /stats` returns counters and transfer metrics.
- `DELETE /stats` resets the counters and returns the post-reset snapshot.
- Responses are `text/plain` by default. Send `Accept: application/json` (or `+json`) to get JSON.
- Send `Accept: text/vnd.github-actions` to emit GitHub Actions notices (empty response when no cache activity is recorded).
- This endpoint is especially useful as the final step of a CI pipeline to record cache effectiveness.

Text output example:

```
omni-cache stats
cache hits: 10472
cache misses: 117
cache hit rate: 98.9%
downloads: count=10472 total=1.9 GiB avg=194 KiB avgTime=7ms avgSpeed=28 MB/s
uploads: count=3810 total=131 MiB avg=35 KiB avgTime=361ms avgSpeed=100 kB/s
```

JSON fields:

- `cache_hits`, `cache_misses`, `cache_hit_rate_percent`
- `downloads` / `uploads`: `count`, `bytes`, `duration_ms`, `avg_bytes`, `avg_duration_ms`, `bytes_per_sec`

## Protocols

Omni Cache ships with built-in protocols enabled. See `PROTOCOLS.md` for build-system-focused examples:

- [Docker Layer Caching (via `gha` cache)](PROTOCOLS.md#docker-layer-caching-github-actions-cache)
- [Bazel (HTTP Cache)](PROTOCOLS.md#bazel-http-cache)
- [Gradle (HTTP Build Cache)](PROTOCOLS.md#gradle-http-build-cache)
- [Xcode / LLVM Compilation Cache](PROTOCOLS.md#xcode--llvm-compilation-cache)
- [Custom HTTP Clients](PROTOCOLS.md#custom-http-clients)

Need a custom protocol? Check [existing issues](https://github.com/cirruslabs/omni-cache/issues?q=is%3Aissue%20state%3Aopen%20Support) or create a new one.

## Development

Run omni-cache with a LocalStack S3 backend (Docker required):

```sh
go run ./cmd/omni-cache dev
```
