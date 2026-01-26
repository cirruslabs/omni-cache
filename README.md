# Omni Cache Sidecar

Omni Cache is a sidecar daemon that exposes multiple cache protocols on a local endpoint while
storing blobs in S3. Run it next to a CI runner or build job so cache traffic stays on the
host network and tools do not need direct S3 credentials.

## Installation

```sh
brew install cirruslabs/cli/omni-cache
```

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
- `OMNI_CACHE_HOST` (optional): listen address. Accepts `host`, `host:port`, or `http(s)://host:port`.
  Default: `localhost:12321`. This address is also embedded into GitHub Actions cache v2
  upload/download URLs, so set it to something your clients can reach.
- S3 credentials and region are resolved via the AWS SDK default chain (`AWS_REGION`,
  shared config/credentials files, instance roles).

CLI flags override env values:
- `omni-cache sidecar --bucket ... --prefix ...`
- `omni-cache dev --bucket ... --prefix ... --localstack-image ...`

## Protocols

Omni Cache ships with built-in protocols enabled.

> [!NOTE]
> Endpoint details live next to each protocol factory:
> [http-cache][http-cache-factory], [gha-cache][gha-cache-factory],
> [gha-cache-v2][gha-cache-v2-factory], [azure-blob][azure-blob-factory],
> [llvm-cache][llvm-cache-factory].

### [HTTP Cache (`http-cache`)][http-cache-factory]
- Supported tools: custom HTTP cache clients, curl-based workflows.
- Configure: point your client to `http://<host>:12321/` and use cache keys as paths.

<details>
<summary>Examples (from Cirrus CI HTTP cache docs)</summary>

```sh
export OMNI_CACHE_HOST=localhost:12321
curl -s -X POST --data-binary @myfolder.tar.gz http://$OMNI_CACHE_HOST/name-key
```

```sh
bazel build \
  --spawn_strategy=sandboxed \
  --strategy=Javac=sandboxed \
  --genrule_strategy=sandboxed \
  --remote_http_cache=http://$OMNI_CACHE_HOST \
  //...
```

```groovy
ext.isCiServer = System.getenv().containsKey("CI")
ext.buildCacheHost = System.getenv().getOrDefault("OMNI_CACHE_HOST", "localhost:12321")

buildCache {
  local {
    enabled = !isCiServer
  }
  remote(HttpBuildCache) {
    url = "http://${buildCacheHost}/"
    enabled = isCiServer
    push = isMasterBranch
  }
}
```

</details>

### [GitHub Actions Cache v1 (`gha-cache`)][gha-cache-factory]
- Supported tools: `actions/cache` (legacy v1 API; deprecated by GitHub), `go-actions-cache`
  (v1 mode), BuildKit `type=gha` (v1 mode).
- Configure: set `ACTIONS_CACHE_URL=http://<host>:12321` and ensure `ACTIONS_RUNTIME_TOKEN` is set if
  your client requires it.

### [GitHub Actions Cache v2 (`gha-cache-v2`)][gha-cache-v2-factory]
- Supported tools: `actions/cache` (v2 API), `go-actions-cache` (v2 mode), BuildKit `type=gha` (v2 mode).
- Configure: set `ACTIONS_CACHE_SERVICE_V2=true` (or `ACTIONS_CACHE_API_FORCE_VERSION=v2` for
  `go-actions-cache`), set `ACTIONS_RESULTS_URL=http://<host>:12321`, and provide
  `ACTIONS_RUNTIME_TOKEN` if your client requires it.

### [LLVM Compilation Cache (`llvm-cache`)][llvm-cache-factory] aka Xcode Cache
- Supported tools: LLVM compilation cache clients (for example, Xcode/xcodebuild) that implement
  `compilation_cache_service` gRPC APIs.

<details>
<summary>Xcode Build Settings</summary>

Add the following build settings to your Xcode project:

| Setting                                       | Value                               |
|-----------------------------------------------|-------------------------------------|
| `COMPILATION_CACHE_ENABLE_CACHING`            | `YES`                               |
| `COMPILATION_CACHE_ENABLE_PLUGIN`             | `YES`                               |
| `COMPILATION_CACHE_ENABLE_DIAGNOSTIC_REMARKS` | `YES`                               |
| `COMPILATION_CACHE_REMOTE_SERVICE_PATH`       | `$HOME/.cirruslabs/omni-cache.sock` |

> [!NOTE]
> `COMPILATION_CACHE_REMOTE_SERVICE_PATH` and `COMPILATION_CACHE_ENABLE_PLUGIN` must be added as
> **user-defined build settings** since they are not exposed in Xcode's build settings UI.

Alternatively, pass these settings directly to xcodebuild:

```sh
xcodebuild \
  COMPILATION_CACHE_ENABLE_CACHING=YES \
  COMPILATION_CACHE_ENABLE_PLUGIN=YES \
  COMPILATION_CACHE_ENABLE_DIAGNOSTIC_REMARKS=YES \
  COMPILATION_CACHE_REMOTE_SERVICE_PATH=$HOME/.cirruslabs/omni-cache.sock \
  ...
```

</details>

[http-cache-factory]: internal/protocols/http_cache/http_cache.go
[gha-cache-factory]: internal/protocols/ghacache/protocol.go
[gha-cache-v2-factory]: internal/protocols/ghacachev2/protocol.go
[azure-blob-factory]: internal/protocols/azureblob/protocol.go
[llvm-cache-factory]: internal/protocols/llvm_cache/protocol.go

## Development

Run omni-cache with a LocalStack S3 backend (Docker required):

```sh
go run ./cmd/omni-cache dev
```
