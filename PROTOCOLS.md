# Build System Integrations

Omni Cache exposes multiple cache protocols on the same endpoint. This guide is organized by
build system or tool and points to the protocol you should use.

## Index

- [Docker Layer Caching (GitHub Actions cache)](#docker-layer-caching-github-actions-cache)
- [Bazel (HTTP cache)](#bazel-http-cache)
- [Bazel (gRPC CAS + Remote Asset)](#bazel-grpc-cas--remote-asset)
- [Gradle (HTTP build cache)](#gradle-http-build-cache)
- [CCache (HTTP storage backend)](#ccache-http-storage-backend)
- [Xcode / LLVM compilation cache](#xcode--llvm-compilation-cache)
- [Tuist module cache](#tuist-module-cache)
- [Custom HTTP clients](#custom-http-clients)

## Docker Layer Caching (GitHub Actions cache)

Use Docker layer caching via BuildKit with the GitHub Actions cache protocol.
Set `OMNI_CACHE_ADDRESS` to the sidecar address (for host networking, usually `127.0.0.1:12321`):

- `gha-cache` (v1): `type=gha,url=http://$OMNI_CACHE_ADDRESS/`
- `gha-cache-v2` (v2): `type=gha,version=2,url_v2=http://$OMNI_CACHE_ADDRESS/`

Docker Buildx example (v1):

```yaml
env:
  OMNI_CACHE_ADDRESS: 127.0.0.1:12321
steps:
  - name: Set up Docker Buildx
    uses: docker/setup-buildx-action@v3
    with:
      driver-opts: network=host
  - name: Build with cache
    uses: docker/build-push-action@v6
    with:
      push: true
      tags: user/app:latest
      cache-from: type=gha,url=http://${{ env.OMNI_CACHE_ADDRESS }}/
      cache-to: type=gha,url=http://${{ env.OMNI_CACHE_ADDRESS }}/,mode=max
```

Docker Buildx example (v2):

```yaml
env:
  OMNI_CACHE_ADDRESS: 127.0.0.1:12321
steps:
  - name: Set up Docker Buildx
    uses: docker/setup-buildx-action@v3
    with:
      driver-opts: network=host
  - name: Build with cache
    uses: docker/build-push-action@v6
    with:
      push: true
      tags: user/app:latest
      cache-from: type=gha,version=2,url_v2=http://${{ env.OMNI_CACHE_ADDRESS }}/
      cache-to: type=gha,version=2,url_v2=http://${{ env.OMNI_CACHE_ADDRESS }}/,mode=max
```

Note: the `network=host` driver option allows BuildKit to reach the sidecar on `$OMNI_CACHE_ADDRESS`.

## Bazel (HTTP cache)

Use the HTTP cache protocol (`http-cache`) and point Bazel at the Omni Cache HTTP endpoint:

```sh
export OMNI_CACHE_ADDRESS=localhost:12321

bazel build \
  --spawn_strategy=sandboxed \
  --strategy=Javac=sandboxed \
  --genrule_strategy=sandboxed \
  --remote_http_cache=http://$OMNI_CACHE_ADDRESS \
  //...
```

## Bazel (gRPC CAS + Remote Asset)

Use the Bazel remote cache/downloader gRPC endpoint (`bazel-remote`) and point Bazel at Omni Cache:

```sh
export OMNI_CACHE_ADDRESS=localhost:12321

bazel build \
  --remote_cache=grpc://$OMNI_CACHE_ADDRESS \
  --experimental_remote_downloader=grpc://$OMNI_CACHE_ADDRESS \
  --remote_instance_name=omni-cache \
  //...
```

Current limits:
- Digest function: SHA256 only.
- Remote Asset origin fetch: `http`/`https` only.
- Remote Asset directory APIs are not implemented yet (`FetchDirectory`/`PushDirectory`).

## Gradle (HTTP build cache)

Use the HTTP cache protocol (`http-cache`) and set the remote cache URL to the Omni Cache endpoint:

```groovy
ext.isCiServer = System.getenv().containsKey("CI")
ext.buildCacheHost = System.getenv().getOrDefault("OMNI_CACHE_ADDRESS", "localhost:12321")

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

## CCache (HTTP storage backend)

Use the HTTP cache protocol (`http-cache`) and set `CCACHE_REMOTE_STORAGE` to a path prefix
served by Omni Cache:

```sh
export OMNI_CACHE_ADDRESS=localhost:12321
export CCACHE_REMOTE_STORAGE=http://$OMNI_CACHE_ADDRESS/cache/
```

Omni Cache supports the HTTP methods ccache uses for this backend (`GET`, `HEAD`, `PUT`, `DELETE`).

Optional ccache layout example:

```sh
export CCACHE_REMOTE_STORAGE='http://localhost:12321/cache/|layout=bazel'
```

## Xcode / LLVM compilation cache

Use the LLVM compilation cache protocol (`llvm-cache`) via the gRPC socket endpoint.

Add the following build settings to your Xcode project:

| Setting                                       | Value                               |
|-----------------------------------------------|-------------------------------------|
| `COMPILATION_CACHE_ENABLE_CACHING`            | `YES`                               |
| `COMPILATION_CACHE_ENABLE_PLUGIN`             | `YES`                               |
| `COMPILATION_CACHE_REMOTE_SERVICE_PATH`       | `$HOME/.cirruslabs/omni-cache.sock` |

Note: `COMPILATION_CACHE_REMOTE_SERVICE_PATH` and `COMPILATION_CACHE_ENABLE_PLUGIN` must be added as
user-defined build settings since they are not exposed in Xcode's build settings UI.

Alternatively, pass these settings directly to xcodebuild:

```sh
xcodebuild \
  COMPILATION_CACHE_ENABLE_CACHING=YES \
  COMPILATION_CACHE_ENABLE_PLUGIN=YES \
  COMPILATION_CACHE_REMOTE_SERVICE_PATH=$HOME/.cirruslabs/omni-cache.sock \
  ...
```

## Tuist Module Cache

Use Tuist's module cache API exposed by Omni Cache under the `/tuist` prefix.

Set the endpoint override so Tuist talks to your local sidecar:

```sh
export OMNI_CACHE_ADDRESS=localhost:12321
export TUIST_CACHE_ENDPOINT=http://$OMNI_CACHE_ADDRESS/tuist
```

Tuist will automatically use the following endpoints on that host:

- `HEAD /tuist/api/cache/module/{id}`
- `GET /tuist/api/cache/module/{id}`
- `POST /tuist/api/cache/module/start`
- `POST /tuist/api/cache/module/part`
- `POST /tuist/api/cache/module/complete`

## Custom HTTP clients

Use the HTTP cache protocol (`http-cache`) and treat cache keys as paths:

```sh
export OMNI_CACHE_ADDRESS=localhost:12321
curl -s -X POST --data-binary @myfolder.tar.gz http://$OMNI_CACHE_ADDRESS/name-key
```
