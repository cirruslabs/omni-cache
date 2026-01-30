# Build System Integrations

Omni Cache exposes multiple cache protocols on the same endpoint. This guide is organized by
build system or tool and points to the protocol you should use.

## Index

- [Docker Layer Caching (GitHub Actions cache)](#docker-layer-caching-github-actions-cache)
- [Bazel (HTTP cache)](#bazel-http-cache)
- [Gradle (HTTP build cache)](#gradle-http-build-cache)
- [Xcode / LLVM compilation cache](#xcode--llvm-compilation-cache)
- [Custom HTTP clients](#custom-http-clients)

## Docker Layer Caching (GitHub Actions cache)

Use Docker layer caching via BuildKit with the GitHub Actions cache protocol:

- `gha-cache` (v1): `type=gha,url=http://127.0.0.1:12321/`
- `gha-cache-v2` (v2): `type=gha,version=2,url_v2=http://127.0.0.1:12321/`

Docker Buildx example (v1):

```yaml
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
      cache-from: type=gha,url=http://127.0.0.1:12321/
      cache-to: type=gha,url=http://127.0.0.1:12321/,mode=max
```

Docker Buildx example (v2):

```yaml
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
      cache-from: type=gha,version=2,url_v2=http://127.0.0.1:12321/
      cache-to: type=gha,version=2,url_v2=http://127.0.0.1:12321/,mode=max
```

Note: the `network=host` driver option allows BuildKit to reach the sidecar on `127.0.0.1`.

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

## Custom HTTP clients

Use the HTTP cache protocol (`http-cache`) and treat cache keys as paths:

```sh
export OMNI_CACHE_ADDRESS=localhost:12321
curl -s -X POST --data-binary @myfolder.tar.gz http://$OMNI_CACHE_ADDRESS/name-key
```
