package ghacachev2

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"strings"

	"github.com/cirruslabs/omni-cache/internal/api/gharesults"
	"github.com/cirruslabs/omni-cache/internal/protocols/azureblob"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/samber/lo"
	"github.com/twitchtv/twirp"
)

// Interface guard
//
// Ensures that Cache struct implements gharesults.CacheService interface.
var _ gharesults.CacheService = (*Cache)(nil)

const APIMountPoint = "/twirp"

type Cache struct {
	cacheHost   string
	backend     storage.BlobStorageBackend
	twirpServer gharesults.TwirpServer
}

func New(cacheHost string, backend storage.BlobStorageBackend) *Cache {
	if backend == nil {
		panic("ghacachev2.New: backend is required")
	}
	cache := &Cache{
		cacheHost: cacheHost,
		backend:   backend,
	}

	cache.twirpServer = gharesults.NewCacheServiceServer(cache)

	return cache
}

func (cache *Cache) PathPrefix() string {
	return cache.twirpServer.PathPrefix()
}

func (cache *Cache) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	cache.twirpServer.ServeHTTP(writer, request)
}

func (cache *Cache) GetCacheEntryDownloadURL(ctx context.Context, request *gharesults.GetCacheEntryDownloadURLRequest) (*gharesults.GetCacheEntryDownloadURLResponse, error) {
	cacheKeyPrefixes := lo.Map(request.RestoreKeys, func(restoreKey string, _ int) string {
		return httpCacheKey(restoreKey, request.Version)
	})
	info, err := cache.backend.CacheInfo(ctx, httpCacheKey(request.Key, request.Version), cacheKeyPrefixes)
	if err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			stats.Default().RecordCacheMiss()
			return &gharesults.GetCacheEntryDownloadURLResponse{
				Ok: false,
			}, nil
		}

		return nil, twirp.NewErrorf(twirp.Internal, "GHA cache v2 failed to retrieve information "+
			"about cache entry with key %q and version %q: %v", request.Key, request.Version, err)
	}

	stats.Default().RecordCacheHit()
	return &gharesults.GetCacheEntryDownloadURLResponse{
		Ok:                true,
		SignedDownloadUrl: cache.azureBlobURL(info.Key, true),
		MatchedKey:        strings.TrimPrefix(info.Key, httpCacheKey("", request.Version)),
	}, nil
}

func (cache *Cache) CreateCacheEntry(ctx context.Context, request *gharesults.CreateCacheEntryRequest) (*gharesults.CreateCacheEntryResponse, error) {
	return &gharesults.CreateCacheEntryResponse{
		Ok:              true,
		SignedUploadUrl: cache.azureBlobURL(httpCacheKey(request.Key, request.Version), false),
	}, nil
}

func (cache *Cache) FinalizeCacheEntryUpload(ctx context.Context, request *gharesults.FinalizeCacheEntryUploadRequest) (*gharesults.FinalizeCacheEntryUploadResponse, error) {
	hash := fnv.New64a()

	_, _ = hash.Write([]byte(request.Key))
	_, _ = fmt.Fprintf(hash, "%d", request.SizeBytes)
	_, _ = hash.Write([]byte(request.Version))

	return &gharesults.FinalizeCacheEntryUploadResponse{
		Ok:      true,
		EntryId: int64(hash.Sum64()),
	}, nil
}

func httpCacheKey(key string, version string) string {
	return fmt.Sprintf("%s-%s", version, key)
}

func (cache *Cache) azureBlobURL(keyWithVersion string, skipHitMiss bool) string {
	rawURL := fmt.Sprintf("http://%s%s/%s", cache.cacheHost, azureblob.APIMountPoint, url.PathEscape(keyWithVersion))
	if !skipHitMiss {
		return rawURL
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	stats.AddSkipHitMissQuery(parsed)
	return parsed.String()
}
