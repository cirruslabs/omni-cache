package llvm_cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

type cacheStore struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
}

func newCacheStore(backend storage.BlobStorageBackend, proxy *urlproxy.Proxy) *cacheStore {
	return &cacheStore{
		backend: backend,
		proxy:   proxy,
	}
}

func (s *cacheStore) download(ctx context.Context, key string) ([]byte, error) {
	if s.backend == nil {
		return nil, fmt.Errorf("storage backend is nil")
	}

	// Pre-flight CacheInfo to surface ErrCacheNotFound consistently across backends.
	if _, err := s.backend.CacheInfo(ctx, key, nil); err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			stats.Default().RecordCacheMiss()
			return nil, storage.ErrCacheNotFound
		}
		return nil, err
	}
	stats.Default().RecordCacheHit()

	infos, err := s.backend.DownloadURLs(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, fmt.Errorf("no download URLs returned")
	}

	var lastErr error
	for _, info := range infos {
		var buffer bytes.Buffer
		if err := s.proxy.DownloadToWriter(ctx, info, key, &buffer); err == nil {
			return buffer.Bytes(), nil
		} else {
			lastErr = err
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("download failed")
}

func (s *cacheStore) upload(ctx context.Context, key string, data []byte) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	info, err := s.backend.UploadURL(ctx, key, nil)
	if err != nil {
		return err
	}
	return s.proxy.UploadFromReader(ctx, info, key, bytes.NewReader(data), int64(len(data)))
}
