package bazel_remote_asset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

type assetStore struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
}

func newAssetStore(backend storage.BlobStorageBackend, proxy *urlproxy.Proxy) *assetStore {
	return &assetStore{
		backend: backend,
		proxy:   proxy,
	}
}

func (s *assetStore) load(ctx context.Context, key string) (*assetRecord, error) {
	if s.backend == nil {
		return nil, fmt.Errorf("storage backend is nil")
	}
	if _, err := s.backend.CacheInfo(ctx, key, nil); err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) || storage.IsNotFoundError(err) {
			return nil, storage.ErrCacheNotFound
		}
		return nil, err
	}

	infos, err := s.backend.DownloadURLs(ctx, key)
	if err != nil {
		if storage.IsNotFoundError(err) {
			return nil, storage.ErrCacheNotFound
		}
		return nil, err
	}
	if len(infos) == 0 {
		return nil, fmt.Errorf("no download URLs returned")
	}

	var lastErr error
	for _, info := range infos {
		var buffer bytes.Buffer
		if err := s.proxy.DownloadToWriter(ctx, info, key, &buffer); err != nil {
			lastErr = err
			continue
		}

		var record assetRecord
		if err := json.Unmarshal(buffer.Bytes(), &record); err != nil {
			lastErr = err
			continue
		}
		return &record, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("download failed")
}

func (s *assetStore) save(ctx context.Context, key string, record *assetRecord) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	if record == nil {
		return fmt.Errorf("record is nil")
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}

	info, err := s.backend.UploadURL(ctx, key, nil)
	if err != nil {
		return err
	}

	return s.proxy.UploadFromReader(ctx, info, key, bytes.NewReader(payload), int64(len(payload)))
}
