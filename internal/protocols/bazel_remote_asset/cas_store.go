package bazel_remote_asset

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

var errLimitReached = errors.New("byte limit reached")

type casStore struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
}

func newCASStore(backend storage.BlobStorageBackend, proxy *urlproxy.Proxy) *casStore {
	return &casStore{
		backend: backend,
		proxy:   proxy,
	}
}

func (s *casStore) has(ctx context.Context, digest assetDigest) (bool, error) {
	if s.backend == nil {
		return false, fmt.Errorf("storage backend is nil")
	}

	if _, err := s.backend.CacheInfo(ctx, casStorageKey(digest), nil); err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) || storage.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *casStore) save(ctx context.Context, digest assetDigest, reader io.Reader, size int64) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	if reader == nil {
		return fmt.Errorf("reader is nil")
	}

	info, err := s.backend.UploadURL(ctx, casStorageKey(digest), nil)
	if err != nil {
		return err
	}

	return s.proxy.UploadFromReader(ctx, info, casStorageKey(digest), reader, size)
}

func (s *casStore) readAll(ctx context.Context, digest assetDigest) ([]byte, error) {
	var buf bytes.Buffer
	if err := s.stream(ctx, digest, 0, 0, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *casStore) stream(ctx context.Context, digest assetDigest, offset, limit int64, w io.Writer) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	if w == nil {
		return fmt.Errorf("writer is nil")
	}

	infos, err := s.backend.DownloadURLs(ctx, casStorageKey(digest))
	if err != nil {
		if storage.IsNotFoundError(err) {
			return storage.ErrCacheNotFound
		}
		return err
	}
	if len(infos) == 0 {
		return fmt.Errorf("no download URLs returned")
	}
	writer := &skipLimitWriter{writer: w, skip: offset, limit: limit}
	if err := s.proxy.DownloadToWriter(ctx, infos[0], casStorageKey(digest), writer); err != nil {
		if errors.Is(err, errLimitReached) {
			return nil
		}
		return err
	}
	return nil
}

func casStorageKey(digest assetDigest) string {
	return path.Join(assetPrefix, "cas", digest.Hash, fmt.Sprintf("%d", digest.SizeBytes))
}

type skipLimitWriter struct {
	writer io.Writer
	skip   int64
	limit  int64
}

func (w *skipLimitWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if w.skip > 0 {
		if int64(len(p)) <= w.skip {
			w.skip -= int64(len(p))
			return len(p), nil
		}
		p = p[w.skip:]
		w.skip = 0
	}

	if w.limit == 0 {
		return w.writer.Write(p)
	}

	if int64(len(p)) <= w.limit {
		n, err := w.writer.Write(p)
		if err == nil {
			w.limit -= int64(n)
		}
		return n, err
	}

	p = p[:w.limit]
	n, err := w.writer.Write(p)
	if err != nil {
		return n, err
	}
	w.limit -= int64(n)
	return n, errLimitReached
}
