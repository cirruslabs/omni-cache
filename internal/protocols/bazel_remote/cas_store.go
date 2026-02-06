package bazel_remote

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

type casStore struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
}

func newCASStore(backend storage.BlobStorageBackend, proxy *urlproxy.Proxy) *casStore {
	return &casStore{backend: backend, proxy: proxy}
}

func (s *casStore) Exists(ctx context.Context, instanceName string, digest *remoteexecution.Digest) (bool, error) {
	if s.backend == nil {
		return false, fmt.Errorf("storage backend is nil")
	}

	digest, err := normalizeDigest(digest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return false, err
	}
	if isEmptyDigest(digest) {
		return true, nil
	}

	if _, err := s.backend.CacheInfo(ctx, casObjectKey(instanceName, digest), nil); err != nil {
		if storage.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (s *casStore) UploadBytes(ctx context.Context, instanceName string, digest *remoteexecution.Digest, data []byte) error {
	if !digestMatchesData(digest, data) {
		return fmt.Errorf("digest does not match data")
	}

	return s.Upload(ctx, instanceName, digest, bytes.NewReader(data))
}

func (s *casStore) Upload(ctx context.Context, instanceName string, digest *remoteexecution.Digest, r io.Reader) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	if r == nil {
		return fmt.Errorf("upload reader is nil")
	}

	digest, err := normalizeDigest(digest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return err
	}
	if isEmptyDigest(digest) {
		return nil
	}

	key := casObjectKey(instanceName, digest)
	info, err := s.backend.UploadURL(ctx, key, nil)
	if err != nil {
		return err
	}

	return s.proxy.UploadFromReader(ctx, info, key, r, digest.GetSizeBytes())
}

func (s *casStore) DownloadBytes(ctx context.Context, instanceName string, digest *remoteexecution.Digest) ([]byte, error) {
	if isEmptyDigest(digest) {
		return nil, nil
	}

	var buffer bytes.Buffer
	if err := s.DownloadToWriter(ctx, instanceName, digest, &buffer); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (s *casStore) DownloadToWriter(ctx context.Context, instanceName string, digest *remoteexecution.Digest, w io.Writer) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	if w == nil {
		return fmt.Errorf("download writer is nil")
	}

	digest, err := normalizeDigest(digest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return err
	}
	if isEmptyDigest(digest) {
		return nil
	}

	key := casObjectKey(instanceName, digest)
	infos, err := s.backend.DownloadURLs(ctx, key)
	if err != nil {
		if storage.IsNotFoundError(err) {
			return storage.ErrCacheNotFound
		}
		return err
	}
	if len(infos) == 0 {
		return storage.ErrCacheNotFound
	}

	var lastErr error
	for _, info := range infos {
		if err := s.proxy.DownloadToWriter(ctx, info, key, w); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	if lastErr == nil {
		return storage.ErrCacheNotFound
	}
	if errors.Is(lastErr, storage.ErrCacheNotFound) {
		return storage.ErrCacheNotFound
	}
	if strings.Contains(strings.ToLower(lastErr.Error()), "404") {
		return storage.ErrCacheNotFound
	}

	return lastErr
}

func casObjectKey(instanceName string, digest *remoteexecution.Digest) string {
	return fmt.Sprintf("bazel/cas/v2/%s/sha256/%s/%d", encodeInstance(instanceName), digest.GetHash(), digest.GetSizeBytes())
}

func encodeInstance(instanceName string) string {
	if strings.TrimSpace(instanceName) == "" {
		return "_"
	}
	return base64.RawURLEncoding.EncodeToString([]byte(instanceName))
}
