package bazel_remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	"google.golang.org/protobuf/proto"
)

type actionCacheStore struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
}

func newActionCacheStore(backend storage.BlobStorageBackend, proxy *urlproxy.Proxy) *actionCacheStore {
	return &actionCacheStore{backend: backend, proxy: proxy}
}

func (s *actionCacheStore) PutActionResult(
	ctx context.Context,
	instanceName string,
	actionDigest *remoteexecution.Digest,
	actionResult *remoteexecution.ActionResult,
) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	if actionResult == nil {
		return fmt.Errorf("action result is nil")
	}

	digest, err := normalizeDigest(actionDigest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return err
	}

	payload, err := proto.Marshal(actionResult)
	if err != nil {
		return err
	}

	key := actionResultObjectKey(instanceName, digest)
	info, err := s.backend.UploadURL(ctx, key, nil)
	if err != nil {
		return err
	}

	return s.proxy.UploadFromReader(ctx, info, key, bytes.NewReader(payload), int64(len(payload)))
}

func (s *actionCacheStore) GetActionResult(
	ctx context.Context,
	instanceName string,
	actionDigest *remoteexecution.Digest,
) (*remoteexecution.ActionResult, bool, error) {
	if s.backend == nil {
		return nil, false, fmt.Errorf("storage backend is nil")
	}

	digest, err := normalizeDigest(actionDigest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return nil, false, err
	}

	key := actionResultObjectKey(instanceName, digest)
	infos, err := s.backend.DownloadURLs(ctx, key)
	if err != nil {
		if storage.IsNotFoundError(err) {
			stats.Default().RecordCacheMiss()
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(infos) == 0 {
		stats.Default().RecordCacheMiss()
		return nil, false, nil
	}

	var (
		buffer  bytes.Buffer
		lastErr error
	)
	for _, info := range infos {
		buffer.Reset()
		if err := s.proxy.DownloadToWriter(ctx, info, key, &buffer); err == nil {
			var result remoteexecution.ActionResult
			if err := proto.Unmarshal(buffer.Bytes(), &result); err != nil {
				return nil, false, err
			}

			referencedBlobsPresent, err := s.actionResultBlobsExist(ctx, instanceName, &result)
			if err != nil {
				return nil, false, err
			}
			if !referencedBlobsPresent {
				stats.Default().RecordCacheMiss()
				return nil, false, nil
			}

			stats.Default().RecordCacheHit()
			return &result, true, nil
		} else {
			lastErr = err
		}
	}

	if lastErr == nil {
		stats.Default().RecordCacheMiss()
		return nil, false, nil
	}
	if errors.Is(lastErr, storage.ErrCacheNotFound) {
		stats.Default().RecordCacheMiss()
		return nil, false, nil
	}
	if strings.Contains(strings.ToLower(lastErr.Error()), "404") {
		stats.Default().RecordCacheMiss()
		return nil, false, nil
	}

	return nil, false, lastErr
}

func actionResultObjectKey(instanceName string, digest *remoteexecution.Digest) string {
	return fmt.Sprintf("bazel/ac/v2/%s/sha256/%s/%d.pb", encodeInstance(instanceName), digest.GetHash(), digest.GetSizeBytes())
}

func (s *actionCacheStore) actionResultBlobsExist(
	ctx context.Context,
	instanceName string,
	result *remoteexecution.ActionResult,
) (bool, error) {
	if result == nil {
		return false, nil
	}

	type digestKey struct {
		hash      string
		sizeBytes int64
	}
	checkedDigests := make(map[digestKey]struct{})
	checkDigest := func(digest *remoteexecution.Digest) (bool, error) {
		if digest == nil {
			return true, nil
		}

		normalized, err := normalizeDigest(digest, remoteexecution.DigestFunction_SHA256)
		if err != nil {
			return false, nil
		}

		key := digestKey{
			hash:      normalized.GetHash(),
			sizeBytes: normalized.GetSizeBytes(),
		}
		if _, ok := checkedDigests[key]; ok {
			return true, nil
		}
		checkedDigests[key] = struct{}{}

		exists, err := s.casBlobExists(ctx, instanceName, normalized)
		if err != nil {
			return false, err
		}
		return exists, nil
	}

	blobDigests := make([]*remoteexecution.Digest, 0, len(result.GetOutputFiles())+len(result.GetOutputDirectories())+2)
	blobDigests = append(blobDigests, result.GetStdoutDigest(), result.GetStderrDigest())
	for _, outputFile := range result.GetOutputFiles() {
		blobDigests = append(blobDigests, outputFile.GetDigest())
	}
	for _, outputDirectory := range result.GetOutputDirectories() {
		blobDigests = append(blobDigests, outputDirectory.GetTreeDigest())
	}

	for _, digest := range blobDigests {
		exists, err := checkDigest(digest)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
	}

	return true, nil
}

func (s *actionCacheStore) casBlobExists(
	ctx context.Context,
	instanceName string,
	digest *remoteexecution.Digest,
) (bool, error) {
	if s.backend == nil {
		return false, fmt.Errorf("storage backend is nil")
	}

	normalized, err := normalizeDigest(digest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return false, err
	}
	if isEmptyDigest(normalized) {
		return true, nil
	}

	if _, err := s.backend.CacheInfo(ctx, casObjectKey(instanceName, normalized), nil); err != nil {
		if storage.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
