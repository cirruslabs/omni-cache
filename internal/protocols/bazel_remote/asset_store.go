package bazel_remote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	remoteasset "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/asset/v1"
	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

type assetStore struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
}

type blobMapping struct {
	URI            string `json:"uri"`
	DigestHash     string `json:"digest_hash"`
	DigestSize     int64  `json:"digest_size_bytes"`
	DigestFunction string `json:"digest_function"`
}

func newAssetStore(backend storage.BlobStorageBackend, proxy *urlproxy.Proxy) *assetStore {
	return &assetStore{backend: backend, proxy: proxy}
}

func (s *assetStore) PutBlobMapping(
	ctx context.Context,
	instanceName string,
	uri string,
	qualifiers []*remoteasset.Qualifier,
	digest *remoteexecution.Digest,
) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	if strings.TrimSpace(uri) == "" {
		return fmt.Errorf("uri is empty")
	}

	digest, err := normalizeDigest(digest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return err
	}

	mapping := blobMapping{
		URI:            uri,
		DigestHash:     digest.GetHash(),
		DigestSize:     digest.GetSizeBytes(),
		DigestFunction: "sha256",
	}

	payload, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	key := blobMappingObjectKey(instanceName, uri, qualifiers)
	info, err := s.backend.UploadURL(ctx, key, nil)
	if err != nil {
		return err
	}

	return s.proxy.UploadFromReader(ctx, info, key, bytes.NewReader(payload), int64(len(payload)))
}

func (s *assetStore) GetBlobMapping(
	ctx context.Context,
	instanceName string,
	uri string,
	qualifiers []*remoteasset.Qualifier,
) (*remoteexecution.Digest, bool, error) {
	if s.backend == nil {
		return nil, false, fmt.Errorf("storage backend is nil")
	}
	if strings.TrimSpace(uri) == "" {
		return nil, false, fmt.Errorf("uri is empty")
	}

	key := blobMappingObjectKey(instanceName, uri, qualifiers)
	infos, err := s.backend.DownloadURLs(ctx, key)
	if err != nil {
		if storage.IsNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	var (
		payload bytes.Buffer
		lastErr error
	)
	for _, info := range infos {
		payload.Reset()
		if err := s.proxy.DownloadToWriter(ctx, info, key, &payload); err == nil {
			lastErr = nil
			break
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, false, lastErr
	}
	if payload.Len() == 0 {
		return nil, false, nil
	}

	var mapping blobMapping
	if err := json.Unmarshal(payload.Bytes(), &mapping); err != nil {
		return nil, false, err
	}

	digest, err := normalizeDigest(
		&remoteexecution.Digest{
			Hash:      mapping.DigestHash,
			SizeBytes: mapping.DigestSize,
		},
		remoteexecution.DigestFunction_SHA256,
	)
	if err != nil {
		return nil, false, err
	}

	return digest, true, nil
}

func blobMappingObjectKey(instanceName string, uri string, qualifiers []*remoteasset.Qualifier) string {
	key := canonicalAssetKey("blob", instanceName, uri, qualifiers, remoteexecution.DigestFunction_SHA256)
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("bazel/asset/v1/%s/blob/%s.json", encodeInstance(instanceName), hex.EncodeToString(sum[:]))
}

func canonicalAssetKey(
	kind string,
	instanceName string,
	uri string,
	qualifiers []*remoteasset.Qualifier,
	digestFunction remoteexecution.DigestFunction_Value,
) string {
	normalized := make([]*remoteasset.Qualifier, 0, len(qualifiers))
	for _, qualifier := range qualifiers {
		if qualifier == nil {
			continue
		}
		normalized = append(normalized, &remoteasset.Qualifier{Name: qualifier.GetName(), Value: qualifier.GetValue()})
	}

	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].GetName() == normalized[j].GetName() {
			return normalized[i].GetValue() < normalized[j].GetValue()
		}
		return normalized[i].GetName() < normalized[j].GetName()
	})

	var builder strings.Builder
	builder.WriteString(kind)
	builder.WriteString("\n")
	builder.WriteString(instanceName)
	builder.WriteString("\n")
	builder.WriteString(uri)
	builder.WriteString("\n")
	builder.WriteString(strings.ToLower(digestFunction.String()))
	for _, qualifier := range normalized {
		builder.WriteString("\n")
		builder.WriteString(qualifier.GetName())
		builder.WriteString("=")
		builder.WriteString(qualifier.GetValue())
	}

	return builder.String()
}
