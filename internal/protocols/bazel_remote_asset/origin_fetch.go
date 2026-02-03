package bazel_remote_asset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"io"
	"net/http"
	"os"
	"strings"
)

func (s *assetService) fetchFromOrigin(ctx context.Context, uri, instanceName string, qualifiers []assetQualifier, fn remoteexecution.DigestFunction_Value) (*assetRecord, *statuspb.Status, error) {
	if uri == "" {
		return nil, nil, nil
	}

	normalizedFn := normalizeDigestFunction(fn)
	if normalizedFn != remoteexecution.DigestFunction_SHA256 {
		return nil, nil, fmt.Errorf("unsupported digest function %v", fn)
	}

	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		return nil, notFoundStatus(), nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, nil, err
	}

	resp, err := s.http.Do(req)
	if err != nil {
		return nil, &statuspb.Status{Code: int32(codes.Unavailable), Message: err.Error()}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, statusFromHTTP(resp.StatusCode), nil
	}

	tmp, err := os.CreateTemp("", "omni-cache-asset-*")
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = os.Remove(tmp.Name())
	}()
	defer tmp.Close()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, hasher), resp.Body)
	if err != nil {
		return nil, nil, err
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	digest := assetDigest{Hash: hash, SizeBytes: written}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}
	if err := s.cas.save(ctx, digest, tmp, written); err != nil {
		return nil, nil, err
	}

	record := assetRecord{
		Kind:         assetKindBlob,
		InstanceName: instanceName,
		URI:          uri,
		Qualifiers:   qualifiers,
		Digest:       digest,
		DigestFunc:   int32(normalizedFn),
		PushedAt:     s.now().UTC(),
	}

	if err := s.store.save(ctx, assetStorageKey(assetKindBlob, instanceName, uri, qualifiers), &record); err != nil {
		return nil, nil, err
	}

	return &record, nil, nil
}

func statusFromHTTP(code int) *statuspb.Status {
	switch code {
	case http.StatusNotFound:
		return &statuspb.Status{Code: int32(codes.NotFound), Message: "asset not found"}
	case http.StatusForbidden, http.StatusUnauthorized:
		return &statuspb.Status{Code: int32(codes.PermissionDenied), Message: "permission denied"}
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return &statuspb.Status{Code: int32(codes.DeadlineExceeded), Message: "deadline exceeded"}
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusBadGateway:
		return &statuspb.Status{Code: int32(codes.Unavailable), Message: "unavailable"}
	default:
		if code >= 400 && code < 500 {
			return &statuspb.Status{Code: int32(codes.NotFound), Message: fmt.Sprintf("asset returned %d", code)}
		}
		if code >= 500 {
			return &statuspb.Status{Code: int32(codes.Unavailable), Message: fmt.Sprintf("asset returned %d", code)}
		}
	}
	return &statuspb.Status{Code: int32(codes.Unknown), Message: fmt.Sprintf("unexpected status %d", code)}
}
