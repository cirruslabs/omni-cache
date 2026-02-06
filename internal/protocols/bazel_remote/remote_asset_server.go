package bazel_remote

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	remoteasset "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/asset/v1"
	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxOriginFetchTimeout = 10 * time.Minute

type remoteAssetServer struct {
	remoteasset.UnimplementedFetchServer
	remoteasset.UnimplementedPushServer

	cas    *casStore
	assets *assetStore
	http   *http.Client
}

func newRemoteAssetServer(cas *casStore, assets *assetStore, httpClient *http.Client) *remoteAssetServer {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &remoteAssetServer{
		cas:    cas,
		assets: assets,
		http:   httpClient,
	}
}

func (s *remoteAssetServer) FetchBlob(ctx context.Context, req *remoteasset.FetchBlobRequest) (*remoteasset.FetchBlobResponse, error) {
	if len(req.GetUris()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one URI is required")
	}
	if err := validateQualifierNames(req.GetQualifiers()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid qualifiers: %v", err)
	}
	if _, err := normalizeDigestFunction(req.GetDigestFunction(), ""); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid digest function: %v", err)
	}
	if req.GetTimeout() != nil && req.GetTimeout().AsDuration() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "timeout must be positive")
	}

	for _, uri := range req.GetUris() {
		digest, ok, err := s.assets.GetBlobMapping(ctx, req.GetInstanceName(), uri, req.GetQualifiers())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "lookup mapping: %v", err)
		}
		if !ok {
			continue
		}

		exists, err := s.cas.Exists(ctx, req.GetInstanceName(), digest)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "check mapped blob: %v", err)
		}
		if !exists {
			continue
		}

		return &remoteasset.FetchBlobResponse{
			Status:         rpcStatus(codes.OK, ""),
			Uri:            uri,
			BlobDigest:     cloneDigest(digest),
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}, nil
	}

	var (
		lastStatus *statuspb.Status
		lastURI    string
		attempted  bool
		sawHTTPURI bool
	)

	for _, candidate := range req.GetUris() {
		parsed, err := url.Parse(candidate)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid URI %q: %v", candidate, err)
		}

		scheme := strings.ToLower(parsed.Scheme)
		if scheme != "http" && scheme != "https" {
			continue
		}
		sawHTTPURI = true
		attempted = true

		digest, fetchStatus, err := s.fetchAndStoreFromOrigin(ctx, req, candidate)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "origin fetch failed: %v", err)
		}
		if fetchStatus != nil && fetchStatus.GetCode() != int32(codes.OK) {
			lastStatus = fetchStatus
			lastURI = candidate
			continue
		}

		for _, uri := range req.GetUris() {
			if err := s.assets.PutBlobMapping(ctx, req.GetInstanceName(), uri, req.GetQualifiers(), digest); err != nil {
				return nil, status.Errorf(codes.Internal, "store mapping for %q: %v", uri, err)
			}
		}

		return &remoteasset.FetchBlobResponse{
			Status:         rpcStatus(codes.OK, ""),
			Uri:            candidate,
			BlobDigest:     cloneDigest(digest),
			DigestFunction: remoteexecution.DigestFunction_SHA256,
		}, nil
	}

	if !sawHTTPURI {
		return nil, status.Error(codes.InvalidArgument, "no fetchable URI scheme found; only http/https are supported")
	}
	if attempted && lastStatus != nil {
		return &remoteasset.FetchBlobResponse{Status: lastStatus, Uri: lastURI}, nil
	}

	return &remoteasset.FetchBlobResponse{
		Status: rpcStatus(codes.NotFound, "asset not found"),
	}, nil
}

func (s *remoteAssetServer) FetchDirectory(context.Context, *remoteasset.FetchDirectoryRequest) (*remoteasset.FetchDirectoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "FetchDirectory is not implemented")
}

func (s *remoteAssetServer) PushBlob(ctx context.Context, req *remoteasset.PushBlobRequest) (*remoteasset.PushBlobResponse, error) {
	if len(req.GetUris()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one URI is required")
	}
	if err := validateQualifierNames(req.GetQualifiers()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid qualifiers: %v", err)
	}

	digest, err := normalizeDigest(req.GetBlobDigest(), req.GetDigestFunction())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid blob digest: %v", err)
	}

	for _, uri := range req.GetUris() {
		if strings.TrimSpace(uri) == "" {
			return nil, status.Error(codes.InvalidArgument, "URI must not be empty")
		}
		if err := s.assets.PutBlobMapping(ctx, req.GetInstanceName(), uri, req.GetQualifiers(), digest); err != nil {
			return nil, status.Errorf(codes.Internal, "store mapping for %q: %v", uri, err)
		}
	}

	return &remoteasset.PushBlobResponse{}, nil
}

func (s *remoteAssetServer) PushDirectory(context.Context, *remoteasset.PushDirectoryRequest) (*remoteasset.PushDirectoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "PushDirectory is not implemented")
}

func (s *remoteAssetServer) fetchAndStoreFromOrigin(
	ctx context.Context,
	req *remoteasset.FetchBlobRequest,
	uri string,
) (*remoteexecution.Digest, *statuspb.Status, error) {
	requestContext := ctx
	cancel := func() {}
	if req.GetTimeout() != nil {
		requested := req.GetTimeout().AsDuration()
		if requested > maxOriginFetchTimeout {
			requested = maxOriginFetchTimeout
		}
		requestContext, cancel = context.WithTimeout(ctx, requested)
	}
	defer cancel()

	httpRequest, err := http.NewRequestWithContext(requestContext, http.MethodGet, uri, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid URI %q: %w", uri, err)
	}

	response, err := s.http.Do(httpRequest)
	if err != nil {
		if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
			return nil, rpcStatus(codes.DeadlineExceeded, requestContext.Err().Error()), nil
		}
		return nil, rpcStatus(codes.Unavailable, err.Error()), nil
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, statusFromOriginHTTP(response.StatusCode), nil
	}

	tmpFile, err := os.CreateTemp("", "omni-cache-bazel-origin-*")
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmpFile, hasher), response.Body)
	if err != nil {
		if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
			return nil, rpcStatus(codes.DeadlineExceeded, requestContext.Err().Error()), nil
		}
		return nil, rpcStatus(codes.Unavailable, err.Error()), nil
	}

	digest := &remoteexecution.Digest{
		Hash:      hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes: size,
	}

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}
	if err := s.cas.Upload(requestContext, req.GetInstanceName(), digest, tmpFile); err != nil {
		return nil, nil, err
	}

	return digest, rpcStatus(codes.OK, ""), nil
}

func validateQualifierNames(qualifiers []*remoteasset.Qualifier) error {
	seen := make(map[string]struct{}, len(qualifiers))
	for _, qualifier := range qualifiers {
		if qualifier == nil {
			return fmt.Errorf("qualifier is nil")
		}
		if _, ok := seen[qualifier.GetName()]; ok {
			return fmt.Errorf("duplicate qualifier name %q", qualifier.GetName())
		}
		seen[qualifier.GetName()] = struct{}{}
	}
	return nil
}

func statusFromOriginHTTP(statusCode int) *statuspb.Status {
	switch {
	case statusCode == http.StatusNotFound:
		return rpcStatus(codes.NotFound, http.StatusText(statusCode))
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return rpcStatus(codes.PermissionDenied, http.StatusText(statusCode))
	case statusCode == http.StatusTooManyRequests:
		return rpcStatus(codes.ResourceExhausted, http.StatusText(statusCode))
	case statusCode >= http.StatusInternalServerError:
		return rpcStatus(codes.Unavailable, http.StatusText(statusCode))
	default:
		return rpcStatus(codes.Unavailable, http.StatusText(statusCode))
	}
}
