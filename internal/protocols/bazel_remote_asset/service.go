package bazel_remote_asset

import (
	"context"
	"errors"
	"net/http"
	"time"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type assetService struct {
	remoteasset.UnimplementedFetchServer
	remoteasset.UnimplementedPushServer

	store *assetStore
	cas   *casStore
	http  *http.Client
	now   func() time.Time
}

func newAssetService(store *assetStore, cas *casStore, httpClient *http.Client) *assetService {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &assetService{
		store: store,
		cas:   cas,
		http:  httpClient,
		now:   time.Now,
	}
}

func (s *assetService) FetchBlob(ctx context.Context, req *remoteasset.FetchBlobRequest) (*remoteasset.FetchBlobResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	uris, err := validateURIs(req.GetUris())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	qualifiers, err := normalizeQualifiers(req.GetQualifiers())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	normalizedFn := normalizeDigestFunction(req.GetDigestFunction())
	if normalizedFn != remoteexecution.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported digest function %v", req.GetDigestFunction())
	}

	oldestAccepted := time.Time{}
	if req.GetOldestContentAccepted() != nil {
		oldestAccepted = req.GetOldestContentAccepted().AsTime()
	}

	for _, uri := range uris {
		key := assetStorageKey(assetKindBlob, req.GetInstanceName(), uri, qualifiers)
		record, err := s.store.load(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrCacheNotFound) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "fetch blob failed: %v", err)
		}
		if record.isExpired(s.now()) || record.isTooOld(oldestAccepted) {
			continue
		}

		stats.Default().RecordCacheHit()
		return &remoteasset.FetchBlobResponse{
			Status:         okStatus(),
			Uri:            uri,
			Qualifiers:     qualifiersToProto(record.Qualifiers),
			ExpiresAt:      record.expiresAtProto(),
			BlobDigest:     record.Digest.toProto(),
			DigestFunction: record.digestFunction(),
		}, nil
	}

	fetchCtx := ctx
	if req.GetTimeout() != nil {
		timeout := req.GetTimeout().AsDuration()
		if timeout > 0 {
			var cancel context.CancelFunc
			fetchCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
	}

	var lastStatus *statuspb.Status
	for _, uri := range uris {
		record, fetchStatus, err := s.fetchFromOrigin(fetchCtx, uri, req.GetInstanceName(), qualifiers, normalizedFn)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "fetch blob failed: %v", err)
		}
		if record != nil {
			stats.Default().RecordCacheMiss()
			return &remoteasset.FetchBlobResponse{
				Status:         okStatus(),
				Uri:            uri,
				Qualifiers:     qualifiersToProto(record.Qualifiers),
				ExpiresAt:      record.expiresAtProto(),
				BlobDigest:     record.Digest.toProto(),
				DigestFunction: record.digestFunction(),
			}, nil
		}
		if fetchStatus != nil {
			lastStatus = fetchStatus
		}
	}

	stats.Default().RecordCacheMiss()
	if lastStatus == nil {
		lastStatus = notFoundStatus()
	}
	return &remoteasset.FetchBlobResponse{Status: lastStatus}, nil
}

func (s *assetService) FetchDirectory(ctx context.Context, req *remoteasset.FetchDirectoryRequest) (*remoteasset.FetchDirectoryResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	uris, err := validateURIs(req.GetUris())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	qualifiers, err := normalizeQualifiers(req.GetQualifiers())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	oldestAccepted := time.Time{}
	if req.GetOldestContentAccepted() != nil {
		oldestAccepted = req.GetOldestContentAccepted().AsTime()
	}

	for _, uri := range uris {
		key := assetStorageKey(assetKindDirectory, req.GetInstanceName(), uri, qualifiers)
		record, err := s.store.load(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrCacheNotFound) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "fetch directory failed: %v", err)
		}
		if record.isExpired(s.now()) || record.isTooOld(oldestAccepted) {
			continue
		}

		stats.Default().RecordCacheHit()
		return &remoteasset.FetchDirectoryResponse{
			Status:              okStatus(),
			Uri:                 uri,
			Qualifiers:          qualifiersToProto(record.Qualifiers),
			ExpiresAt:           record.expiresAtProto(),
			RootDirectoryDigest: record.Digest.toProto(),
			DigestFunction:      record.digestFunction(),
		}, nil
	}

	stats.Default().RecordCacheMiss()
	return &remoteasset.FetchDirectoryResponse{Status: notFoundStatus()}, nil
}

func (s *assetService) PushBlob(ctx context.Context, req *remoteasset.PushBlobRequest) (*remoteasset.PushBlobResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	uris, err := validateURIs(req.GetUris())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	qualifiers, err := normalizeQualifiers(req.GetQualifiers())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	digest, err := digestFromProto(req.GetBlobDigest())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	normalizedFn := normalizeDigestFunction(req.GetDigestFunction())
	if normalizedFn != remoteexecution.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported digest function %v", req.GetDigestFunction())
	}

	record := assetRecord{
		Kind:         assetKindBlob,
		InstanceName: req.GetInstanceName(),
		Qualifiers:   qualifiers,
		Digest:       digest,
		DigestFunc:   int32(normalizedFn),
		ExpiresAt:    expireAtFromProto(req.GetExpireAt()),
		PushedAt:     s.now().UTC(),
	}

	for _, uri := range uris {
		record.URI = uri
		key := assetStorageKey(assetKindBlob, req.GetInstanceName(), uri, qualifiers)
		if err := s.store.save(ctx, key, &record); err != nil {
			return nil, status.Errorf(codes.Internal, "push blob failed: %v", err)
		}
	}

	return &remoteasset.PushBlobResponse{}, nil
}

func (s *assetService) PushDirectory(ctx context.Context, req *remoteasset.PushDirectoryRequest) (*remoteasset.PushDirectoryResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	uris, err := validateURIs(req.GetUris())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	qualifiers, err := normalizeQualifiers(req.GetQualifiers())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	digest, err := digestFromProto(req.GetRootDirectoryDigest())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	normalizedFn := normalizeDigestFunction(req.GetDigestFunction())
	if normalizedFn != remoteexecution.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported digest function %v", req.GetDigestFunction())
	}

	record := assetRecord{
		Kind:         assetKindDirectory,
		InstanceName: req.GetInstanceName(),
		Qualifiers:   qualifiers,
		Digest:       digest,
		DigestFunc:   int32(normalizedFn),
		ExpiresAt:    expireAtFromProto(req.GetExpireAt()),
		PushedAt:     s.now().UTC(),
	}

	for _, uri := range uris {
		record.URI = uri
		key := assetStorageKey(assetKindDirectory, req.GetInstanceName(), uri, qualifiers)
		if err := s.store.save(ctx, key, &record); err != nil {
			return nil, status.Errorf(codes.Internal, "push directory failed: %v", err)
		}
	}

	return &remoteasset.PushDirectoryResponse{}, nil
}

func okStatus() *statuspb.Status {
	return &statuspb.Status{Code: int32(codes.OK)}
}

func notFoundStatus() *statuspb.Status {
	return &statuspb.Status{
		Code:    int32(codes.NotFound),
		Message: "asset not found",
	}
}

func expireAtFromProto(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	expireAt := ts.AsTime()
	if expireAt.IsZero() {
		return nil
	}
	return &expireAt
}
