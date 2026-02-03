package bazel_remote_asset

import (
	"bytes"
	"context"
	"errors"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type casService struct {
	remoteexecution.UnimplementedContentAddressableStorageServer
	cas *casStore
}

func newCASService(cas *casStore) *casService {
	return &casService{cas: cas}
}

func (s *casService) FindMissingBlobs(ctx context.Context, req *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	missing := make([]*remoteexecution.Digest, 0)
	for _, digest := range req.GetBlobDigests() {
		if digest == nil || digest.GetHash() == "" {
			continue
		}
		exists, err := s.cas.has(ctx, assetDigest{Hash: digest.GetHash(), SizeBytes: digest.GetSizeBytes()})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "find missing blobs: %v", err)
		}
		if !exists {
			missing = append(missing, digest)
		}
	}
	return &remoteexecution.FindMissingBlobsResponse{MissingBlobDigests: missing}, nil
}

func (s *casService) BatchReadBlobs(ctx context.Context, req *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	responses := make([]*remoteexecution.BatchReadBlobsResponse_Response, 0, len(req.GetDigests()))
	for _, digest := range req.GetDigests() {
		if digest == nil || digest.GetHash() == "" {
			continue
		}

		data, err := s.cas.readAll(ctx, assetDigest{Hash: digest.GetHash(), SizeBytes: digest.GetSizeBytes()})
		if err != nil {
			code := codes.Internal
			if errors.Is(err, storage.ErrCacheNotFound) {
				code = codes.NotFound
			}
			responses = append(responses, &remoteexecution.BatchReadBlobsResponse_Response{
				Digest: digest,
				Status: &statuspb.Status{Code: int32(code)},
			})
			continue
		}

		responses = append(responses, &remoteexecution.BatchReadBlobsResponse_Response{
			Digest: digest,
			Data:   data,
			Status: &statuspb.Status{Code: int32(codes.OK)},
		})
	}

	return &remoteexecution.BatchReadBlobsResponse{Responses: responses}, nil
}

func (s *casService) BatchUpdateBlobs(ctx context.Context, req *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	responses := make([]*remoteexecution.BatchUpdateBlobsResponse_Response, 0, len(req.GetRequests()))
	for _, request := range req.GetRequests() {
		digest := request.GetDigest()
		if digest == nil || digest.GetHash() == "" {
			responses = append(responses, &remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: digest,
				Status: &statuspb.Status{Code: int32(codes.InvalidArgument), Message: "digest is required"},
			})
			continue
		}
		if int64(len(request.GetData())) != digest.GetSizeBytes() {
			responses = append(responses, &remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: digest,
				Status: &statuspb.Status{Code: int32(codes.InvalidArgument), Message: "size mismatch"},
			})
			continue
		}

		if err := s.cas.save(ctx, assetDigest{Hash: digest.GetHash(), SizeBytes: digest.GetSizeBytes()}, bytes.NewReader(request.GetData()), digest.GetSizeBytes()); err != nil {
			responses = append(responses, &remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: digest,
				Status: &statuspb.Status{Code: int32(codes.Internal), Message: err.Error()},
			})
			continue
		}

		responses = append(responses, &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest: digest,
			Status: &statuspb.Status{Code: int32(codes.OK)},
		})
	}

	return &remoteexecution.BatchUpdateBlobsResponse{Responses: responses}, nil
}
