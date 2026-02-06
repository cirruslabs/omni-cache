package bazel_remote

import (
	"context"
	"errors"
	"fmt"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type casServer struct {
	remoteexecution.UnimplementedContentAddressableStorageServer
	store *casStore
}

func newCASServer(store *casStore) *casServer {
	return &casServer{store: store}
}

func (s *casServer) FindMissingBlobs(ctx context.Context, req *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	missing := make([]*remoteexecution.Digest, 0)
	for _, requested := range req.GetBlobDigests() {
		digest, err := normalizeDigest(requested, req.GetDigestFunction())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid digest: %v", err)
		}

		exists, err := s.store.Exists(ctx, req.GetInstanceName(), digest)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "check blob existence: %v", err)
		}
		if !exists {
			missing = append(missing, digest)
		}
	}

	return &remoteexecution.FindMissingBlobsResponse{MissingBlobDigests: missing}, nil
}

func (s *casServer) BatchUpdateBlobs(ctx context.Context, req *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	responses := make([]*remoteexecution.BatchUpdateBlobsResponse_Response, 0, len(req.GetRequests()))
	for _, request := range req.GetRequests() {
		response := &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest: request.GetDigest(),
			Status: rpcStatus(codes.OK, ""),
		}

		if request.GetCompressor() != remoteexecution.Compressor_IDENTITY {
			response.Status = rpcStatus(codes.InvalidArgument, "only IDENTITY compressor is supported")
			responses = append(responses, response)
			continue
		}

		digest, err := normalizeDigest(request.GetDigest(), req.GetDigestFunction())
		if err != nil {
			response.Status = rpcStatus(codes.InvalidArgument, fmt.Sprintf("invalid digest: %v", err))
			responses = append(responses, response)
			continue
		}
		response.Digest = digest

		if !digestMatchesData(digest, request.GetData()) {
			response.Status = rpcStatus(codes.InvalidArgument, "digest does not match uploaded data")
			responses = append(responses, response)
			continue
		}

		if err := s.store.UploadBytes(ctx, req.GetInstanceName(), digest, request.GetData()); err != nil {
			response.Status = rpcStatus(codes.Internal, fmt.Sprintf("upload failed: %v", err))
		}
		responses = append(responses, response)
	}

	return &remoteexecution.BatchUpdateBlobsResponse{Responses: responses}, nil
}

func (s *casServer) BatchReadBlobs(ctx context.Context, req *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	responses := make([]*remoteexecution.BatchReadBlobsResponse_Response, 0, len(req.GetDigests()))
	for _, requested := range req.GetDigests() {
		digest, err := normalizeDigest(requested, req.GetDigestFunction())
		if err != nil {
			responses = append(responses, &remoteexecution.BatchReadBlobsResponse_Response{
				Digest: requested,
				Status: rpcStatus(codes.InvalidArgument, fmt.Sprintf("invalid digest: %v", err)),
			})
			continue
		}

		response := &remoteexecution.BatchReadBlobsResponse_Response{
			Digest:     digest,
			Compressor: remoteexecution.Compressor_IDENTITY,
		}

		data, err := s.store.DownloadBytes(ctx, req.GetInstanceName(), digest)
		if err != nil {
			if errors.Is(err, storage.ErrCacheNotFound) {
				response.Status = rpcStatus(codes.NotFound, "blob not found")
			} else {
				response.Status = rpcStatus(codes.Internal, fmt.Sprintf("read failed: %v", err))
			}
			responses = append(responses, response)
			continue
		}

		response.Data = data
		response.Status = rpcStatus(codes.OK, "")
		responses = append(responses, response)
	}

	return &remoteexecution.BatchReadBlobsResponse{Responses: responses}, nil
}

func (s *casServer) GetTree(*remoteexecution.GetTreeRequest, grpc.ServerStreamingServer[remoteexecution.GetTreeResponse]) error {
	return status.Error(codes.Unimplemented, "GetTree is not implemented")
}

func (s *casServer) SplitBlob(context.Context, *remoteexecution.SplitBlobRequest) (*remoteexecution.SplitBlobResponse, error) {
	return nil, status.Error(codes.Unimplemented, "SplitBlob is not implemented")
}

func (s *casServer) SpliceBlob(context.Context, *remoteexecution.SpliceBlobRequest) (*remoteexecution.SpliceBlobResponse, error) {
	return nil, status.Error(codes.Unimplemented, "SpliceBlob is not implemented")
}

func rpcStatus(code codes.Code, message string) *statuspb.Status {
	return &statuspb.Status{Code: int32(code), Message: message}
}
