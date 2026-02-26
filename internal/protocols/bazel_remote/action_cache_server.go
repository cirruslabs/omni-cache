package bazel_remote

import (
	"context"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionCacheServer struct {
	remoteexecution.UnimplementedActionCacheServer
	store *actionCacheStore
}

func newActionCacheServer(store *actionCacheStore) *actionCacheServer {
	return &actionCacheServer{store: store}
}

func (s *actionCacheServer) GetActionResult(
	ctx context.Context,
	req *remoteexecution.GetActionResultRequest,
) (*remoteexecution.ActionResult, error) {
	digest, err := normalizeDigest(req.GetActionDigest(), req.GetDigestFunction())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid action digest: %v", err)
	}

	result, ok, err := s.store.GetActionResult(ctx, req.GetInstanceName(), digest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get action result: %v", err)
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "action result not found")
	}

	return result, nil
}

func (s *actionCacheServer) UpdateActionResult(
	ctx context.Context,
	req *remoteexecution.UpdateActionResultRequest,
) (*remoteexecution.ActionResult, error) {
	if req.GetActionResult() == nil {
		return nil, status.Error(codes.InvalidArgument, "action_result is required")
	}

	digest, err := normalizeDigest(req.GetActionDigest(), req.GetDigestFunction())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid action digest: %v", err)
	}

	if err := s.store.PutActionResult(ctx, req.GetInstanceName(), digest, req.GetActionResult()); err != nil {
		return nil, status.Errorf(codes.Internal, "store action result: %v", err)
	}

	return req.GetActionResult(), nil
}

var _ remoteexecution.ActionCacheServer = (*actionCacheServer)(nil)
