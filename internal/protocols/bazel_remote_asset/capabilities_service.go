package bazel_remote_asset

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type capabilitiesService struct {
	remoteexecution.UnimplementedCapabilitiesServer
}

func newCapabilitiesService() *capabilitiesService {
	return &capabilitiesService{}
}

func (s *capabilitiesService) GetCapabilities(ctx context.Context, req *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: []remoteexecution.DigestFunction_Value{
				remoteexecution.DigestFunction_SHA256,
			},
		},
	}, nil
}
