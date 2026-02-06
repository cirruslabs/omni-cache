package bazel_remote

import (
	"context"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	semver "github.com/cirruslabs/omni-cache/internal/api/build/bazel/semver"
)

type capabilitiesServer struct {
	remoteexecution.UnimplementedCapabilitiesServer
}

func newCapabilitiesServer() *capabilitiesServer {
	return &capabilitiesServer{}
}

func (s *capabilitiesServer) GetCapabilities(context.Context, *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: []remoteexecution.DigestFunction_Value{remoteexecution.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
				UpdateEnabled: false,
			},
			MaxBatchTotalSizeBytes:          0,
			SupportedCompressors:            nil,
			SupportedBatchUpdateCompressors: nil,
			SplitBlobSupport:                false,
			SpliceBlobSupport:               false,
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0, Patch: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 3, Patch: 0},
	}, nil
}
