package bazel_remote_asset

import (
	"fmt"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

// Factory wires the Bazel Remote Asset gRPC services.
// Services:
//
//	build.bazel.remote.asset.v1.Fetch
//	build.bazel.remote.asset.v1.Push
//
// Served over h2c (plaintext HTTP/2) on the sidecar port.
type Factory struct{}

func (Factory) ID() string {
	return "bazel-remote-asset"
}

func (Factory) New(deps protocols.Dependencies) (protocols.Protocol, error) {
	deps = deps.WithDefaults()
	return &protocol{
		backend:  deps.Storage,
		urlProxy: deps.URLProxy,
	}, nil
}

type protocol struct {
	backend  storage.BlobStorageBackend
	urlProxy *urlproxy.Proxy
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	grpcRegistrar := registrar.GRPC()
	if grpcRegistrar == nil {
		return fmt.Errorf("grpc registrar is nil")
	}

	store := newAssetStore(p.backend, p.urlProxy)
	service := newAssetService(store)

	remoteasset.RegisterFetchServer(grpcRegistrar, service)
	remoteasset.RegisterPushServer(grpcRegistrar, service)
	return nil
}
