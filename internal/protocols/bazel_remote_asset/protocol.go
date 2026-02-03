package bazel_remote_asset

import (
	"fmt"
	"net/http"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
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
		http:     deps.HTTP,
	}, nil
}

type protocol struct {
	backend  storage.BlobStorageBackend
	urlProxy *urlproxy.Proxy
	http     *http.Client
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	grpcRegistrar := registrar.GRPC()
	if grpcRegistrar == nil {
		return fmt.Errorf("grpc registrar is nil")
	}

	store := newAssetStore(p.backend, p.urlProxy)
	cas := newCASStore(p.backend, p.urlProxy)
	service := newAssetService(store, cas, p.http)

	remoteasset.RegisterFetchServer(grpcRegistrar, service)
	remoteasset.RegisterPushServer(grpcRegistrar, service)
	remoteexecution.RegisterContentAddressableStorageServer(grpcRegistrar, newCASService(cas))
	remoteexecution.RegisterCapabilitiesServer(grpcRegistrar, newCapabilitiesService())
	grpcServer, ok := grpcRegistrar.(*grpc.Server)
	if !ok {
		return fmt.Errorf("grpc registrar is not *grpc.Server")
	}
	bytestream.RegisterByteStreamServer(grpcServer, newByteStreamService(cas))
	return nil
}
