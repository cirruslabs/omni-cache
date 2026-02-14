package bazel_remote

import (
	"fmt"
	"net/http"

	remoteasset "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/asset/v1"
	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

// Factory wires Bazel REAPI cache and Remote Asset services.
type Factory struct{}

func (Factory) ID() string {
	return "bazel-remote"
}

func (Factory) New(deps protocols.Dependencies) (protocols.Protocol, error) {
	deps = deps.WithDefaults()
	return &protocol{
		backend: deps.Storage,
		proxy:   deps.URLProxy,
		http:    deps.HTTP,
	}, nil
}

type protocol struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
	http    *http.Client
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	grpcRegistrar := registrar.GRPC()
	if grpcRegistrar == nil {
		return fmt.Errorf("grpc registrar is nil")
	}

	grpcServer, ok := grpcRegistrar.(*grpc.Server)
	if !ok {
		return fmt.Errorf("grpc registrar is not *grpc.Server")
	}

	cas := newCASStore(p.backend, p.proxy)
	assets := newAssetStore(p.backend, p.proxy)

	remoteexecution.RegisterContentAddressableStorageServer(grpcRegistrar, newCASServer(cas))
	remoteexecution.RegisterCapabilitiesServer(grpcRegistrar, newCapabilitiesServer())
	bytestream.RegisterByteStreamServer(grpcServer, newByteStreamServer(cas))

	assetServer := newRemoteAssetServer(cas, assets, p.http)
	remoteasset.RegisterFetchServer(grpcRegistrar, assetServer)
	remoteasset.RegisterPushServer(grpcRegistrar, assetServer)

	return nil
}
