package llvm_cache

import (
	"fmt"

	casv1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/cas/v1"
	keyvaluev1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/keyvalue/v1"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

type Factory struct{}

func (Factory) ID() string {
	return "llvm-cache"
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

	store := newCacheStore(p.backend, p.urlProxy)
	casv1.RegisterCASDBServiceServer(grpcRegistrar, newCASService(store))
	keyvaluev1.RegisterKeyValueDBServer(grpcRegistrar, newKVService(store))
	return nil
}
