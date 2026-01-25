package ghacachev2

import (
	"fmt"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

// Factory wires the gha-cache-v2 (GitHub Actions cache v2) protocol.
// Twirp JSON endpoints (cache.PathPrefix()):
//
//	POST /twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry
//	POST /twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload
//	POST /twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL
type Factory struct{}

func (Factory) ID() string {
	return "gha-cache-v2"
}

func (Factory) New(deps protocols.Dependencies) (protocols.Protocol, error) {
	deps = deps.WithDefaults()
	return &protocol{backend: deps.Storage, host: deps.Host}, nil
}

type protocol struct {
	backend storage.BlobStorageBackend
	host    string
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	mux := registrar.HTTP()
	if mux == nil {
		return fmt.Errorf("http mux is nil")
	}

	cache := New(p.host, p.backend)
	mux.Handle("POST "+cache.PathPrefix(), cache)
	return nil
}
