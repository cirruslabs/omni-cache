package ghacache

import (
	"fmt"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
)

// Factory wires the gha-cache (GitHub Actions cache v1) protocol.
// Endpoints (under APIMountPoint):
//
//	GET /_apis/artifactcache/cache
//	POST /_apis/artifactcache/caches
//	PATCH /_apis/artifactcache/caches/{id}
//	POST /_apis/artifactcache/caches/{id}
type Factory struct{}

func (Factory) ID() string {
	return "gha-cache"
}

func (Factory) New(deps protocols.Dependencies) (protocols.Protocol, error) {
	deps = deps.WithDefaults()

	backend, ok := deps.Storage.(cacheBackend)
	if !ok {
		return nil, fmt.Errorf("gha-cache requires multipart storage backend with cache info support")
	}

	return &protocol{
		backend: backend,
		http:    deps.HTTP,
	}, nil
}

type protocol struct {
	backend cacheBackend
	http    *http.Client
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	mux := registrar.HTTP()
	if mux == nil {
		return fmt.Errorf("http mux is nil")
	}

	ghaCache := New("", p.backend, p.http)
	handler := http.StripPrefix(APIMountPoint, ghaCache)
	mux.Handle("GET "+APIMountPoint+"/cache", handler)
	mux.Handle("POST "+APIMountPoint+"/caches", handler)
	mux.Handle("PATCH "+APIMountPoint+"/caches/{id}", handler)
	mux.Handle("POST "+APIMountPoint+"/caches/{id}", handler)
	return nil
}
