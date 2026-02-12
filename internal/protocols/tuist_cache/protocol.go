package tuist_cache

import (
	"fmt"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

// Factory wires the Tuist module cache HTTP protocol.
// Endpoints:
//
//	HEAD /api/cache/module/{id}
//	GET /api/cache/module/{id}
//	POST /api/cache/module/start
//	POST /api/cache/module/part
//	POST /api/cache/module/complete
type Factory struct{}

func (Factory) ID() string {
	return "tuist-cache"
}

func (Factory) New(deps protocols.Dependencies) (protocols.Protocol, error) {
	deps = deps.WithDefaults()

	backend, ok := deps.Storage.(storage.MultipartBlobStorageBackend)
	if !ok {
		return nil, fmt.Errorf("tuist-cache requires multipart storage backend")
	}

	return &protocol{
		cache: newTuistCache(backend, deps.HTTP, deps.URLProxy),
	}, nil
}

type protocol struct {
	cache *tuistCache
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	mux := registrar.HTTP()
	if mux == nil {
		return fmt.Errorf("http mux is nil")
	}

	mux.Handle("HEAD "+moduleArtifactPath, p.cache)
	mux.Handle("GET "+moduleArtifactPath, p.cache)
	mux.Handle("POST "+moduleStartPath, p.cache)
	mux.Handle("POST "+modulePartPath, p.cache)
	mux.Handle("POST "+moduleCompletePath, p.cache)
	return nil
}
