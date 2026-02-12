package tuist_cache

import (
	"fmt"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

// Factory wires the Tuist module cache HTTP protocol.
// Endpoints (behind the /tuist prefix):
//
//	HEAD /tuist/api/cache/module/{id}
//	GET /tuist/api/cache/module/{id}
//	POST /tuist/api/cache/module/start
//	POST /tuist/api/cache/module/part
//	POST /tuist/api/cache/module/complete
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

	cache, err := newTuistCache(backend, deps.HTTP)
	if err != nil {
		return nil, err
	}

	return &protocol{
		cache: cache,
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

	for _, method := range []string{
		"DELETE",
		"GET",
		"HEAD",
		"POST",
		"PUT",
	} {
		mux.Handle(method+" /tuist/api/cache/", p.cache)
	}
	return nil
}
