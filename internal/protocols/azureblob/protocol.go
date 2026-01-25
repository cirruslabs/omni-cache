package azureblob

import (
	"fmt"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

// Factory wires the azure-blob compatibility protocol used by GHA cache v2 clients.
// Endpoints (under APIMountPoint):
//
//	GET /_azureblob/cirrus-runners-cache/{key...} (supports range requests)
//	HEAD /_azureblob/cirrus-runners-cache/{key...}
//	PUT /_azureblob/cirrus-runners-cache/{key...}
type Factory struct{}

func (Factory) ID() string {
	return "azure-blob"
}

func (Factory) New(deps protocols.Dependencies) (protocols.Protocol, error) {
	deps = deps.WithDefaults()

	backend, ok := deps.Storage.(storage.MultipartBlobStorageBackend)
	if !ok {
		return nil, fmt.Errorf("azure-blob requires multipart storage backend")
	}

	return &protocol{
		backend: backend,
		http:    deps.HTTP,
	}, nil
}

type protocol struct {
	backend storage.MultipartBlobStorageBackend
	http    *http.Client
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	mux := registrar.HTTP()
	if mux == nil {
		return fmt.Errorf("http mux is nil")
	}

	azure := New(p.backend, p.http)
	handler := http.StripPrefix(APIMountPoint, azure)

	mux.Handle("GET "+APIMountPoint+"/{key...}", handler)
	mux.Handle("HEAD "+APIMountPoint+"/{key...}", handler)
	mux.Handle("PUT "+APIMountPoint+"/{key...}", handler)
	return nil
}
