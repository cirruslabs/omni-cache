package ghacache

import (
	"fmt"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type GHACacheProtocolFactory struct {
	protocols.CachingProtocolFactory
}

func (factory *GHACacheProtocolFactory) ID() string {
	return "gha-cache"
}

func (factory *GHACacheProtocolFactory) NewInstance(storagBackend storage.BlobStorageBackend, httpClient *http.Client) (protocols.CachingProtocol, error) {
	backend, ok := storagBackend.(cacheBackend)
	if !ok {
		return nil, fmt.Errorf("gha-cache requires multipart storage backend with cache info support")
	}

	return &internalGHACache{
		backend:    backend,
		httpClient: httpClient,
	}, nil
}

type internalGHACache struct {
	protocols.CachingProtocol
	backend    cacheBackend
	httpClient *http.Client
}

func (cache *internalGHACache) Register(mux *http.ServeMux) error {
	ghaCache := New("", cache.backend, cache.httpClient)
	handler := http.StripPrefix(APIMountPoint, ghaCache)
	mux.Handle("GET "+APIMountPoint+"/cache", handler)
	mux.Handle("POST "+APIMountPoint+"/caches", handler)
	mux.Handle("PATCH "+APIMountPoint+"/caches/{id}", handler)
	mux.Handle("POST "+APIMountPoint+"/caches/{id}", handler)
	return nil
}
