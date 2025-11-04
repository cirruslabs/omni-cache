package protocols

import (
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type CachingProtocolFactory interface {
	ID() string
	NewInstance(storagBackend storage.BlobStorageBacked, httpClient *http.Client) (CachingProtocol, error)
}

type CachingProtocol interface {
	Register(mux *http.ServeMux) error
}
