package protocols

import (
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

type Environment struct {
	Storage    storage.BlobStorageBackend
	HTTPClient *http.Client
	Proxy      *urlproxy.Proxy
}

type CachingProtocolFactory interface {
	ID() string
	NewInstance(env Environment) (CachingProtocol, error)
}

type CachingProtocol interface {
	Register(registry *Registry) error
}
