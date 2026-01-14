package protocols

import (
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	"google.golang.org/grpc"
)

type Dependencies struct {
	Storage  storage.BlobStorageBackend
	HTTP     *http.Client
	URLProxy *urlproxy.Proxy
}

func (deps Dependencies) WithDefaults() Dependencies {
	if deps.HTTP == nil {
		deps.HTTP = http.DefaultClient
	}
	if deps.URLProxy == nil {
		deps.URLProxy = urlproxy.NewProxy(
			urlproxy.WithHTTPClient(deps.HTTP),
		)
	}
	return deps
}

type Factory interface {
	ID() string
	New(deps Dependencies) (Protocol, error)
}

type Registrar struct {
	httpMux       *http.ServeMux
	grpcRegistrar grpc.ServiceRegistrar
}

func NewRegistrar(httpMux *http.ServeMux, grpcRegistrar grpc.ServiceRegistrar) *Registrar {
	return &Registrar{
		httpMux:       httpMux,
		grpcRegistrar: grpcRegistrar,
	}
}

func (r *Registrar) HTTP() *http.ServeMux {
	return r.httpMux
}

func (r *Registrar) GRPC() grpc.ServiceRegistrar {
	return r.grpcRegistrar
}

type Protocol interface {
	Register(registrar *Registrar) error
}
