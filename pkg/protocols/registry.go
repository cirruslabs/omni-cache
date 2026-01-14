package protocols

import (
	"errors"
	"net/http"

	"google.golang.org/grpc"
)

var ErrGRPCUnavailable = errors.New("protocols: gRPC registry unavailable")

// Registry collects protocol registrations for supported transports.
type Registry struct {
	httpMux        *http.ServeMux
	grpcServer     *grpc.Server
	grpcRegistered bool
}

func NewRegistry(httpMux *http.ServeMux, grpcServer *grpc.Server) *Registry {
	return &Registry{
		httpMux:    httpMux,
		grpcServer: grpcServer,
	}
}

func (r *Registry) Handle(pattern string, handler http.Handler) {
	r.httpMux.Handle(pattern, handler)
}

func (r *Registry) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	r.httpMux.HandleFunc(pattern, handler)
}

func (r *Registry) RegisterGRPC(register func(*grpc.Server)) error {
	if r.grpcServer == nil {
		return ErrGRPCUnavailable
	}
	if register == nil {
		return errors.New("protocols: gRPC register func is nil")
	}
	register(r.grpcServer)
	r.grpcRegistered = true
	return nil
}

func (r *Registry) HasGRPC() bool {
	return r.grpcRegistered
}
