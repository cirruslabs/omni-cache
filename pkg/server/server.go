package server

import (
	"context"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/cirruslabs/omni-cache/internal/protocols/http_cache"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

const (
	activeRequestsPerLogicalCPU = 4
)

var DefaultProtocolFactories = []protocols.CachingProtocolFactory{
	&http_cache.HttpCacheProtocolFactory{},
}

func Start(ctx context.Context, listener net.Listener, backend storage.BlobStorageBackend, protocols ...protocols.CachingProtocolFactory) (*http.Server, error) {
	if len(protocols) == 0 {
		protocols = DefaultProtocolFactories
	}

	handler, grpcServer, err := createHandler(backend, protocols...)
	if err != nil {
		return nil, err
	}

	httpServer := &http.Server{
		// Use parent context as a base for the HTTP cache handlers
		//
		// This way the HTTP cache handlers will be able to further propagate that context using W3C Trace Context
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		Handler: handler,
	}
	if grpcServer != nil {
		httpServer.RegisterOnShutdown(grpcServer.GracefulStop)
	}
	go httpServer.Serve(listener)
	return httpServer, nil
}

func createHandler(backend storage.BlobStorageBackend, protocolFactories ...protocols.CachingProtocolFactory) (http.Handler, *grpc.Server, error) {
	maxConcurrentConnections := runtime.NumCPU() * activeRequestsPerLogicalCPU

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxConcurrentConnections,
			MaxIdleConnsPerHost: maxConcurrentConnections, // default is 2 which is too small
		},
		Timeout: 10 * time.Minute,
	}

	mux := http.NewServeMux()
	grpcServer := grpc.NewServer()
	proxy := urlproxy.NewProxy(urlproxy.WithHTTPClient(httpClient))
	env := protocols.Environment{
		Storage:    backend,
		HTTPClient: httpClient,
		Proxy:      proxy,
	}
	registry := protocols.NewRegistry(mux, grpcServer)

	for _, registeredProtocolFactory := range protocolFactories {
		cachingProtocol, err := registeredProtocolFactory.NewInstance(env)
		if err != nil {
			return nil, nil, err
		}
		if err := cachingProtocol.Register(registry); err != nil {
			return nil, nil, err
		}
	}

	if registry.HasGRPC() {
		return grpcHandler(grpcServer, mux), grpcServer, nil
	}

	return mux, nil, nil
}

func grpcHandler(grpcServer *grpc.Server, httpHandler http.Handler) http.Handler {
	return h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		httpHandler.ServeHTTP(w, r)
	}), &http2.Server{})
}
