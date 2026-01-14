package server

import (
	"context"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/cirruslabs/omni-cache/internal/protocols/ghacache"
	"github.com/cirruslabs/omni-cache/internal/protocols/http_cache"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

const (
	activeRequestsPerLogicalCPU = 4
)

var DefaultProtocolFactories = []protocols.CachingProtocolFactory{
	&http_cache.HttpCacheProtocolFactory{},
	&ghacache.GHACacheProtocolFactory{},
}

func Start(ctx context.Context, listener net.Listener, backend storage.BlobStorageBackend, protocols ...protocols.CachingProtocolFactory) (*http.Server, error) {
	if len(protocols) == 0 {
		protocols = DefaultProtocolFactories
	}

	mux, err := createMux(backend, protocols...)
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
		Handler: mux,
	}
	go httpServer.Serve(listener)
	return httpServer, nil
}

func createMux(backend storage.BlobStorageBackend, protocols ...protocols.CachingProtocolFactory) (*http.ServeMux, error) {
	maxConcurrentConnections := runtime.NumCPU() * activeRequestsPerLogicalCPU

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxConcurrentConnections,
			MaxIdleConnsPerHost: maxConcurrentConnections, // default is 2 which is too small
		},
		Timeout: 10 * time.Minute,
	}

	mux := http.NewServeMux()

	for _, registeredProtocolFactory := range protocols {
		cachingProtocol, err := registeredProtocolFactory.NewInstance(backend, httpClient)
		if err != nil {
			return nil, err
		}
		if err := cachingProtocol.Register(mux); err != nil {
			return nil, err
		}
	}

	return mux, nil
}
