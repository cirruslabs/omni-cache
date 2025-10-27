package server

import (
	"context"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

const (
	activeRequestsPerLogicalCPU = 4
)

func DefaultTransport() *http.Transport {
	maxConcurrentConnections := runtime.NumCPU() * activeRequestsPerLogicalCPU

	return &http.Transport{
		MaxIdleConns:        maxConcurrentConnections,
		MaxIdleConnsPerHost: maxConcurrentConnections, // default is 2 which is too small
	}
}

func Start(ctx context.Context, listener net.Listener, backend storage.BlobStorageBacked) (*http.Server, error) {
	mux, err := Create(backend)
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

func Create(backend storage.BlobStorageBacked) (*http.ServeMux, error) {
	httpClient := &http.Client{
		Transport: DefaultTransport(),
		Timeout:   10 * time.Minute,
	}

	mux := http.NewServeMux()

	for _, registeredProtocolFactory := range registry {
		mux.Handle(registeredProtocolFactory.Pattern, registeredProtocolFactory.Create(httpClient, backend))
	}

	return mux, nil
}
