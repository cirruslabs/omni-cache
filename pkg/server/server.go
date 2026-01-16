package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	activeRequestsPerLogicalCPU = 4
)

func Start(ctx context.Context, listeners []net.Listener, backend storage.BlobStorageBackend, factories ...protocols.Factory) (*http.Server, error) {
	if len(listeners) == 0 {
		return nil, fmt.Errorf("no listeners provided")
	}
	for i, listener := range listeners {
		if listener == nil {
			return nil, fmt.Errorf("listener at index %d is nil", i)
		}
	}
	if len(factories) == 0 {
		return nil, fmt.Errorf("no protocols provided")
	}

	mux, grpcServer, err := createMuxAndGRPCServer(backend, factories...)
	if err != nil {
		return nil, err
	}

	handler := h2c.NewHandler(grpcOrHTTPHandler(grpcServer, mux), &http2.Server{})

	httpServer := &http.Server{
		// Use parent context as a base for the HTTP cache handlers
		//
		// This way the HTTP cache handlers will be able to further propagate that context using W3C Trace Context
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		Handler: handler,
	}

	httpServer.RegisterOnShutdown(func() {
		grpcServer.GracefulStop()
	})

	for _, listener := range listeners {
		listener := listener
		go func() {
			if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.ErrorContext(ctx, "server exited with error", "err", err, "addr", listener.Addr().String())
			}
		}()
	}
	return httpServer, nil
}

func grpcOrHTTPHandler(grpcServer *grpc.Server, httpHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}

		httpHandler.ServeHTTP(w, r)
	})
}

func createMuxAndGRPCServer(backend storage.BlobStorageBackend, factories ...protocols.Factory) (*http.ServeMux, *grpc.Server, error) {
	maxConcurrentConnections := runtime.NumCPU() * activeRequestsPerLogicalCPU

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxConcurrentConnections,
			MaxIdleConnsPerHost: maxConcurrentConnections, // default is 2 which is too small
		},
		Timeout: 10 * time.Minute,
	}

	deps := protocols.Dependencies{
		Storage: backend,
		HTTP:    httpClient,
	}.WithDefaults()

	mux := http.NewServeMux()
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	registrar := protocols.NewRegistrar(mux, grpcServer)

	seenIDs := map[string]struct{}{}
	for _, factory := range factories {
		id := factory.ID()
		if id == "" {
			return nil, nil, fmt.Errorf("protocol factory with empty ID")
		}
		if _, ok := seenIDs[id]; ok {
			return nil, nil, fmt.Errorf("duplicate protocol factory ID %q", id)
		}
		seenIDs[id] = struct{}{}

		protocol, err := factory.New(deps)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: create failed: %w", id, err)
		}
		if err := protocol.Register(registrar); err != nil {
			return nil, nil, fmt.Errorf("%s: register failed: %w", id, err)
		}
	}

	return mux, grpcServer, nil
}
