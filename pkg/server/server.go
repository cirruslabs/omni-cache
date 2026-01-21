package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/protocols/builtin"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	activeRequestsPerLogicalCPU = 4

	defaultTCPListenAddr  = "127.0.0.1:12321"
	fallbackTCPListenAddr = "127.0.0.1:0"

	defaultSocketDirName = ".cirruslabs"
	defaultSocketName    = "omni-cache.sock"
)

func StartDefault(ctx context.Context, backend storage.BlobStorageBackend, factories ...protocols.Factory) (*http.Server, error) {
	if len(factories) == 0 {
		factories = builtin.Factories()
	}

	tcpListener, err := net.Listen("tcp", defaultTCPListenAddr)
	if err != nil {
		slog.Warn("Port 12321 is occupied, looking for another one", "err", err)
		tcpListener, err = net.Listen("tcp", fallbackTCPListenAddr)
		if err != nil {
			return nil, fmt.Errorf("listen on tcp: %w", err)
		}
	}

	listeners := []net.Listener{tcpListener}

	socketPath, err := defaultSocketPath()
	socketCleanup := func() {
		if socketPath != "" {
			_ = os.Remove(socketPath)
		}
	}
	if runtime.GOOS != "windows" && err != nil {
		unixListener, err := listenUnixSocket(socketPath)
		if err != nil {
			_ = tcpListener.Close()
			return nil, err
		}
		listeners = append(listeners, unixListener)
	} else {
		slog.Info("skipping unix socket creation")
	}

	srv, err := Start(ctx, listeners, backend, factories...)
	if err != nil {
		for _, listener := range listeners {
			_ = listener.Close()
		}
		return nil, err
	}

	srv.Addr = tcpListener.Addr().String()
	srv.RegisterOnShutdown(socketCleanup)

	return srv, nil
}

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

func listenUnixSocket(socketPath string) (net.Listener, error) {
	if strings.TrimSpace(socketPath) == "" {
		return nil, fmt.Errorf("unix socket path is empty")
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on unix socket: %w", err)
	}

	return listener, nil
}

func defaultSocketPath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}

	return filepath.Join(homeDir, defaultSocketDirName, defaultSocketName), nil
}
