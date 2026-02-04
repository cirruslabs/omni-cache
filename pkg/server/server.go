package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/protocols/builtin"
	"github.com/cirruslabs/omni-cache/pkg/stats"
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

	listenAddr := defaultTCPListenAddr
	tcpListener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		if !isAddrInUse(err) {
			return nil, fmt.Errorf("listen on tcp: %w", err)
		}

		fallbackAddr := fallbackListenAddr(listenAddr)
		slog.Warn("TCP listen address unavailable, trying ephemeral port", "addr", listenAddr, "fallback", fallbackAddr, "err", err)
		tcpListener, err = net.Listen("tcp", fallbackAddr)
		if err != nil {
			return nil, fmt.Errorf("listen on tcp: %w", err)
		}
	}

	listeners := []net.Listener{tcpListener}

	socketPath, err := DefaultSocketPath()
	if err != nil {
		_ = tcpListener.Close()
		return nil, err
	}
	socketCleanup := func() {
		_ = os.Remove(socketPath)
	}
	if runtime.GOOS != "windows" {
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

func fallbackListenAddr(listenAddr string) string {
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return fallbackTCPListenAddr
	}

	return net.JoinHostPort(host, "0")
}

func isAddrInUse(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}

	return strings.Contains(err.Error(), "address already in use")
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

	host := selectHost(listeners)
	mux, grpcServer, err := createMuxAndGRPCServer(host, backend, factories...)
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

func createMuxAndGRPCServer(host string, backend storage.BlobStorageBackend, factories ...protocols.Factory) (*http.ServeMux, *grpc.Server, error) {
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
		Host:    host,
	}.WithDefaults()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics/cache", statsHandler)
	mux.HandleFunc("DELETE /metrics/cache", statsResetHandler)
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

func selectHost(listeners []net.Listener) string {
	for _, listener := range listeners {
		if listener == nil {
			continue
		}
		network := listener.Addr().Network()
		if strings.HasPrefix(network, "unix") {
			continue
		}
		return listener.Addr().String()
	}

	return ""
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

func statsHandler(w http.ResponseWriter, r *http.Request) {
	writeStatsResponse(w, r)
}

func statsResetHandler(w http.ResponseWriter, r *http.Request) {
	stats.Default().Reset()
	writeStatsResponse(w, r)
}

func writeStatsResponse(w http.ResponseWriter, r *http.Request) {
	if acceptsGithubActions(r.Header.Get("Accept")) {
		snapshot := stats.Default().Snapshot()
		if !snapshot.HasActivity() {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, stats.FormatGithubActionsSummary(snapshot))
		return
	}

	if acceptsJSON(r.Header.Get("Accept")) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats.Default().Summary()); err != nil {
			slog.ErrorContext(r.Context(), "failed to encode stats response", "err", err)
		}
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, stats.Default().SummaryText())
}

func acceptsJSON(acceptHeader string) bool {
	if strings.TrimSpace(acceptHeader) == "" {
		return false
	}
	for _, part := range strings.Split(acceptHeader, ",") {
		mediaType := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if mediaType == "application/json" || strings.HasSuffix(mediaType, "+json") {
			return true
		}
	}
	return false
}

func acceptsGithubActions(acceptHeader string) bool {
	if strings.TrimSpace(acceptHeader) == "" {
		return false
	}
	for _, part := range strings.Split(acceptHeader, ",") {
		mediaType := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if strings.Contains(mediaType, "github-actions") {
			return true
		}
	}
	return false
}

// DefaultSocketPath returns the default unix socket path for omni-cache.
func DefaultSocketPath() (string, error) {
	homeDir := strings.TrimSpace(os.Getenv("HOME"))
	if homeDir != "" {
		return filepath.Join(homeDir, defaultSocketDirName, defaultSocketName), nil
	}

	tempDir := strings.TrimSpace(os.TempDir())
	if tempDir == "" {
		return "", fmt.Errorf("resolve temp dir: empty")
	}

	return filepath.Join(tempDir, defaultSocketDirName, defaultSocketName), nil
}
