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
)

const (
	defaultTCPListenAddr  = "127.0.0.1:12321"
	fallbackTCPListenAddr = "127.0.0.1:0"

	defaultSocketDirName = ".cirruslabs"
	defaultSocketName    = "omni-cache.sock"

	defaultShutdownTimeout = 10 * time.Second
)

type OmniCache struct {
	Addr       string
	SocketPath string

	server          *http.Server
	listeners       []net.Listener
	cleanupSocket   func()
	shutdownTimeout time.Duration
}

type Option func(*options)

type options struct {
	tcpListenAddr         string
	fallbackTCPListenAddr string
	unixSocketPath        string
	unixSocketEnabled     bool
	shutdownTimeout       time.Duration
	factories             []protocols.Factory
}

func New(ctx context.Context, backend storage.BlobStorageBackend, opts ...Option) (*OmniCache, error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	if len(cfg.factories) == 0 {
		cfg.factories = builtin.Factories()
	}
	if strings.TrimSpace(cfg.tcpListenAddr) == "" {
		return nil, fmt.Errorf("tcp listen address is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	tcpListener, err := net.Listen("tcp", cfg.tcpListenAddr)
	if err != nil {
		if strings.TrimSpace(cfg.fallbackTCPListenAddr) == "" {
			return nil, fmt.Errorf("listen on tcp: %w", err)
		}
		if cfg.tcpListenAddr == defaultTCPListenAddr {
			slog.Warn("Port 12321 is occupied, looking for another one", "err", err)
		} else {
			slog.Warn("TCP listen failed, trying fallback", "addr", cfg.tcpListenAddr, "err", err)
		}
		tcpListener, err = net.Listen("tcp", cfg.fallbackTCPListenAddr)
		if err != nil {
			return nil, fmt.Errorf("listen on tcp: %w", err)
		}
	}

	listeners := []net.Listener{tcpListener}
	var socketPath string
	var cleanupSocket func()

	if cfg.unixSocketEnabled && runtime.GOOS != "windows" {
		path := cfg.unixSocketPath
		if strings.TrimSpace(path) == "" {
			path, err = defaultSocketPath()
			if err != nil {
				closeListeners(listeners)
				return nil, err
			}
		}
		unixListener, path, cleanup, err := listenUnixSocket(path)
		if err != nil {
			closeListeners(listeners)
			return nil, err
		}
		listeners = append(listeners, unixListener)
		socketPath = path
		cleanupSocket = cleanup
	} else if cfg.unixSocketEnabled && runtime.GOOS == "windows" {
		slog.Info("skipping unix socket on windows")
	}

	srv, err := Start(ctx, listeners, backend, cfg.factories...)
	if err != nil {
		closeListeners(listeners)
		if cleanupSocket != nil {
			cleanupSocket()
		}
		return nil, err
	}

	return &OmniCache{
		Addr:            tcpListener.Addr().String(),
		SocketPath:      socketPath,
		server:          srv,
		listeners:       listeners,
		cleanupSocket:   cleanupSocket,
		shutdownTimeout: cfg.shutdownTimeout,
	}, nil
}

func (c *OmniCache) Shutdown(ctx context.Context) error {
	if c == nil || c.server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	shutdownErr := c.server.Shutdown(ctx)
	closeListeners(c.listeners)
	if c.cleanupSocket != nil {
		c.cleanupSocket()
	}
	return shutdownErr
}

func (c *OmniCache) Close() error {
	if c == nil {
		return nil
	}

	ctx := context.Background()
	if c.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.shutdownTimeout)
		defer cancel()
	}
	return c.Shutdown(ctx)
}

func WithFactories(factories ...protocols.Factory) Option {
	return func(opts *options) {
		if len(factories) == 0 {
			opts.factories = nil
			return
		}
		opts.factories = append([]protocols.Factory(nil), factories...)
	}
}

func WithTCPListenAddr(addr string) Option {
	return func(opts *options) {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			opts.tcpListenAddr = trimmed
		}
	}
}

func WithFallbackTCPListenAddr(addr string) Option {
	return func(opts *options) {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			opts.fallbackTCPListenAddr = trimmed
		}
	}
}

func WithUnixSocketPath(path string) Option {
	return func(opts *options) {
		opts.unixSocketEnabled = true
		opts.unixSocketPath = strings.TrimSpace(path)
	}
}

func WithoutUnixSocket() Option {
	return func(opts *options) {
		opts.unixSocketEnabled = false
		opts.unixSocketPath = ""
	}
}

func WithShutdownTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.shutdownTimeout = timeout
	}
}

func defaultOptions() options {
	return options{
		tcpListenAddr:         defaultTCPListenAddr,
		fallbackTCPListenAddr: fallbackTCPListenAddr,
		unixSocketEnabled:     true,
		shutdownTimeout:       defaultShutdownTimeout,
	}
}

func listenUnixSocket(socketPath string) (net.Listener, string, func(), error) {
	if strings.TrimSpace(socketPath) == "" {
		return nil, "", nil, fmt.Errorf("unix socket path is empty")
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, "", nil, fmt.Errorf("create socket dir: %w", err)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, "", nil, fmt.Errorf("remove stale socket: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, "", nil, fmt.Errorf("listen on unix socket: %w", err)
	}

	cleanup := func() {
		_ = os.Remove(socketPath)
	}

	return listener, socketPath, cleanup, nil
}

func defaultSocketPath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}

	return filepath.Join(homeDir, defaultSocketDirName, defaultSocketName), nil
}

func closeListeners(listeners []net.Listener) {
	for _, listener := range listeners {
		_ = listener.Close()
	}
}
