package commands

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cirruslabs/omni-cache/pkg/protocols/builtin"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/spf13/cobra"
)

const (
	defaultListenAddr = "localhost:12321"
	defaultPort       = "12321"
	defaultAWSRegion  = "us-east-1"

	cacheHostEnv  = "OMNI_CACHE_HOST"
	bucketEnv     = "OMNI_CACHE_BUCKET"
	prefixEnv     = "OMNI_CACHE_PREFIX"
	s3EndpointEnv = "OMNI_CACHE_S3_ENDPOINT"

	shutdownTimeout = 10 * time.Second
)

type sidecarOptions struct {
	bucketName string
	prefix     string
	s3Endpoint string
}

func newSidecarCmd() *cobra.Command {
	opts := &sidecarOptions{
		bucketName: envOrFirst(bucketEnv),
		prefix:     envOrFirst(prefixEnv),
		s3Endpoint: envOrFirst(s3EndpointEnv),
	}

	cmd := &cobra.Command{
		Use:   "sidecar",
		Short: "Start the cache sidecar",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// https://github.com/spf13/cobra/issues/340#issuecomment-374617413
			cmd.SilenceUsage = true

			return runSidecar(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.bucketName, "bucket", opts.bucketName, "S3 bucket name")
	cmd.Flags().StringVar(&opts.prefix, "prefix", opts.prefix, "S3 object key prefix")
	cmd.Flags().StringVar(&opts.s3Endpoint, "s3-endpoint", opts.s3Endpoint, "S3 endpoint override (e.g. https://s3.example.com)")

	return cmd
}

func runSidecar(ctx context.Context, opts *sidecarOptions) error {
	if opts == nil {
		return fmt.Errorf("sidecar options are nil")
	}

	bucketName := strings.TrimSpace(opts.bucketName)
	if bucketName == "" {
		return fmt.Errorf("missing required bucket: set --bucket or %s", bucketEnv)
	}
	prefixValue := strings.TrimSpace(opts.prefix)
	s3Endpoint := strings.TrimSpace(opts.s3Endpoint)

	listenAddr, err := resolveListenAddr()
	if err != nil {
		return err
	}

	backend, err := newS3Backend(ctx, bucketName, prefixValue, s3Endpoint)
	if err != nil {
		return err
	}

	return runServer(ctx, listenAddr, bucketName, backend)
}

func runServer(ctx context.Context, listenAddr, bucketName string, backend storage.MultipartBlobStorageBackend) error {
	if strings.TrimSpace(listenAddr) == "" {
		return fmt.Errorf("listen address is empty")
	}
	if backend == nil {
		return fmt.Errorf("storage backend is nil")
	}

	listeners := make([]net.Listener, 0, 2)
	tcpListener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	defer func() {
		_ = tcpListener.Close()
	}()
	listeners = append(listeners, tcpListener)
	actualAddr := tcpListener.Addr().String()

	var socketPath string
	if runtime.GOOS != "windows" {
		unixListener, path, cleanup, err := listenUnixSocket()
		if err != nil {
			return err
		}
		defer cleanup()
		defer func() {
			_ = unixListener.Close()
		}()
		socketPath = path
		listeners = append(listeners, unixListener)
	} else {
		slog.Info("skipping unix socket on windows")
	}

	factories := builtin.Factories()
	serverCtx := context.WithoutCancel(ctx)
	srv, err := server.Start(serverCtx, listeners, backend, factories...)
	if err != nil {
		return err
	}

	if socketPath != "" {
		attrs := []any{"addr", actualAddr, "socket", socketPath, "bucket", bucketName}
		slog.InfoContext(ctx, "omni-cache started", attrs...)
	} else {
		attrs := []any{"addr", actualAddr, "bucket", bucketName}
		slog.InfoContext(ctx, "omni-cache started", attrs...)
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	shutdownErr := srv.Shutdown(shutdownCtx)
	stats.Default().LogSummary()
	if shutdownErr != nil {
		return fmt.Errorf("shutdown: %w", shutdownErr)
	}
	slog.Info("omni-cache stopped")
	return nil
}

func resolveListenAddr() (string, error) {
	addr := strings.TrimSpace(os.Getenv(cacheHostEnv))
	if addr == "" {
		return defaultListenAddr, nil
	}

	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		parsed, err := url.Parse(addr)
		if err != nil || parsed.Host == "" {
			return "", fmt.Errorf("%s must be host:port, got %q", cacheHostEnv, addr)
		}
		addr = parsed.Host
	}

	if _, _, err := net.SplitHostPort(addr); err != nil {
		var addrErr *net.AddrError
		if errors.As(err, &addrErr) && strings.Contains(addrErr.Err, "missing port in address") {
			addr = net.JoinHostPort(addr, defaultPort)
		} else {
			return "", fmt.Errorf("%s must be host:port, got %q", cacheHostEnv, addr)
		}
	}

	return addr, nil
}

func newS3Backend(ctx context.Context, bucketName, prefix, s3Endpoint string) (storage.MultipartBlobStorageBackend, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if cfg.Region == "" {
		cfg.Region = defaultAWSRegion
	}

	client, err := newS3Client(cfg, s3Endpoint)
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		return storage.NewS3Storage(ctx, client, bucketName)
	}
	return storage.NewS3Storage(ctx, client, bucketName, prefix)
}

func newS3Client(cfg aws.Config, s3Endpoint string) (*s3.Client, error) {
	s3Endpoint = strings.TrimSpace(s3Endpoint)
	if s3Endpoint == "" {
		return s3.NewFromConfig(cfg), nil
	}

	parsed, err := url.Parse(s3Endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("s3 endpoint must be a full URL, got %q", s3Endpoint)
	}

	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(s3Endpoint)
		options.UsePathStyle = true
	})
	return client, nil
}

func listenUnixSocket() (net.Listener, string, func(), error) {
	socketPath, err := server.DefaultSocketPath()
	if err != nil {
		return nil, "", nil, err
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

func envOrFirst(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}
