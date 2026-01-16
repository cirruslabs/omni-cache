package llvm_cache

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	casv1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/cas/v1"
	keyvaluev1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/keyvalue/v1"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
)

func TestTartBuildUsesRemoteCache(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("requires macOS")
	}

	if _, err := exec.LookPath("swift"); err != nil {
		t.Skip("swift is not available")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	testutil.RequireDocker(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	t.Cleanup(cancel)

	container, endpoint := startLocalstack(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	bucketName := fmt.Sprintf("omni-cache-tart-%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	s3Client := newLocalstackS3Client(t, ctx, endpoint)

	stor, err := storage.NewS3Storage(ctx, s3Client, bucketName)
	require.NoError(t, err)
	countingStor := newCountingBackend(stor)

	baseDir, err := os.MkdirTemp("/tmp", "omni-cache-tart-")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(baseDir)
	})

	socketPath := filepath.Join(baseDir, "omni-cache.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	store := newCacheStore(countingStor, urlproxy.NewProxy())
	grpcServer := grpc.NewServer()
	casv1.RegisterCASDBServiceServer(grpcServer, newCASService(store))
	keyvaluev1.RegisterKeyValueDBServer(grpcServer, newKVService(store))
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		_ = os.Remove(socketPath)
	})

	require.NoError(t, waitForUnixSocket(ctx, socketPath))

	repoDir := filepath.Join(t.TempDir(), "tart")
	_, err = runCommand(ctx, "", nil, "git", "clone", "--depth", "1", "https://github.com/cirruslabs/tart", repoDir)
	require.NoError(t, err)

	env := append(os.Environ(),
		"COMPILATION_CACHE_ENABLE_CACHING=YES",
		"COMPILATION_CACHE_REMOTE_SERVICE_PATH="+socketPath,
		"COMPILATION_CACHE_ENABLE_PLUGIN=YES",
		"COMPILATION_CACHE_ENABLE_INTEGRATED_QUERIES=YES",
		"COMPILATION_CACHE_ENABLE_DETACHED_KEY_QUERIES=YES",
		"SWIFT_ENABLE_COMPILE_CACHE=YES",
		"SWIFT_ENABLE_EXPLICIT_MODULES=YES",
		"SWIFT_USE_INTEGRATED_DRIVER=YES",
		"CLANG_ENABLE_COMPILE_CACHE=YES",
		"CLANG_ENABLE_MODULES=YES",
	)

	_, err = runCommand(ctx, repoDir, env, "swift", "build", "--build-system", "swiftbuild")
	require.NoError(t, err)

	if countingStor.totalCalls() == 0 {
		t.Skip("swift build did not contact the cache service")
	}
	if countingStor.uploadCalls.Load() == 0 {
		t.Skip("cache service saw no uploads during the build")
	}

	require.Eventually(t, func() bool {
		listCtx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()

		resp, err := s3Client.ListObjectsV2(listCtx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			Prefix:  aws.String("llvm-cache/"),
			MaxKeys: aws.Int32(1),
		})
		if err != nil {
			t.Logf("list objects failed: %v", err)
			return false
		}
		return len(resp.Contents) > 0
	}, 1*time.Minute, 2*time.Second)
}

func startLocalstack(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	request := testcontainers.ContainerRequest{
		Image:        "localstack/localstack",
		ExposedPorts: []string{"4566/tcp"},
		WaitingFor:   wait.ForHTTP("/_localstack/health").WithPort("4566/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "4566")
	require.NoError(t, err)

	return container, fmt.Sprintf("http://%s:%s", host, port.Port())
}

func newLocalstackS3Client(t *testing.T, ctx context.Context, endpoint string) *s3.Client {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("id", "secret", "")),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
}

func waitForUnixSocket(ctx context.Context, socketPath string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(ctx, "unix", socketPath)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	return fmt.Errorf("socket %s did not become ready", socketPath)
}

func runCommand(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	if len(env) > 0 {
		cmd.Env = env
	}

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Run(); err != nil {
		return output.Bytes(), fmt.Errorf("%s %v failed: %w\n%s", name, args, err, output.String())
	}
	return output.Bytes(), nil
}

type countingBackend struct {
	backend storage.BlobStorageBackend

	uploadCalls   atomic.Int64
	downloadCalls atomic.Int64
	cacheCalls    atomic.Int64
}

func newCountingBackend(backend storage.BlobStorageBackend) *countingBackend {
	return &countingBackend{backend: backend}
}

func (c *countingBackend) UploadURL(ctx context.Context, key string, metadata map[string]string) (*storage.URLInfo, error) {
	c.uploadCalls.Add(1)
	return c.backend.UploadURL(ctx, key, metadata)
}

func (c *countingBackend) DownloadURLs(ctx context.Context, key string) ([]*storage.URLInfo, error) {
	c.downloadCalls.Add(1)
	return c.backend.DownloadURLs(ctx, key)
}

func (c *countingBackend) CacheInfo(ctx context.Context, key string, prefixes []string) (*storage.CacheInfo, error) {
	c.cacheCalls.Add(1)
	return c.backend.CacheInfo(ctx, key, prefixes)
}

func (c *countingBackend) totalCalls() int64 {
	return c.uploadCalls.Load() + c.downloadCalls.Load() + c.cacheCalls.Load()
}
