package http_cache_test

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

	"github.com/cirruslabs/omni-cache/internal/protocols/http_cache"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPCacheCirrusCLIBuildIntegration(t *testing.T) {
	if _, err := exec.LookPath("cirrus"); err != nil {
		t.Skip("cirrus CLI not available")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}
	testutil.RequireDocker(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	t.Cleanup(cancel)

	storageBackend := testutil.NewStorage(t)
	countingBackend := newCountingBackend(storageBackend)

	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv, err := server.Start(ctx, []net.Listener{listener}, countingBackend, http_cache.Factory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	cacheHost, err := cacheHostForContainers(ctx, listener.Addr())
	if err != nil {
		t.Skipf("unable to resolve cache host for containers: %v", err)
	}

	projectDir := t.TempDir()
	writeProjectFiles(t, projectDir)

	env := append(os.Environ(),
		"CIRRUS_CONTAINER_BACKEND=docker",
	)

	_, err = runCommand(ctx, projectDir, env, "cirrus", "run", "--dirty", "-v", "-o", "simple", "--env", "CIRRUS_HTTP_CACHE_HOST="+cacheHost)
	require.NoError(t, err)

	if countingBackend.uploadCalls.Load() == 0 {
		t.Fatal("expected cirrus-cli to upload cache entries")
	}

	downloadsBefore := countingBackend.downloadCalls.Load()
	_, err = runCommand(ctx, projectDir, env, "cirrus", "run", "--dirty", "-v", "-o", "simple", "--env", "CIRRUS_HTTP_CACHE_HOST="+cacheHost)
	require.NoError(t, err)

	if countingBackend.downloadCalls.Load() <= downloadsBefore {
		t.Fatal("expected cirrus-cli to download cache entries on second run")
	}
}

func writeProjectFiles(t *testing.T, dir string) {
	t.Helper()

	err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/hello\n\ngo 1.22\n"), 0o600)
	require.NoError(t, err)

	mainSource := `package main

import "fmt"

func main() {
	fmt.Println("hello")
}
`
	err = os.WriteFile(filepath.Join(dir, "main.go"), []byte(mainSource), 0o600)
	require.NoError(t, err)

	containerKey := "container"
	if runtime.GOARCH == "arm64" {
		containerKey = "arm_container"
	}

	cirrusConfig := fmt.Sprintf(`task:
  name: Build
  %s:
    image: golang:1.22
  env:
    GOCACHE: /cache
  cache:
    name: go-cache
    folder: /cache
    fingerprint_key: omni-cache-cirrus-cli
  script:
    - mkdir -p /cache
    - go build -o /tmp/hello .
    - echo "marker" > /cache/marker.txt
`, containerKey)
	err = os.WriteFile(filepath.Join(dir, ".cirrus.yml"), []byte(cirrusConfig), 0o600)
	require.NoError(t, err)
}

func cacheHostForContainers(ctx context.Context, addr net.Addr) (string, error) {
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return "", fmt.Errorf("unexpected listener address type %T", addr)
	}

	if runtime.GOOS == "linux" {
		gateway, err := dockerBridgeGateway(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s:%d", gateway, tcpAddr.Port), nil
	}

	return fmt.Sprintf("host.docker.internal:%d", tcpAddr.Port), nil
}

func dockerBridgeGateway(ctx context.Context) (string, error) {
	output, err := exec.CommandContext(ctx, "docker", "network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}").Output()
	if err != nil {
		return "", fmt.Errorf("resolve docker bridge gateway: %w", err)
	}
	gateway := strings.TrimSpace(string(output))
	if gateway == "" {
		return "", fmt.Errorf("docker bridge gateway is empty")
	}
	return gateway, nil
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
