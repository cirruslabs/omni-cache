package bazel_remote_asset_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	bazelremoteasset "github.com/cirruslabs/omni-cache/internal/protocols/bazel_remote_asset"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRemoteAssetBazelBuildIntegration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a Unix-like Docker environment")
	}
	if runtime.GOARCH == "arm64" && os.Getenv("OMNI_CACHE_BAZEL_EMULATION") == "" {
		t.Skip("bazel image is amd64-only; set OMNI_CACHE_BAZEL_EMULATION=1 to run with emulation")
	}

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}

	testutil.RequireDocker(t)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	t.Cleanup(cancel)

	repoDir := cloneRemoteAPIs(t, ctx)

	storage := testutil.NewStorage(t)
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	srv, err := server.Start(ctx, []net.Listener{listener}, storage, bazelremoteasset.Factory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok, "expected TCP listener address")
	downloaderAddr := fmt.Sprintf("grpc://host.docker.internal:%d", tcpAddr.Port)

	stats.Default().Reset()

	extraHosts := []string{}
	if runtime.GOOS == "linux" {
		extraHosts = []string{"host.docker.internal:host-gateway"}
	}

	user := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	imagePlatform := ""
	if runtime.GOARCH == "arm64" {
		imagePlatform = "linux/amd64"
	}

	request := testcontainers.ContainerRequest{
		Image:         "gcr.io/bazel-public/bazel:latest",
		ImagePlatform: imagePlatform,
		Cmd:           []string{"sleep", "3600"},
		WorkingDir:    "/workspace",
		Env: map[string]string{
			"HOME": "/tmp",
		},
		User:       user,
		ExtraHosts: extraHosts,
		HostConfigModifier: func(hostConfig *dockercontainer.HostConfig) {
			hostConfig.Binds = append(hostConfig.Binds, fmt.Sprintf("%s:/workspace", repoDir))
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	cmd := []string{
		"bazel",
		"build",
		"//build/bazel/remote/asset/v1:remote_asset_proto",
		fmt.Sprintf("--experimental_remote_downloader=%s", downloaderAddr),
		"--experimental_remote_downloader_local_fallback",
		"--symlink_prefix=/tmp/bazel-",
		"--jobs=1",
		"--local_ram_resources=2048",
	}

	exitCode, reader, err := container.Exec(ctx, cmd, tcexec.Multiplexed())
	require.NoError(t, err)

	output, readErr := io.ReadAll(reader)
	require.NoError(t, readErr)

	if exitCode != 0 {
		t.Fatalf("bazel build failed (exit %d):\n%s", exitCode, output)
	}

	snapshot := stats.Default().Snapshot()
	if snapshot.CacheHits+snapshot.CacheMisses == 0 {
		t.Fatalf("expected remote asset requests, got none (bazel output: %s)", output)
	}
}

func cloneRemoteAPIs(t *testing.T, ctx context.Context) string {
	t.Helper()

	repoDir := filepath.Join(t.TempDir(), "remote-apis")
	cmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "https://github.com/bazelbuild/remote-apis", repoDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git clone failed: %v\n%s", err, output)
	}
	return repoDir
}
