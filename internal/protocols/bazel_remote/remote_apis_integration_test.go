//go:build integration

package bazel_remote_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	bazel_remote "github.com/cirruslabs/omni-cache/internal/protocols/bazel_remote"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	remoteAPIsCommitSHA = "080cf125032eb61b21a53c72c1a09a45145fc152"
	bazelImageV9        = "gcr.io/bazel-public/bazel:9"
	bazelImageV9_0_0    = "gcr.io/bazel-public/bazel:9.0.0"
)

func TestBuildRemoteAPIsUsesRemoteAsset(t *testing.T) {
	t.Parallel()

	testutil.RequireDocker(t)
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not available")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Minute)
	t.Cleanup(cancel)

	s3Client := testutil.S3Client(t)
	bucketName := fmt.Sprintf("omni-cache-bazel-remote-%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	backend, err := storage.NewS3Storage(ctx, s3Client, bucketName)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	httpServer, err := server.Start(ctx, []net.Listener{listener}, backend, bazel_remote.Factory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, httpServer.Shutdown(context.Background()))
	})

	repoDir := filepath.Join(t.TempDir(), "remote-apis")
	_, err = runCommand(ctx, "", nil, "git", "clone", "--filter=blob:none", "https://github.com/bazelbuild/remote-apis", repoDir)
	require.NoError(t, err)
	_, err = runCommand(ctx, repoDir, nil, "git", "checkout", remoteAPIsCommitSHA)
	require.NoError(t, err)

	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	bazelImage, ok := firstAvailableDockerImage(ctx, bazelImageV9, bazelImageV9_0_0)
	if !ok {
		t.Skipf("none of the Bazel images are available: %s, %s", bazelImageV9, bazelImageV9_0_0)
	}

	remoteAddr := "host.docker.internal:" + port
	dockerArgs := []string{"run", "--rm"}
	if runtime.GOARCH == "arm64" {
		dockerArgs = append(dockerArgs, "--platform=linux/amd64")
	}
	if runtime.GOOS == "linux" {
		dockerArgs = append(dockerArgs, "--network=host")
		remoteAddr = "127.0.0.1:" + port
	}

	dockerArgs = append(
		dockerArgs,
		"-v", fmt.Sprintf("%s:/work", repoDir),
		"-w", "/work",
		bazelImage,
		"build",
		"//...",
		"--remote_cache=grpc://"+remoteAddr,
		"--experimental_remote_downloader=grpc://"+remoteAddr,
		"--remote_instance_name=omni-cache",
	)

	output, err := runCommand(ctx, "", nil, "docker", dockerArgs...)
	if err != nil {
		if isKnownRemoteAPIsBazel9Incompatibility(string(output)) {
			t.Skipf("remote-apis@%s is incompatible with Bazel 9 in this environment", remoteAPIsCommitSHA)
		}
		t.Logf("bazel container exited with error (continuing to validate Remote Asset activity): %v", err)
	}

	require.Eventually(t, func() bool {
		listCtx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()

		resp, err := s3Client.ListObjectsV2(listCtx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			Prefix:  aws.String("bazel/asset/v1/omni-cache/"),
			MaxKeys: aws.Int32(1),
		})
		if err != nil {
			t.Logf("list objects failed: %v", err)
			return false
		}
		return len(resp.Contents) > 0
	}, 2*time.Minute, 2*time.Second, "expected at least one Remote Asset mapping; bazel output:\n%s", string(output))
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

func firstAvailableDockerImage(ctx context.Context, images ...string) (string, bool) {
	for _, image := range images {
		if _, err := runCommand(ctx, "", nil, "docker", "manifest", "inspect", image); err == nil {
			return image, true
		}
	}
	return "", false
}

func isKnownRemoteAPIsBazel9Incompatibility(output string) bool {
	patterns := []string{
		"no native function or rule 'sh_binary'",
		"got element of type string, want sequence",
		"rule() got unexpected keyword argument 'incompatible_use_toolchain_transition'",
		"key \"3.14.0b2\" not found in dictionary",
	}

	for _, pattern := range patterns {
		if strings.Contains(output, pattern) {
			return true
		}
	}

	return false
}
