package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/docker/docker/client"
)

// RequireDocker skips the calling test when Docker isn't reachable.
func RequireDocker(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skipf("docker client unavailable: %v", err)
	}
	defer cli.Close()

	if _, err := cli.Info(ctx); err != nil {
		t.Skipf("docker not available: %v", err)
	}
}
