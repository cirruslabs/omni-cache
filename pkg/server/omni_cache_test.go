package server_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestOmniCacheNew(t *testing.T) {
	homeDir := shortTempDir(t)
	t.Setenv("HOME", homeDir)

	cache, err := server.New(context.Background(), nil, server.WithFactories(testFactory{}))
	require.NoError(t, err)
	require.NotNil(t, cache)
	require.NotEmpty(t, cache.Addr)

	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	if runtime.GOOS == "windows" {
		require.Empty(t, cache.SocketPath)
	} else {
		expectedSocketPath := filepath.Join(homeDir, ".cirruslabs", "omni-cache.sock")
		require.Equal(t, expectedSocketPath, cache.SocketPath)
		_, err := os.Stat(cache.SocketPath)
		require.NoError(t, err)
	}

	httpClient := &http.Client{
		Timeout: 2 * time.Second,
	}
	t.Cleanup(func() {
		httpClient.CloseIdleConnections()
	})

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+cache.Addr+"/ping", nil)
		if err != nil {
			return false
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, time.Minute, time.Second)
}
