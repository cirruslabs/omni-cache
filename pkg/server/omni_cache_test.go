package server_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestStartDefault(t *testing.T) {
	homeDir := shortTempDir(t)
	t.Setenv("HOME", homeDir)

	srv, err := server.StartDefault(context.Background(), nil, testFactory{})
	require.NoError(t, err)
	require.NotNil(t, srv)
	require.NotEmpty(t, srv.Addr)

	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})

	expectedSocketPath := filepath.Join(homeDir, ".cirruslabs", "omni-cache.sock")
	_, err = os.Stat(expectedSocketPath)
	require.NoError(t, err)

	httpClient := &http.Client{
		Timeout: 2 * time.Second,
	}
	t.Cleanup(func() {
		httpClient.CloseIdleConnections()
	})

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+srv.Addr+"/ping", nil)
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
