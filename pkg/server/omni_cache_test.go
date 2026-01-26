package server_test

import (
	"context"
	"net"
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

func TestStartDefaultFallsBackWhenPortInUse(t *testing.T) {
	homeDir := shortTempDir(t)
	t.Setenv("HOME", homeDir)

	occupiedListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = occupiedListener.Close()
	})

	t.Setenv("OMNI_CACHE_HOST", occupiedListener.Addr().String())

	srv, err := server.StartDefault(context.Background(), nil, testFactory{})
	require.NoError(t, err)
	require.NotNil(t, srv)
	require.NotEmpty(t, srv.Addr)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})

	require.NotEqual(t, occupiedListener.Addr().String(), srv.Addr)

	host, port, err := net.SplitHostPort(srv.Addr)
	require.NoError(t, err)
	require.NotEmpty(t, host)
	require.NotEqual(t, "0", port)
}
