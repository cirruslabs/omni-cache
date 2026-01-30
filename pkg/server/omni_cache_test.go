package server_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

	defaultAddr := "127.0.0.1:12321"
	occupiedListener, err := net.Listen("tcp", defaultAddr)
	if err != nil && !isAddrInUseError(err) {
		t.Skipf("unable to occupy %s: %v", defaultAddr, err)
	}
	if occupiedListener != nil {
		t.Cleanup(func() {
			_ = occupiedListener.Close()
		})
	}

	srv, err := server.StartDefault(context.Background(), nil, testFactory{})
	require.NoError(t, err)
	require.NotNil(t, srv)
	require.NotEmpty(t, srv.Addr)
	t.Cleanup(func() {
		require.NoError(t, srv.Shutdown(context.Background()))
	})

	require.NotEqual(t, defaultAddr, srv.Addr)

	host, port, err := net.SplitHostPort(srv.Addr)
	require.NoError(t, err)
	require.NotEmpty(t, host)
	require.NotEqual(t, "0", port)
}

func isAddrInUseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	return strings.Contains(err.Error(), "address already in use")
}
