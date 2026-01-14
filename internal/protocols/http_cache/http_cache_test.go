package http_cache_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/protocols/builtin"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestHTTPCache(t *testing.T) {
	storage := testutil.NewStorage(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	testServer, serverStartError := server.Start(t.Context(), listener, storage, builtin.Factories()...)
	require.NoError(t, serverStartError)
	t.Cleanup(func() {
		testServer.Shutdown(context.Background())
	})

	httpCacheObjectURL := "http://" + listener.Addr().String() + "/cache/" + uuid.NewString() + "/test.txt"

	// Ensure that the cache entry does not exist
	resp, err := http.Get(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, err = http.Head(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Create the cache entry
	resp, err = http.Post(httpCacheObjectURL, "text/plain", strings.NewReader("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Ensure that the cache entry now exists
	resp, err = http.Head(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cacheEntryBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "Hello, World!", string(cacheEntryBody))
}
