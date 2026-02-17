package http_cache_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	protohttpcache "github.com/cirruslabs/omni-cache/internal/protocols/http_cache"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/protocols/builtin"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestHTTPCache(t *testing.T) {
	baseURL := startServer(t)
	httpCacheObjectURL := baseURL + "/cache/" + uuid.NewString() + "/test.txt"

	// Ensure that the cache entry does not exist
	resp, err := http.Get(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = http.Head(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Create the cache entry
	resp, err = http.Post(httpCacheObjectURL, "text/plain", strings.NewReader("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Ensure that the cache entry now exists
	resp, err = http.Head(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = http.Get(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cacheEntryBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "Hello, World!", string(cacheEntryBody))

	// Delete the cache entry.
	req, err := http.NewRequest(http.MethodDelete, httpCacheObjectURL, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Ensure that the cache entry no longer exists.
	resp, err = http.Head(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = http.Get(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestHTTPCacheHeadDoesNotRecordDownloads(t *testing.T) {
	baseURL := startServer(t)
	httpCacheObjectURL := baseURL + "/cache/" + uuid.NewString() + "/test.txt"

	resp, err := http.Post(httpCacheObjectURL, "text/plain", strings.NewReader("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Reset stats to isolate HEAD behavior.
	req, err := http.NewRequest(http.MethodDelete, baseURL+"/metrics/cache", nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp, err = http.Head(httpCacheObjectURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	req, err = http.NewRequest(http.MethodGet, baseURL+"/metrics/cache", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var summary struct {
		Downloads struct {
			Count int64 `json:"count"`
			Bytes int64 `json:"bytes"`
		} `json:"downloads"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&summary))
	require.EqualValues(t, 0, summary.Downloads.Count)
	require.EqualValues(t, 0, summary.Downloads.Bytes)
}

func TestHTTPCacheHeadBackendErrorTreatedAsMiss(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	testServer, serverStartError := server.Start(
		t.Context(),
		[]net.Listener{listener},
		headErrorStorage{cacheInfoErr: errors.New("backend unavailable")},
		protohttpcache.Factory{},
	)
	require.NoError(t, serverStartError)
	t.Cleanup(func() {
		testServer.Shutdown(context.Background())
	})

	baseURL := "http://" + listener.Addr().String()

	// Reset stats to isolate behavior in this test.
	resetReq, err := http.NewRequest(http.MethodDelete, baseURL+"/metrics/cache", nil)
	require.NoError(t, err)
	resetResp, err := http.DefaultClient.Do(resetReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resetResp.StatusCode)
	require.NoError(t, resetResp.Body.Close())

	req, err := http.NewRequest(http.MethodHead, "http://"+listener.Addr().String()+"/cache/missing", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	metricsReq, err := http.NewRequest(http.MethodGet, baseURL+"/metrics/cache", nil)
	require.NoError(t, err)
	metricsReq.Header.Set("Accept", "application/json")
	metricsResp, err := http.DefaultClient.Do(metricsReq)
	require.NoError(t, err)
	defer metricsResp.Body.Close()
	require.Equal(t, http.StatusOK, metricsResp.StatusCode)

	var summary struct {
		CacheMisses int64 `json:"cache_misses"`
	}
	require.NoError(t, json.NewDecoder(metricsResp.Body).Decode(&summary))
	require.EqualValues(t, 1, summary.CacheMisses)
}

func TestHTTPCacheHeadRecordsHitMiss(t *testing.T) {
	baseURL := startServer(t)
	existingURL := baseURL + "/cache/" + uuid.NewString() + "/test.txt"
	missingURL := baseURL + "/cache/" + uuid.NewString() + "/missing.txt"

	uploadResp, err := http.Post(existingURL, "text/plain", strings.NewReader("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode)
	require.NoError(t, uploadResp.Body.Close())

	// Reset stats to isolate HEAD behavior in this test.
	resetReq, err := http.NewRequest(http.MethodDelete, baseURL+"/metrics/cache", nil)
	require.NoError(t, err)
	resetResp, err := http.DefaultClient.Do(resetReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resetResp.StatusCode)
	require.NoError(t, resetResp.Body.Close())

	missingResp, err := http.Head(missingURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, missingResp.StatusCode)
	require.NoError(t, missingResp.Body.Close())

	existingResp, err := http.Head(existingURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, existingResp.StatusCode)
	require.NoError(t, existingResp.Body.Close())

	metricsReq, err := http.NewRequest(http.MethodGet, baseURL+"/metrics/cache", nil)
	require.NoError(t, err)
	metricsReq.Header.Set("Accept", "application/json")
	metricsResp, err := http.DefaultClient.Do(metricsReq)
	require.NoError(t, err)
	defer metricsResp.Body.Close()
	require.Equal(t, http.StatusOK, metricsResp.StatusCode)

	var summary struct {
		CacheHits   int64 `json:"cache_hits"`
		CacheMisses int64 `json:"cache_misses"`
	}
	require.NoError(t, json.NewDecoder(metricsResp.Body).Decode(&summary))
	require.EqualValues(t, 1, summary.CacheHits)
	require.EqualValues(t, 1, summary.CacheMisses)
}

func startServer(t *testing.T) string {
	t.Helper()

	storage := testutil.NewStorage(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	testServer, serverStartError := server.Start(t.Context(), []net.Listener{listener}, storage, builtin.Factories()...)
	require.NoError(t, serverStartError)
	t.Cleanup(func() {
		testServer.Shutdown(context.Background())
	})

	return "http://" + listener.Addr().String()
}

type headErrorStorage struct {
	cacheInfoErr error
}

func (s headErrorStorage) DownloadURLs(context.Context, string) ([]*storage.URLInfo, error) {
	return nil, storage.ErrCacheNotFound
}

func (s headErrorStorage) UploadURL(context.Context, string, map[string]string) (*storage.URLInfo, error) {
	return nil, errors.New("not implemented")
}

func (s headErrorStorage) CacheInfo(context.Context, string, []string) (*storage.CacheInfo, error) {
	return nil, s.cacheInfoErr
}
