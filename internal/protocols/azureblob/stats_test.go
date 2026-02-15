package azureblob

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGetBlobRecordsCacheHitMiss(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hit":
			_, _ = w.Write([]byte("payload"))
		case "/miss":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(origin.Close)

	backend := &downloadURLBackend{
		downloadURLs: map[string][]*storage.URLInfo{
			"hit":  {{URL: origin.URL + "/hit"}},
			"miss": {{URL: origin.URL + "/miss"}},
		},
	}
	azure := New(backend, origin.Client())

	hitReq := httptest.NewRequest(http.MethodGet, "/hit", nil)
	hitResp := httptest.NewRecorder()
	azure.ServeHTTP(hitResp, hitReq)
	require.Equal(t, http.StatusOK, hitResp.Code)

	missReq := httptest.NewRequest(http.MethodGet, "/miss", nil)
	missResp := httptest.NewRecorder()
	azure.ServeHTTP(missResp, missReq)
	require.Equal(t, http.StatusNotFound, missResp.Code)

	snapshot := stats.Default().Snapshot()
	require.EqualValues(t, 1, snapshot.CacheHits)
	require.EqualValues(t, 1, snapshot.CacheMisses)
}

func TestHeadBlobRecordsCacheHitMiss(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hit":
			w.Header().Set("Content-Length", "7")
			_, _ = w.Write([]byte("payload"))
		case "/miss":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(origin.Close)

	backend := &downloadURLBackend{
		downloadURLs: map[string][]*storage.URLInfo{
			"hit":  {{URL: origin.URL + "/hit"}},
			"miss": {{URL: origin.URL + "/miss"}},
		},
	}
	azure := New(backend, origin.Client())

	hitReq := httptest.NewRequest(http.MethodHead, "/hit", nil)
	hitResp := httptest.NewRecorder()
	azure.ServeHTTP(hitResp, hitReq)
	require.Equal(t, http.StatusOK, hitResp.Code)

	missReq := httptest.NewRequest(http.MethodHead, "/miss", nil)
	missResp := httptest.NewRecorder()
	azure.ServeHTTP(missResp, missReq)
	require.Equal(t, http.StatusNotFound, missResp.Code)

	snapshot := stats.Default().Snapshot()
	require.EqualValues(t, 1, snapshot.CacheHits)
	require.EqualValues(t, 1, snapshot.CacheMisses)
}

type downloadURLBackend struct {
	downloadURLs map[string][]*storage.URLInfo
}

func (b *downloadURLBackend) DownloadURLs(_ context.Context, key string) ([]*storage.URLInfo, error) {
	infos, ok := b.downloadURLs[key]
	if !ok {
		return nil, storage.ErrCacheNotFound
	}
	return infos, nil
}

func (b *downloadURLBackend) UploadURL(context.Context, string, map[string]string) (*storage.URLInfo, error) {
	return nil, errors.New("not implemented")
}

func (b *downloadURLBackend) CacheInfo(context.Context, string, []string) (*storage.CacheInfo, error) {
	return nil, storage.ErrCacheNotFound
}

func (b *downloadURLBackend) CreateMultipartUpload(context.Context, string, map[string]string) (string, error) {
	return "", errors.New("not implemented")
}

func (b *downloadURLBackend) UploadPartURL(context.Context, string, string, uint32, uint64) (*storage.URLInfo, error) {
	return nil, errors.New("not implemented")
}

func (b *downloadURLBackend) CommitMultipartUpload(context.Context, string, string, []storage.MultipartUploadPart) error {
	return errors.New("not implemented")
}

var _ storage.MultipartBlobStorageBackend = (*downloadURLBackend)(nil)
