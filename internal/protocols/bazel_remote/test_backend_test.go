package bazel_remote

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type memoryHTTPBackend struct {
	mu      sync.RWMutex
	objects map[string][]byte
	server  *httptest.Server
}

func newMemoryHTTPBackend(t *testing.T) *memoryHTTPBackend {
	t.Helper()

	backend := &memoryHTTPBackend{objects: make(map[string][]byte)}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /upload/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key, err := url.PathUnescape(r.PathValue("key"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		backend.mu.Lock()
		backend.objects[key] = append([]byte(nil), data...)
		backend.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /download/{key...}", func(w http.ResponseWriter, r *http.Request) {
		key, err := url.PathUnescape(r.PathValue("key"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		backend.mu.RLock()
		data, ok := backend.objects[key]
		backend.mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})

	backend.server = httptest.NewServer(mux)
	t.Cleanup(backend.server.Close)

	return backend
}

func (b *memoryHTTPBackend) UploadURL(ctx context.Context, key string, _ map[string]string) (*storage.URLInfo, error) {
	return &storage.URLInfo{URL: b.server.URL + "/upload/" + url.PathEscape(key)}, nil
}

func (b *memoryHTTPBackend) DownloadURLs(ctx context.Context, key string) ([]*storage.URLInfo, error) {
	b.mu.RLock()
	_, ok := b.objects[key]
	b.mu.RUnlock()
	if !ok {
		return nil, storage.ErrCacheNotFound
	}
	return []*storage.URLInfo{{URL: b.server.URL + "/download/" + url.PathEscape(key)}}, nil
}

func (b *memoryHTTPBackend) CacheInfo(ctx context.Context, key string, _ []string) (*storage.CacheInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data, ok := b.objects[key]
	if !ok {
		return nil, storage.ErrCacheNotFound
	}

	return &storage.CacheInfo{
		Key:       key,
		SizeBytes: int64(len(data)),
	}, nil
}

func newTestStores(t *testing.T) (*casStore, *assetStore) {
	t.Helper()

	backend := newMemoryHTTPBackend(t)
	proxy := urlproxy.NewProxy(urlproxy.WithHTTPClient(backend.server.Client()))
	cas := newCASStore(backend, proxy)
	assets := newAssetStore(backend, proxy)
	return cas, assets
}

func newGRPCConn(t *testing.T, register func(server *grpc.Server)) *grpc.ClientConn {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	register(server)

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	return conn
}
