package http_cache

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

// Factory wires the http-cache protocol.
// Endpoints:
//
//	GET /{key...} downloads a cache entry.
//	PUT or POST /{key...} uploads a cache entry.
type Factory struct{}

func (Factory) ID() string {
	return "http-cache"
}

func (Factory) New(deps protocols.Dependencies) (protocols.Protocol, error) {
	deps = deps.WithDefaults()
	return &protocol{
		storageBackend: deps.Storage,
		urlProxy:       deps.URLProxy,
	}, nil
}

type protocol struct {
	urlProxy       *urlproxy.Proxy
	storageBackend storage.BlobStorageBackend
}

func (p *protocol) Register(registrar *protocols.Registrar) error {
	mux := registrar.HTTP()
	if mux == nil {
		return fmt.Errorf("http mux is nil")
	}

	mux.HandleFunc("GET /{key...}", p.downloadCache)
	mux.HandleFunc("POST /{key...}", p.uploadCacheEntry)
	mux.HandleFunc("PUT /{key...}", p.uploadCacheEntry)
	return nil
}

func (p *protocol) downloadCache(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.PathValue("key")

	infos, err := p.storageBackend.DownloadURLs(r.Context(), cacheKey)
	if err != nil {
		if !stats.ShouldSkipHitMiss(r) && storage.IsNotFoundError(err) {
			stats.Default().RecordCacheMiss()
		}
		slog.ErrorContext(r.Context(), "cache download failed", "cacheKey", cacheKey, "err", err)
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if !stats.ShouldSkipHitMiss(r) {
		stats.Default().RecordCacheHit()
	}
	slog.InfoContext(r.Context(), "redirecting cache download", "cacheKey", cacheKey)
	p.proxyDownloadFromURLs(w, r, infos)
}

func (p *protocol) proxyDownloadFromURLs(w http.ResponseWriter, r *http.Request, infos []*storage.URLInfo) {
	for _, info := range infos {
		if p.urlProxy.ProxyDownloadFromURL(r.Context(), w, info, r.PathValue("key")) {
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func (p *protocol) uploadCacheEntry(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.PathValue("key")

	info, err := p.storageBackend.UploadURL(r.Context(), cacheKey, nil)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to initialized uploading of %s cache! %s", cacheKey, err)
		slog.ErrorContext(r.Context(), "failed to initialize cache upload", "cacheKey", cacheKey, "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errorMsg))
		return
	}

	p.urlProxy.ProxyUploadToURL(r.Context(), w, info, urlproxy.UploadResource{
		Body:          r.Body,
		ContentLength: r.ContentLength,
		ResourceName:  cacheKey,
	})
}
