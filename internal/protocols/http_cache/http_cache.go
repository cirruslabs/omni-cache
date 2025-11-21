package http_cache

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

type HttpCacheProtocolFactory struct {
	protocols.CachingProtocolFactory
}

func (factory *HttpCacheProtocolFactory) ID() string {
	return "http-cache"
}

func (factory *HttpCacheProtocolFactory) NewInstance(storagBackend storage.BlobStorageBacked, httpClient *http.Client) (protocols.CachingProtocol, error) {
	return &internalHTTPCache{
		storagBackend: storagBackend,
		httpClient:    httpClient,
	}, nil
}

type internalHTTPCache struct {
	http.Handler
	protocols.CachingProtocol
	httpClient    *http.Client
	storagBackend storage.BlobStorageBacked
}

func (httpCache *internalHTTPCache) Register(mux *http.ServeMux) error {
	mux.HandleFunc("GET /{key...}", httpCache.downloadCache)
	mux.HandleFunc("HEAD /{key...}", httpCache.downloadCache)
	mux.HandleFunc("POST /{key...}", httpCache.uploadCacheEntry)
	mux.HandleFunc("PUT /{key...}", httpCache.uploadCacheEntry)
	return nil
}

func (httpCache *internalHTTPCache) downloadCache(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.PathValue("key")

	infos, err := httpCache.storagBackend.DownloadURLs(r.Context(), cacheKey)
	if err != nil {
		slog.ErrorContext(r.Context(), "cache download failed", "cacheKey", cacheKey, "err", err)
		w.WriteHeader(http.StatusNotFound)

		return
	}

	slog.InfoContext(r.Context(), "redirecting cache download", "cacheKey", cacheKey)
	httpCache.proxyDownloadFromURLs(w, r, infos)
}

func (httpCache *internalHTTPCache) proxyDownloadFromURLs(w http.ResponseWriter, r *http.Request, infos []*storage.URLInfo) {
	for _, info := range infos {
		if urlproxy.ProxyDownloadFromURL(r.Context(), w, info) {
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func (httpCache *internalHTTPCache) uploadCacheEntry(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.PathValue("key")

	info, err := httpCache.storagBackend.UploadURL(r.Context(), cacheKey, nil)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to initialized uploading of %s cache! %s", cacheKey, err)
		slog.ErrorContext(r.Context(), "failed to initialize cache upload", "cacheKey", cacheKey, "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errorMsg))
		return
	}

	urlproxy.ProxyUploadToURL(r.Context(), w, info, urlproxy.UploadResource{
		Body:          r.Body,
		ContentLength: r.ContentLength,
		ResourceName:  cacheKey,
	})
}
