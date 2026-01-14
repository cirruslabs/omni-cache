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

func (factory *HttpCacheProtocolFactory) NewInstance(env protocols.Environment) (protocols.CachingProtocol, error) {
	proxy := env.Proxy
	if proxy == nil {
		proxy = urlproxy.NewProxy(urlproxy.WithHTTPClient(env.HTTPClient))
	}
	return &internalHTTPCache{
		storageBackend: env.Storage,
		urlProxy:       proxy,
	}, nil
}

type internalHTTPCache struct {
	urlProxy       *urlproxy.Proxy
	storageBackend storage.BlobStorageBackend
}

func (httpCache *internalHTTPCache) Register(registry *protocols.Registry) error {
	registry.HandleFunc("GET /{key...}", httpCache.downloadCache)
	registry.HandleFunc("HEAD /{key...}", httpCache.downloadCache)
	registry.HandleFunc("POST /{key...}", httpCache.uploadCacheEntry)
	registry.HandleFunc("PUT /{key...}", httpCache.uploadCacheEntry)
	return nil
}

func (httpCache *internalHTTPCache) downloadCache(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.PathValue("key")

	infos, err := httpCache.storageBackend.DownloadURLs(r.Context(), cacheKey)
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
		if httpCache.urlProxy.ProxyDownloadFromURL(r.Context(), w, info, r.PathValue("key")) {
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func (httpCache *internalHTTPCache) uploadCacheEntry(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.PathValue("key")

	info, err := httpCache.storageBackend.UploadURL(r.Context(), cacheKey, nil)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to initialized uploading of %s cache! %s", cacheKey, err)
		slog.ErrorContext(r.Context(), "failed to initialize cache upload", "cacheKey", cacheKey, "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errorMsg))
		return
	}

	httpCache.urlProxy.ProxyUploadToURL(r.Context(), w, info, urlproxy.UploadResource{
		Body:          r.Body,
		ContentLength: r.ContentLength,
		ResourceName:  cacheKey,
	})
}
