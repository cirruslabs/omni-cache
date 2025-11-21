package http_cache

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/storage"
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
		if httpCache.proxyDownloadFromURL(w, r, info) {
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func (httpCache *internalHTTPCache) proxyDownloadFromURL(w http.ResponseWriter, r *http.Request, info *storage.URLInfo) bool {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, info.URL, nil)
	if err != nil {
		slog.ErrorContext(r.Context(), "failed to create cache proxy request", "url", info.URL, "err", err)
		return false
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := httpCache.httpClient.Do(req)
	if err != nil {
		slog.ErrorContext(r.Context(), "proxy cache request failed", "url", info.URL, "err", err)
		return false
	}
	defer resp.Body.Close()
	successfulStatus := 100 <= resp.StatusCode && resp.StatusCode < 300
	if !successfulStatus {
		slog.ErrorContext(r.Context(), "proxy cache request returned non-successful status", "url", info.URL, "statusCode", resp.StatusCode)
		return false
	}
	w.WriteHeader(resp.StatusCode)
	bytesRead, err := io.Copy(w, resp.Body)
	if err != nil {
		slog.ErrorContext(r.Context(), "proxy cache download failed", "url", info.URL, "err", err)
		return false
	}

	slog.InfoContext(r.Context(), "proxy cache succeeded", "url", info.URL, "bytesProxied", bytesRead)
	return true
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
	req, err := http.NewRequest("PUT", info.URL, bufio.NewReader(r.Body))
	if err != nil {
		slog.ErrorContext(r.Context(), "cache upload request creation failed", "cacheKey", cacheKey, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = r.ContentLength
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := httpCache.httpClient.Do(req)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to proxy upload of %s cache! %s", cacheKey, err)
		slog.ErrorContext(r.Context(), "failed to proxy cache upload", "cacheKey", cacheKey, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errorMsg))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		slog.ErrorContext(
			r.Context(),
			"cache upload proxy returned error response",
			"cacheKey", cacheKey,
			"status", resp.Status,
			"statusCode", resp.StatusCode,
			"uploadURL", info.URL,
			"requestHeaders", req.Header,
		)

		body, bodyErr := io.ReadAll(resp.Body)
		switch {
		case bodyErr != nil:
			slog.ErrorContext(r.Context(), "failed to read cache upload error response body", "cacheKey", cacheKey, "uploadURL", info.URL, "err", bodyErr)
		case len(body) > 0:
			slog.ErrorContext(r.Context(), "cache upload error response body", "cacheKey", cacheKey, "uploadURL", info.URL, "responseBody", string(body))
		}
	}
	if resp.StatusCode == http.StatusOK {
		// our semantic is that if the object is created, then we return 201
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(resp.StatusCode)
	}
}
