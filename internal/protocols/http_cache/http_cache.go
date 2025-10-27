package http_cache

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

const httpCachePattern = "/{objectname...}"

func newHttpCacheHandler(httpClient *http.Client, storagBackend storage.BlobStorageBacked) http.Handler {
	cache := &internalHTTPCache{
		httpClient:    httpClient,
		storagBackend: storagBackend,
	}
	return cache
}

func init() {
	server.RegisterDefaultCachingServerFactory(&protocols.CachingServerFactory{Pattern: httpCachePattern, Create: newHttpCacheHandler})
}

type internalHTTPCache struct {
	http.Handler
	httpClient    *http.Client
	storagBackend storage.BlobStorageBacked
}

func (httpCache *internalHTTPCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	if key[0] == '/' {
		key = key[1:]
	}
	if len(key) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodGet {
		httpCache.downloadCache(w, r, key)
	} else if r.Method == http.MethodHead {
		httpCache.downloadCache(w, r, key)
	} else if r.Method == http.MethodPost {
		httpCache.uploadCacheEntry(w, r, key)
	} else if r.Method == http.MethodPut {
		httpCache.uploadCacheEntry(w, r, key)
	} else {
		log.Printf("Not supported request method: %s\n", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (httpCache *internalHTTPCache) downloadCache(w http.ResponseWriter, r *http.Request, cacheKey string) {
	infos, err := httpCache.storagBackend.DownloadURLs(r.Context(), cacheKey)
	if err != nil {
		log.Printf("%s cache download failed: %v\n", cacheKey, err)

		w.WriteHeader(http.StatusNotFound)
	} else {
		log.Printf("Redirecting cache download of %s\n", cacheKey)
		httpCache.proxyDownloadFromURLs(w, r, infos)
	}
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
		log.Printf("Failed to create a new GET HTTP request to URL %s: %v", info.URL, err)
		return false
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := httpCache.httpClient.Do(req)
	if err != nil {
		log.Printf("Proxying cache %s failed: %v\n", info.URL, err)
		return false
	}
	defer resp.Body.Close()
	successfulStatus := 100 <= resp.StatusCode && resp.StatusCode < 300
	if !successfulStatus {
		log.Printf("Proxying cache %s failed with %d status\n", info.URL, resp.StatusCode)
		return false
	}
	w.WriteHeader(resp.StatusCode)
	bytesRead, err := io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("Proxying cache download for %s failed with %v\n", info.URL, err)
		return false
	}

	log.Printf("Proxying cache %s succeded! Proxies %d bytes!\n", info.URL, bytesRead)
	return true
}

func (httpCache *internalHTTPCache) uploadCacheEntry(w http.ResponseWriter, r *http.Request, cacheKey string) {
	info, err := httpCache.storagBackend.UploadURL(r.Context(), cacheKey, nil)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to initialized uploading of %s cache! %s", cacheKey, err)
		log.Println(errorMsg)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errorMsg))
		return
	}
	req, err := http.NewRequest("PUT", info.URL, bufio.NewReader(r.Body))
	if err != nil {
		log.Printf("%s cache upload failed: %v\n", cacheKey, err)
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
		log.Println(errorMsg)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errorMsg))
		return
	}
	if resp.StatusCode >= 400 {
		log.Printf("Failed to proxy upload of %s cache! %s", cacheKey, resp.Status)
		log.Printf("Headers for PUT request to  %s\n", info.URL)
		req.Header.Write(log.Writer())
		log.Println("Failed response:")
		resp.Write(log.Writer())
	}
	w.WriteHeader(resp.StatusCode)
}
