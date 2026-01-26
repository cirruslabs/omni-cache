package ghacache

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cirruslabs/omni-cache/internal/protocols/ghacache/httprange"
	"github.com/cirruslabs/omni-cache/internal/protocols/ghacache/uploadable"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

const (
	APIMountPoint = "/_apis/artifactcache"

	// JavaScript's Number is limited to 2^53-1.
	jsNumberMaxSafeInteger = 9007199254740991
)

type cacheBackend interface {
	storage.MultipartBlobStorageBackend
}

type GHACache struct {
	cacheHost   string
	backend     cacheBackend
	httpClient  *http.Client
	mux         *http.ServeMux
	uploadables sync.Map // map[int64]*uploadable.Uploadable
}

func New(cacheHost string, backend cacheBackend, httpClient *http.Client) *GHACache {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	cache := &GHACache{
		cacheHost:   cacheHost,
		backend:     backend,
		httpClient:  httpClient,
		mux:         http.NewServeMux(),
		uploadables: sync.Map{},
	}

	cache.mux.HandleFunc("GET /cache", cache.get)
	cache.mux.HandleFunc("POST /caches", cache.reserveUploadable)
	cache.mux.HandleFunc("PATCH /caches/{id}", cache.updateUploadable)
	cache.mux.HandleFunc("POST /caches/{id}", cache.commitUploadable)

	return cache
}

func (cache *GHACache) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	cache.mux.ServeHTTP(writer, request)
}

func (cache *GHACache) get(writer http.ResponseWriter, request *http.Request) {
	keys := strings.Split(request.URL.Query().Get("keys"), ",")
	version := request.URL.Query().Get("version")

	keysWithVersions := make([]string, 0, len(keys))
	for _, key := range keys {
		keysWithVersions = append(keysWithVersions, httpCacheKey(key, version))
	}

	cacheKeyPrefixes := keysWithVersions[1:]
	info, err := cache.backend.CacheInfo(request.Context(), keysWithVersions[0], cacheKeyPrefixes)
	if err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			stats.Default().RecordCacheMiss()
			writer.WriteHeader(http.StatusNoContent)
			return
		}

		fail(writer, request, http.StatusInternalServerError, "GHA cache failed to "+
			"retrieve information about cache entry", "key", keys[0], "err", err)
		return
	}

	stats.Default().RecordCacheHit()
	jsonResp := struct {
		Key string `json:"cacheKey"`
		URL string `json:"archiveLocation"`
	}{
		Key: strings.TrimPrefix(info.Key, httpCacheKey("", version)),
		URL: cache.httpCacheURL(request, info.Key),
	}

	writeJSON(writer, request, http.StatusOK, jsonResp)
}

func (cache *GHACache) reserveUploadable(writer http.ResponseWriter, request *http.Request) {
	var jsonReq struct {
		Key     string `json:"key"`
		Version string `json:"version"`
	}

	if err := json.NewDecoder(request.Body).Decode(&jsonReq); err != nil {
		fail(writer, request, http.StatusBadRequest, "GHA cache failed to read/decode the "+
			"JSON passed to the reserve uploadable endpoint", "err", err)
		return
	}

	jsonResp := struct {
		CacheID int64 `json:"cacheId"`
	}{
		CacheID: rand.Int63n(jsNumberMaxSafeInteger),
	}

	uploadID, err := cache.backend.CreateMultipartUpload(request.Context(), httpCacheKey(jsonReq.Key, jsonReq.Version), nil)
	if err != nil {
		fail(writer, request, http.StatusInternalServerError, "GHA cache failed to create "+
			"multipart upload", "key", jsonReq.Key, "version", jsonReq.Version, "err", err)
		return
	}

	cache.uploadables.Store(jsonResp.CacheID, uploadable.New(jsonReq.Key, jsonReq.Version, uploadID))

	writeJSON(writer, request, http.StatusOK, jsonResp)
}

func (cache *GHACache) updateUploadable(writer http.ResponseWriter, request *http.Request) {
	id, ok := getID(request)
	if !ok {
		fail(writer, request, http.StatusBadRequest, "GHA cache failed to get/decode the "+
			"ID passed to the update uploadable endpoint")
		return
	}

	uploadableValue, ok := cache.uploadables.Load(id)
	if !ok {
		fail(writer, request, http.StatusNotFound, "GHA cache failed to find an uploadable",
			"id", id)
		return
	}
	currentUploadable := uploadableValue.(*uploadable.Uploadable)
	currentUploadable.MarkStarted()

	httpRanges, err := httprange.ParseRange(request.Header.Get("Content-Range"), math.MaxInt64)
	if err != nil {
		fail(writer, request, http.StatusBadRequest, "GHA cache failed to parse Content-Range header",
			"header_value", request.Header.Get("Content-Range"), "err", err)
		return
	}

	if len(httpRanges) != 1 {
		fail(writer, request, http.StatusBadRequest, "GHA cache expected exactly one Content-Range value",
			"expected", 1, "actual", len(httpRanges))
		return
	}

	partNumber, err := currentUploadable.RangeToPart.Tell(request.Context(), httpRanges[0].Start, httpRanges[0].Length)
	if err != nil {
		fail(writer, request, http.StatusBadRequest, "GHA cache failed to tell the part number for "+
			"Content-Range header", "header_value", request.Header.Get("Content-Range"), "err", err)
		return
	}

	urlInfo, err := cache.backend.UploadPartURL(request.Context(),
		httpCacheKey(currentUploadable.Key(), currentUploadable.Version()),
		currentUploadable.UploadID(),
		uint32(partNumber),
		uint64(httpRanges[0].Length),
	)
	if err != nil {
		fail(writer, request, http.StatusInternalServerError, "GHA cache failed create pre-signed "+
			"upload part URL", "key", currentUploadable.Key(), "version", currentUploadable.Version(),
			"part_number", partNumber, "err", err)
		return
	}

	uploadPartRequest, err := http.NewRequest(http.MethodPut, urlInfo.URL, request.Body)
	if err != nil {
		fail(writer, request, http.StatusInternalServerError, "GHA cache failed to create upload part "+
			"request", "key", currentUploadable.Key(), "version", currentUploadable.Version(), "part_number", partNumber,
			"err", err)
		return
	}

	// Ensure Content-Length is set so signed request headers match.
	uploadPartRequest.ContentLength = httpRanges[0].Length

	for key, value := range urlInfo.ExtraHeaders {
		uploadPartRequest.Header.Set(key, value)
	}

	uploadPartResponse, err := cache.httpClient.Do(uploadPartRequest)
	if err != nil {
		// Return HTTP 502 to trigger a retry by the Actions Toolkit.
		fail(writer, request, http.StatusBadGateway, "GHA cache failed to upload part",
			"key", currentUploadable.Key(), "version", currentUploadable.Version(), "part_number", partNumber,
			"err", err)
		return
	}
	defer uploadPartResponse.Body.Close()

	if uploadPartResponse.StatusCode != http.StatusOK {
		// Pass through status code to keep Actions Toolkit retry behavior.
		fail(writer, request, uploadPartResponse.StatusCode, "GHA cache failed to upload part",
			"key", currentUploadable.Key(), "version", currentUploadable.Version(), "part_number", partNumber,
			"unexpected_status_code", uploadPartResponse.StatusCode)
		return
	}

	err = currentUploadable.AppendPart(uint32(partNumber), uploadPartResponse.Header.Get("ETag"), httpRanges[0].Length)
	if err != nil {
		fail(writer, request, http.StatusInternalServerError, "GHA cache failed to append part",
			"key", currentUploadable.Key(), "version", currentUploadable.Version(), "part_number", partNumber,
			"err", err)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (cache *GHACache) commitUploadable(writer http.ResponseWriter, request *http.Request) {
	id, ok := getID(request)
	if !ok {
		fail(writer, request, http.StatusBadRequest, "GHA cache failed to get/decode the "+
			"ID passed to the commit uploadable endpoint")
		return
	}

	uploadableValue, ok := cache.uploadables.Load(id)
	if !ok {
		fail(writer, request, http.StatusNotFound, "GHA cache failed to find an uploadable",
			"id", id)
		return
	}
	currentUploadable := uploadableValue.(*uploadable.Uploadable)

	var jsonReq struct {
		Size int64 `json:"size"`
	}

	if err := json.NewDecoder(request.Body).Decode(&jsonReq); err != nil {
		fail(writer, request, http.StatusBadRequest, "GHA cache failed to read/decode "+
			"the JSON passed to the commit uploadable endpoint", "err", err)
		return
	}

	parts, partsSize, err := currentUploadable.Finalize()
	if err != nil {
		fail(writer, request, http.StatusInternalServerError, "GHA cache failed to "+
			"finalize uploadable", "id", id, "err", err)
		return
	}

	if jsonReq.Size != partsSize {
		fail(writer, request, http.StatusBadRequest, "GHA cache detected a cache entry "+
			"size mismatch for uploadable", "id", id, "expected_bytes", partsSize,
			"actual_bytes", jsonReq.Size)
		return
	}

	err = cache.backend.CommitMultipartUpload(request.Context(),
		httpCacheKey(currentUploadable.Key(), currentUploadable.Version()),
		currentUploadable.UploadID(),
		parts,
	)
	if err != nil {
		fail(writer, request, http.StatusInternalServerError, "GHA cache failed to commit multipart upload",
			"id", currentUploadable.UploadID(), "key", currentUploadable.Key(), "version", currentUploadable.Version(),
			"err", err)
		return
	}

	if startedAt, ok := currentUploadable.StartedAt(); ok {
		stats.Default().RecordUpload(partsSize, time.Since(startedAt))
	}

	cache.uploadables.Delete(id)

	writer.WriteHeader(http.StatusCreated)
}

func httpCacheKey(key string, version string) string {
	return fmt.Sprintf("%s-%s", url.PathEscape(version), url.PathEscape(key))
}

func (cache *GHACache) httpCacheURL(request *http.Request, keyWithVersion string) string {
	host := cache.cacheHost
	if host == "" {
		host = request.Host
	}

	scheme := "http"
	if forwarded := request.Header.Get("X-Forwarded-Proto"); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if candidate := strings.TrimSpace(parts[0]); candidate != "" {
			scheme = candidate
		}
	} else if request.TLS != nil {
		scheme = "https"
	}

	rawURL := fmt.Sprintf("%s://%s/%s", scheme, host, url.PathEscape(keyWithVersion))
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	stats.AddSkipHitMissQuery(parsed)
	return parsed.String()
}

func getID(request *http.Request) (int64, bool) {
	idRaw := request.PathValue("id")

	id, err := strconv.ParseInt(idRaw, 10, 64)
	if err != nil {
		return 0, false
	}

	return id, true
}

func writeJSON(writer http.ResponseWriter, request *http.Request, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(payload); err != nil {
		slog.ErrorContext(request.Context(), "GHA cache failed to write JSON response", "err", err)
	}
}

func fail(writer http.ResponseWriter, request *http.Request, status int, msg string, args ...any) {
	message := formatMessage(msg, args...)

	slog.ErrorContext(request.Context(), msg, args...)
	writeJSON(writer, request, status, struct {
		Message string `json:"message"`
	}{
		Message: message,
	})
}

func formatMessage(msg string, args ...any) string {
	var stringBuilder strings.Builder
	logger := slog.New(slog.NewTextHandler(&stringBuilder, nil))
	logger.Error(msg, args...)
	return stringBuilder.String()
}
