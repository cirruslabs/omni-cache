package tuist_cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	tuistopenapi "github.com/cirruslabs/omni-cache/internal/protocols/tuist_cache/openapi"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
)

const (
	moduleBasePath     = "/api/cache/module"
	moduleArtifactPath = moduleBasePath + "/{id}"
	moduleStartPath    = moduleBasePath + "/start"
	modulePartPath     = moduleBasePath + "/part"
	moduleCompletePath = moduleBasePath + "/complete"

	defaultCacheCategory = "builds"

	maxPartSizeBytes int64 = 10 * 1024 * 1024
)

type tuistCache struct {
	backend    storage.MultipartBlobStorageBackend
	httpClient *http.Client
	urlProxy   *urlproxy.Proxy
	uploads    *uploadStore
}

func newTuistCache(
	backend storage.MultipartBlobStorageBackend,
	httpClient *http.Client,
	urlProxy *urlproxy.Proxy,
) *tuistCache {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if urlProxy == nil {
		urlProxy = urlproxy.NewProxy(urlproxy.WithHTTPClient(httpClient))
	}

	return &tuistCache{
		backend:    backend,
		httpClient: httpClient,
		urlProxy:   urlProxy,
		uploads:    newUploadStore(time.Now, 5*time.Minute),
	}
}

func (t *tuistCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		t.headModuleArtifact(w, r)
	case http.MethodGet:
		t.getModuleArtifact(w, r)
	case http.MethodPost:
		switch r.URL.Path {
		case moduleStartPath:
			t.startMultipart(w, r)
		case modulePartPath:
			t.uploadPart(w, r)
		case moduleCompletePath:
			t.completeMultipart(w, r)
		default:
			http.NotFound(w, r)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (t *tuistCache) headModuleArtifact(w http.ResponseWriter, r *http.Request) {
	request, err := parseModuleArtifactRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	key, err := moduleStorageKey(request.accountHandle, request.projectHandle, request.category, request.hash, request.name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if _, err := t.backend.CacheInfo(r.Context(), key, nil); err != nil {
		if storage.IsNotFoundError(err) {
			writeError(w, http.StatusNotFound, "artifact not found")
			return
		}

		slog.ErrorContext(r.Context(), "tuist module exists lookup failed", "key", key, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to check artifact existence")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (t *tuistCache) getModuleArtifact(w http.ResponseWriter, r *http.Request) {
	request, err := parseModuleArtifactRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	key, err := moduleStorageKey(request.accountHandle, request.projectHandle, request.category, request.hash, request.name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	infos, err := t.backend.DownloadURLs(r.Context(), key)
	if err != nil {
		if storage.IsNotFoundError(err) {
			writeError(w, http.StatusNotFound, "artifact not found")
			return
		}

		slog.ErrorContext(r.Context(), "tuist module download URL generation failed", "key", key, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to prepare artifact download")
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	for _, info := range infos {
		if t.urlProxy.ProxyDownloadFromURL(r.Context(), w, info, key) {
			return
		}
	}

	writeError(w, http.StatusNotFound, "artifact not found")
}

func (t *tuistCache) startMultipart(w http.ResponseWriter, r *http.Request) {
	request, err := parseModuleArtifactRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	key, err := moduleStorageKey(request.accountHandle, request.projectHandle, request.category, request.hash, request.name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if _, err := t.backend.CacheInfo(r.Context(), key, nil); err == nil {
		writeJSON(w, http.StatusOK, tuistopenapi.StartMultipartUploadResponse{})
		return
	} else if !storage.IsNotFoundError(err) {
		slog.ErrorContext(r.Context(), "tuist multipart preflight failed", "key", key, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to start multipart upload")
		return
	}

	backendUploadID, err := t.backend.CreateMultipartUpload(r.Context(), key, nil)
	if err != nil {
		slog.ErrorContext(r.Context(), "tuist create multipart upload failed", "key", key, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to start multipart upload")
		return
	}

	uploadID := t.uploads.create(key, backendUploadID)
	writeJSON(w, http.StatusOK, tuistopenapi.StartMultipartUploadResponse{
		UploadId: &uploadID,
	})
}

func (t *tuistCache) uploadPart(w http.ResponseWriter, r *http.Request) {
	request, err := parseUploadPartRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	partData, err := readPartBody(r, maxPartSizeBytes)
	if err != nil {
		switch {
		case errors.Is(err, errPartTooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, "part exceeds 10MB limit")
		case errors.Is(err, context.DeadlineExceeded):
			writeError(w, http.StatusRequestTimeout, "request body read timed out")
		default:
			slog.ErrorContext(r.Context(), "tuist read multipart part failed", "uploadID", request.uploadID, "partNumber", request.partNumber, "err", err)
			writeError(w, http.StatusBadRequest, "failed to read part body")
		}
		return
	}

	key, backendUploadID, err := t.uploads.preparePart(request.uploadID, int64(len(partData)))
	if err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			writeError(w, http.StatusNotFound, "upload not found")
		case errors.Is(err, errPartTooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, "part exceeds 10MB limit")
		default:
			slog.ErrorContext(r.Context(), "tuist prepare multipart part failed", "uploadID", request.uploadID, "partNumber", request.partNumber, "err", err)
			writeError(w, http.StatusInternalServerError, "failed to upload part")
		}
		return
	}

	etag, err := t.uploadPartToBackend(r.Context(), key, backendUploadID, request.partNumber, partData)
	if err != nil {
		slog.ErrorContext(r.Context(), "tuist upload multipart part failed", "uploadID", request.uploadID, "partNumber", request.partNumber, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to upload part")
		return
	}

	if err := t.uploads.setPart(request.uploadID, request.partNumber, etag); err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			writeError(w, http.StatusNotFound, "upload not found")
		default:
			slog.ErrorContext(r.Context(), "tuist record multipart part failed", "uploadID", request.uploadID, "partNumber", request.partNumber, "err", err)
			writeError(w, http.StatusInternalServerError, "failed to upload part")
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (t *tuistCache) completeMultipart(w http.ResponseWriter, r *http.Request) {
	request, err := parseCompleteMultipartRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	body, err := parseCompleteBody(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Tuist sends only ordered part numbers here; key/backend upload ID and part
	// ETags are resolved from the in-memory upload session.
	completion, err := t.uploads.complete(request.uploadID, body.Parts)
	if err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			writeError(w, http.StatusNotFound, "upload not found")
		case errors.Is(err, errPartsMismatch):
			writeError(w, http.StatusBadRequest, "parts mismatch or missing parts")
		default:
			slog.ErrorContext(r.Context(), "tuist complete multipart pre-commit failed", "uploadID", request.uploadID, "err", err)
			writeError(w, http.StatusInternalServerError, "failed to complete multipart upload")
		}
		return
	}

	if err := t.backend.CommitMultipartUpload(r.Context(), completion.key, completion.backendUploadID, completion.parts); err != nil {
		slog.ErrorContext(r.Context(), "tuist complete multipart commit failed", "uploadID", request.uploadID, "key", completion.key, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to complete multipart upload")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (t *tuistCache) uploadPartToBackend(
	ctx context.Context,
	key string,
	backendUploadID string,
	partNumber int,
	partData []byte,
) (string, error) {
	info, err := t.backend.UploadPartURL(ctx, key, backendUploadID, uint32(partNumber), uint64(len(partData)))
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, info.URL, bytes.NewReader(partData))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(partData))
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseSnippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("upload part returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseSnippet)))
	}

	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	if etag == "" {
		return "", fmt.Errorf("upload part response missing ETag")
	}

	return etag, nil
}

func moduleStorageKey(accountHandle, projectHandle, category, hash, name string) (string, error) {
	if len(hash) < 4 {
		return "", fmt.Errorf("hash must be at least 4 characters")
	}

	shard1 := hash[:2]
	shard2 := hash[2:4]

	return fmt.Sprintf(
		"%s/%s/module/%s/%s/%s/%s/%s",
		accountHandle,
		projectHandle,
		category,
		shard1,
		shard2,
		hash,
		name,
	), nil
}

type moduleArtifactRequest struct {
	accountHandle string
	projectHandle string
	hash          string
	name          string
	category      string
}

func parseModuleArtifactRequest(r *http.Request) (*moduleArtifactRequest, error) {
	accountHandle, err := requiredQueryParam(r, "account_handle")
	if err != nil {
		return nil, err
	}
	projectHandle, err := requiredQueryParam(r, "project_handle")
	if err != nil {
		return nil, err
	}
	hash, err := requiredQueryParam(r, "hash")
	if err != nil {
		return nil, err
	}
	name, err := requiredQueryParam(r, "name")
	if err != nil {
		return nil, err
	}

	category := strings.TrimSpace(r.URL.Query().Get("cache_category"))
	if category == "" {
		category = defaultCacheCategory
	}

	return &moduleArtifactRequest{
		accountHandle: accountHandle,
		projectHandle: projectHandle,
		hash:          hash,
		name:          name,
		category:      category,
	}, nil
}

type uploadPartRequest struct {
	uploadID   string
	partNumber int
}

func parseUploadPartRequest(r *http.Request) (*uploadPartRequest, error) {
	if _, err := requiredQueryParam(r, "account_handle"); err != nil {
		return nil, err
	}
	if _, err := requiredQueryParam(r, "project_handle"); err != nil {
		return nil, err
	}

	uploadID, err := requiredQueryParam(r, "upload_id")
	if err != nil {
		return nil, err
	}

	partNumberRaw, err := requiredQueryParam(r, "part_number")
	if err != nil {
		return nil, err
	}
	partNumber, err := strconv.Atoi(partNumberRaw)
	if err != nil || partNumber <= 0 {
		return nil, fmt.Errorf("part_number must be a positive integer")
	}

	return &uploadPartRequest{
		uploadID:   uploadID,
		partNumber: partNumber,
	}, nil
}

type completeMultipartRequest struct {
	uploadID string
}

func parseCompleteMultipartRequest(r *http.Request) (*completeMultipartRequest, error) {
	if _, err := requiredQueryParam(r, "account_handle"); err != nil {
		return nil, err
	}
	if _, err := requiredQueryParam(r, "project_handle"); err != nil {
		return nil, err
	}

	uploadID, err := requiredQueryParam(r, "upload_id")
	if err != nil {
		return nil, err
	}

	return &completeMultipartRequest{uploadID: uploadID}, nil
}

func requiredQueryParam(r *http.Request, key string) (string, error) {
	value := r.URL.Query().Get(key)
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("missing required query parameter %q", key)
	}
	return value, nil
}

func parseCompleteBody(body io.Reader) (*tuistopenapi.CompleteMultipartUploadRequest, error) {
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()

	var parsed tuistopenapi.CompleteMultipartUploadRequest
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("invalid request body")
	}
	if parsed.Parts == nil {
		return nil, fmt.Errorf("request body must include parts")
	}
	for _, part := range parsed.Parts {
		if part <= 0 {
			return nil, fmt.Errorf("parts must contain positive integers")
		}
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("invalid request body")
	}

	return &parsed, nil
}

var errPartTooLarge = errors.New("part too large")

func readPartBody(r *http.Request, maxBytes int64) ([]byte, error) {
	if r.ContentLength > maxBytes {
		return nil, errPartTooLarge
	}

	data, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, errPartTooLarge
	}
	return data, nil
}

type errorResponse struct {
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Message: message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("failed to encode tuist cache response", "err", err)
	}
}

func equalPartNumbers(lhs []int, rhs []int) bool {
	if len(lhs) != len(rhs) {
		return false
	}

	leftSorted := slices.Clone(lhs)
	rightSorted := slices.Clone(rhs)
	slices.Sort(leftSorted)
	slices.Sort(rightSorted)
	return slices.Equal(leftSorted, rightSorted)
}
