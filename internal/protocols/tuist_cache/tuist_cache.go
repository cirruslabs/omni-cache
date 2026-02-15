package tuist_cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	tuistopenapi "github.com/cirruslabs/omni-cache/internal/protocols/tuist_cache/openapi"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

const (
	defaultCacheCategory = "builds"

	maxPartSizeBytes int64 = 10 * 1024 * 1024
)

type tuistCache struct {
	tuistopenapi.UnimplementedHandler

	backend    storage.MultipartBlobStorageBackend
	httpClient *http.Client
	uploads    *uploadStore
	server     *tuistopenapi.Server
}

var _ tuistopenapi.Handler = (*tuistCache)(nil)

func newTuistCache(
	backend storage.MultipartBlobStorageBackend,
	httpClient *http.Client,
) (*tuistCache, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	cache := &tuistCache{
		backend:    backend,
		httpClient: httpClient,
		uploads:    newUploadStore(time.Now, 5*time.Minute),
	}

	server, err := tuistopenapi.NewServer(cache, tuistopenapi.WithPathPrefix("/tuist"))
	if err != nil {
		return nil, err
	}
	cache.server = server

	return cache, nil
}

func (t *tuistCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.server.ServeHTTP(w, r)
}

func (t *tuistCache) ModuleCacheArtifactExists(
	ctx context.Context,
	params tuistopenapi.ModuleCacheArtifactExistsParams,
) (tuistopenapi.ModuleCacheArtifactExistsRes, error) {
	key, err := moduleStorageKey(
		params.AccountHandle,
		params.ProjectHandle,
		params.CacheCategory.Or(defaultCacheCategory),
		params.Hash,
		params.Name,
	)
	if err != nil {
		return &tuistopenapi.ModuleCacheArtifactExistsBadRequest{Message: err.Error()}, nil
	}

	if _, err := t.backend.CacheInfo(ctx, key, nil); err != nil {
		if storage.IsNotFoundError(err) {
			stats.Default().RecordCacheMiss()
			return &tuistopenapi.ModuleCacheArtifactExistsNotFound{Message: "artifact not found"}, nil
		}

		slog.ErrorContext(ctx, "tuist module exists lookup failed", "key", key, "err", err)
		return nil, err
	}

	stats.Default().RecordCacheHit()
	return &tuistopenapi.ModuleCacheArtifactExistsNoContent{}, nil
}

func (t *tuistCache) DownloadModuleCacheArtifact(
	ctx context.Context,
	params tuistopenapi.DownloadModuleCacheArtifactParams,
) (tuistopenapi.DownloadModuleCacheArtifactRes, error) {
	key, err := moduleStorageKey(
		params.AccountHandle,
		params.ProjectHandle,
		params.CacheCategory.Or(defaultCacheCategory),
		params.Hash,
		params.Name,
	)
	if err != nil {
		return &tuistopenapi.DownloadModuleCacheArtifactBadRequest{Message: err.Error()}, nil
	}

	infos, err := t.backend.DownloadURLs(ctx, key)
	if err != nil {
		if storage.IsNotFoundError(err) {
			stats.Default().RecordCacheMiss()
			return &tuistopenapi.DownloadModuleCacheArtifactNotFound{Message: "artifact not found"}, nil
		}

		slog.ErrorContext(ctx, "tuist module download URL generation failed", "key", key, "err", err)
		return nil, err
	}

	reader, err := t.openDownloadStream(ctx, infos)
	if err != nil {
		slog.ErrorContext(ctx, "tuist module download failed", "key", key, "err", err)
		return nil, err
	}
	if reader == nil {
		stats.Default().RecordCacheMiss()
		return &tuistopenapi.DownloadModuleCacheArtifactNotFound{Message: "artifact not found"}, nil
	}

	stats.Default().RecordCacheHit()
	return &tuistopenapi.DownloadModuleCacheArtifactOK{Data: newStatsReadCloser(reader)}, nil
}

func (t *tuistCache) StartModuleCacheMultipartUpload(
	ctx context.Context,
	params tuistopenapi.StartModuleCacheMultipartUploadParams,
) (tuistopenapi.StartModuleCacheMultipartUploadRes, error) {
	key, err := moduleStorageKey(
		params.AccountHandle,
		params.ProjectHandle,
		params.CacheCategory.Or(defaultCacheCategory),
		params.Hash,
		params.Name,
	)
	if err != nil {
		return &tuistopenapi.StartModuleCacheMultipartUploadBadRequest{Message: err.Error()}, nil
	}

	if _, err := t.backend.CacheInfo(ctx, key, nil); err == nil {
		stats.Default().RecordCacheHit()
		uploadID := tuistopenapi.NilString{}
		uploadID.SetToNull()
		return &tuistopenapi.StartMultipartUploadResponse{UploadID: uploadID}, nil
	} else if !storage.IsNotFoundError(err) {
		slog.ErrorContext(ctx, "tuist multipart preflight failed", "key", key, "err", err)
		return nil, err
	}
	stats.Default().RecordCacheMiss()

	backendUploadID, err := t.backend.CreateMultipartUpload(ctx, key, nil)
	if err != nil {
		slog.ErrorContext(ctx, "tuist create multipart upload failed", "key", key, "err", err)
		return nil, err
	}

	uploadID := t.uploads.create(key, backendUploadID)
	return &tuistopenapi.StartMultipartUploadResponse{
		UploadID: tuistopenapi.NewNilString(uploadID),
	}, nil
}

func (t *tuistCache) UploadModuleCachePart(
	ctx context.Context,
	req tuistopenapi.UploadModuleCachePartReq,
	params tuistopenapi.UploadModuleCachePartParams,
) (tuistopenapi.UploadModuleCachePartRes, error) {
	if params.PartNumber <= 0 {
		return &tuistopenapi.UploadModuleCachePartBadRequest{Message: "part_number must be a positive integer"}, nil
	}

	partData, err := readPartBody(req.Data, maxPartSizeBytes)
	if err != nil {
		switch {
		case errors.Is(err, errPartTooLarge):
			return &tuistopenapi.UploadModuleCachePartRequestEntityTooLarge{Message: "part exceeds 10MB limit"}, nil
		case errors.Is(err, context.DeadlineExceeded):
			return &tuistopenapi.UploadModuleCachePartRequestTimeout{Message: "request body read timed out"}, nil
		default:
			slog.ErrorContext(ctx, "tuist read multipart part failed", "uploadID", params.UploadID, "partNumber", params.PartNumber, "err", err)
			return &tuistopenapi.UploadModuleCachePartBadRequest{Message: "failed to read part body"}, nil
		}
	}

	key, backendUploadID, err := t.uploads.preparePart(params.UploadID, int64(len(partData)))
	if err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			return &tuistopenapi.UploadModuleCachePartNotFound{Message: "upload not found"}, nil
		case errors.Is(err, errPartTooLarge):
			return &tuistopenapi.UploadModuleCachePartRequestEntityTooLarge{Message: "part exceeds 10MB limit"}, nil
		default:
			slog.ErrorContext(ctx, "tuist prepare multipart part failed", "uploadID", params.UploadID, "partNumber", params.PartNumber, "err", err)
			return nil, err
		}
	}

	etag, err := t.uploadPartToBackend(ctx, key, backendUploadID, params.PartNumber, partData)
	if err != nil {
		slog.ErrorContext(ctx, "tuist upload multipart part failed", "uploadID", params.UploadID, "partNumber", params.PartNumber, "err", err)
		return nil, err
	}

	if err := t.uploads.setPart(params.UploadID, params.PartNumber, etag, int64(len(partData))); err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			return &tuistopenapi.UploadModuleCachePartNotFound{Message: "upload not found"}, nil
		default:
			slog.ErrorContext(ctx, "tuist record multipart part failed", "uploadID", params.UploadID, "partNumber", params.PartNumber, "err", err)
			return nil, err
		}
	}

	return &tuistopenapi.UploadModuleCachePartNoContent{}, nil
}

func (t *tuistCache) CompleteModuleCacheMultipartUpload(
	ctx context.Context,
	req *tuistopenapi.CompleteMultipartUploadRequest,
	params tuistopenapi.CompleteModuleCacheMultipartUploadParams,
) (tuistopenapi.CompleteModuleCacheMultipartUploadRes, error) {
	if req == nil || req.Parts == nil {
		return &tuistopenapi.CompleteModuleCacheMultipartUploadBadRequest{Message: "request body must include parts"}, nil
	}
	for _, part := range req.Parts {
		if part <= 0 {
			return &tuistopenapi.CompleteModuleCacheMultipartUploadBadRequest{Message: "parts must contain positive integers"}, nil
		}
	}

	// Tuist sends only ordered part numbers here; key/backend upload ID and part
	// ETags are resolved from the in-memory upload session.
	completion, err := t.uploads.complete(params.UploadID, req.Parts)
	if err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			return &tuistopenapi.CompleteModuleCacheMultipartUploadNotFound{Message: "upload not found"}, nil
		case errors.Is(err, errPartsMismatch):
			return &tuistopenapi.CompleteModuleCacheMultipartUploadBadRequest{Message: "parts mismatch or missing parts"}, nil
		default:
			slog.ErrorContext(ctx, "tuist complete multipart pre-commit failed", "uploadID", params.UploadID, "err", err)
			return &tuistopenapi.CompleteModuleCacheMultipartUploadInternalServerError{Message: "failed to complete multipart upload"}, nil
		}
	}

	if err := t.backend.CommitMultipartUpload(ctx, completion.key, completion.backendUploadID, completion.parts); err != nil {
		slog.ErrorContext(ctx, "tuist complete multipart commit failed", "uploadID", params.UploadID, "key", completion.key, "err", err)
		return &tuistopenapi.CompleteModuleCacheMultipartUploadInternalServerError{Message: "failed to complete multipart upload"}, nil
	}
	stats.Default().RecordUpload(completion.totalBytes, time.Since(completion.startedAt))
	t.uploads.finalize(params.UploadID)

	return &tuistopenapi.CompleteModuleCacheMultipartUploadNoContent{}, nil
}

func (t *tuistCache) openDownloadStream(ctx context.Context, infos []*storage.URLInfo) (io.ReadCloser, error) {
	var lastErr error

	for _, info := range infos {
		body, status, err := t.fetchFromDownloadURL(ctx, info)
		if err != nil {
			lastErr = err
			continue
		}
		switch status {
		case http.StatusOK:
			return body, nil
		case http.StatusNotFound:
			continue
		default:
			lastErr = fmt.Errorf("download returned status %d", status)
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}

	return nil, nil
}

func (t *tuistCache) fetchFromDownloadURL(ctx context.Context, info *storage.URLInfo) (io.ReadCloser, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.URL, nil)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if resp.StatusCode == http.StatusOK {
		return resp.Body, resp.StatusCode, nil
	}

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	return nil, resp.StatusCode, nil
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

var errPartTooLarge = errors.New("part too large")

func readPartBody(body io.Reader, maxBytes int64) ([]byte, error) {
	if body == nil {
		return nil, nil
	}

	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, errPartTooLarge
	}
	return data, nil
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

type statsReadCloser struct {
	reader    io.ReadCloser
	startedAt time.Time
	bytesRead int64
	recorded  bool
}

func newStatsReadCloser(reader io.ReadCloser) io.ReadCloser {
	if reader == nil {
		return nil
	}
	return &statsReadCloser{
		reader:    reader,
		startedAt: time.Now(),
	}
}

func (r *statsReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytesRead += int64(n)
	if errors.Is(err, io.EOF) {
		r.record()
	}
	return n, err
}

func (r *statsReadCloser) Close() error {
	err := r.reader.Close()
	r.record()
	return err
}

func (r *statsReadCloser) record() {
	if r.recorded {
		return
	}
	r.recorded = true
	stats.Default().RecordDownload(r.bytesRead, time.Since(r.startedAt))
}
