package tuist_cache_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	tuistcache "github.com/cirruslabs/omni-cache/internal/protocols/tuist_cache"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	moduleBasePath     = "/api/cache/module"
	moduleStartPath    = moduleBasePath + "/start"
	modulePartPath     = moduleBasePath + "/part"
	moduleCompletePath = moduleBasePath + "/complete"
	maxPartSizeBytes   = 10 * 1024 * 1024
	minPartSizeBytes   = 5 * 1024 * 1024
)

type errorResponse struct {
	Message string `json:"message"`
}

type startMultipartResponse struct {
	UploadID *string `json:"upload_id"`
}

type completeMultipartBody struct {
	Parts []int `json:"parts"`
}

func TestModuleCacheMiss(t *testing.T) {
	baseURL := startTuistCacheServer(t)
	query := moduleQuery("acme", "ios-app", "abcd1234", "artifact.zip", "")

	headReq, err := http.NewRequest(http.MethodHead, baseURL+moduleBasePath+"/abcd1234?"+query.Encode(), nil)
	require.NoError(t, err)
	headResp, err := http.DefaultClient.Do(headReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, headResp.StatusCode)
	require.NoError(t, headResp.Body.Close())

	getResp, err := http.Get(baseURL + moduleBasePath + "/abcd1234?" + query.Encode())
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, getResp.StatusCode)
	defer getResp.Body.Close()

	var payload errorResponse
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&payload))
	require.NotEmpty(t, payload.Message)
}

func TestModuleCacheMultipartRoundTrip(t *testing.T) {
	baseURL := startTuistCacheServer(t)
	client := &http.Client{}

	uploadID := startMultipartUpload(t, client, baseURL, moduleQuery("acme", "ios-app", "abcd1234", "artifact.zip", "builds"))
	require.NotNil(t, uploadID)

	part1 := bytes.Repeat([]byte("a"), minPartSizeBytes)
	part2 := []byte("world")

	uploadPart(t, client, baseURL, "acme", "ios-app", *uploadID, 1, part1)
	uploadPart(t, client, baseURL, "acme", "ios-app", *uploadID, 2, part2)

	completeMultipartUpload(t, client, baseURL, "acme", "ios-app", *uploadID, []int{1, 2}, http.StatusNoContent)

	query := moduleQuery("acme", "ios-app", "abcd1234", "artifact.zip", "builds")

	headReq, err := http.NewRequest(http.MethodHead, baseURL+moduleBasePath+"/abcd1234?"+query.Encode(), nil)
	require.NoError(t, err)
	headResp, err := client.Do(headReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, headResp.StatusCode)
	require.NoError(t, headResp.Body.Close())

	getResp, err := client.Get(baseURL + moduleBasePath + "/abcd1234?" + query.Encode())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	require.Equal(t, "application/octet-stream", getResp.Header.Get("Content-Type"))

	data, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	require.NoError(t, getResp.Body.Close())
	expected := append(append([]byte{}, part1...), part2...)
	require.Equal(t, expected, data)

	secondUploadID := startMultipartUpload(t, client, baseURL, query)
	require.Nil(t, secondUploadID)
}

func TestModuleCacheMultipartErrors(t *testing.T) {
	baseURL := startTuistCacheServer(t)
	client := &http.Client{}

	unknownPartReq, err := http.NewRequest(
		http.MethodPost,
		baseURL+modulePartPath+"?"+partQuery("acme", "ios-app", "missing-upload", 1).Encode(),
		strings.NewReader("data"),
	)
	require.NoError(t, err)
	unknownPartReq.Header.Set("Content-Type", "application/octet-stream")
	unknownPartResp, err := client.Do(unknownPartReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, unknownPartResp.StatusCode)
	require.NoError(t, unknownPartResp.Body.Close())

	completeMultipartUpload(t, client, baseURL, "acme", "ios-app", "missing-upload", []int{1}, http.StatusNotFound)

	tooLargeUploadID := startMultipartUpload(t, client, baseURL, moduleQuery("acme", "ios-app", "bbbb1234", "big.zip", "builds"))
	require.NotNil(t, tooLargeUploadID)

	tooLargePayload := bytes.Repeat([]byte{'x'}, int(maxPartSizeBytes)+1)
	tooLargeReq, err := http.NewRequest(
		http.MethodPost,
		baseURL+modulePartPath+"?"+partQuery("acme", "ios-app", *tooLargeUploadID, 1).Encode(),
		bytes.NewReader(tooLargePayload),
	)
	require.NoError(t, err)
	tooLargeReq.Header.Set("Content-Type", "application/octet-stream")
	tooLargeResp, err := client.Do(tooLargeReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusRequestEntityTooLarge, tooLargeResp.StatusCode)
	require.NoError(t, tooLargeResp.Body.Close())

	mismatchUploadID := startMultipartUpload(t, client, baseURL, moduleQuery("acme", "ios-app", "cccc1234", "mismatch.zip", "tests"))
	require.NotNil(t, mismatchUploadID)
	uploadPart(t, client, baseURL, "acme", "ios-app", *mismatchUploadID, 1, []byte("abc"))
	completeMultipartUpload(t, client, baseURL, "acme", "ios-app", *mismatchUploadID, []int{2}, http.StatusBadRequest)
}

func TestUnimplementedEndpointsReturnNotImplemented(t *testing.T) {
	baseURL := startTuistCacheServer(t)
	client := &http.Client{}

	values := url.Values{
		"account_handle": []string{"acme"},
		"project_handle": []string{"ios-app"},
	}

	req, err := http.NewRequest(http.MethodDelete, baseURL+"/api/cache/clean?"+values.Encode(), nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestCompleteCanRetryAfterCommitFailure(t *testing.T) {
	backend := &failOnceCommitBackend{MultipartBlobStorageBackend: testutil.NewMultipartStorage(t)}
	baseURL := startTuistCacheServerWithStorage(t, backend)
	client := &http.Client{}

	query := moduleQuery("acme", "ios-app", "abcd1234", "artifact.zip", "builds")
	uploadID := startMultipartUpload(t, client, baseURL, query)
	require.NotNil(t, uploadID)

	uploadPart(t, client, baseURL, "acme", "ios-app", *uploadID, 1, []byte("payload"))

	completeMultipartUpload(t, client, baseURL, "acme", "ios-app", *uploadID, []int{1}, http.StatusInternalServerError)
	completeMultipartUpload(t, client, baseURL, "acme", "ios-app", *uploadID, []int{1}, http.StatusNoContent)
}

func startTuistCacheServer(t *testing.T) string {
	t.Helper()

	return startTuistCacheServerWithStorage(t, testutil.NewMultipartStorage(t))
}

func startTuistCacheServerWithStorage(t *testing.T, stor storage.MultipartBlobStorageBackend) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := server.Start(t.Context(), []net.Listener{listener}, stor, tuistcache.Factory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	return "http://" + listener.Addr().String()
}

type failOnceCommitBackend struct {
	storage.MultipartBlobStorageBackend

	mu      sync.Mutex
	failed  bool
	failErr error
}

func (b *failOnceCommitBackend) CommitMultipartUpload(
	ctx context.Context,
	key string,
	uploadID string,
	parts []storage.MultipartUploadPart,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.failed {
		b.failed = true
		if b.failErr == nil {
			return errors.New("simulated transient commit failure")
		}
		return b.failErr
	}

	return b.MultipartBlobStorageBackend.CommitMultipartUpload(ctx, key, uploadID, parts)
}

func moduleQuery(account, project, hash, name, category string) url.Values {
	values := url.Values{
		"account_handle": []string{account},
		"project_handle": []string{project},
		"hash":           []string{hash},
		"name":           []string{name},
	}
	if category != "" {
		values.Set("cache_category", category)
	}
	return values
}

func partQuery(account, project, uploadID string, partNumber int) url.Values {
	values := url.Values{
		"account_handle": []string{account},
		"project_handle": []string{project},
		"upload_id":      []string{uploadID},
		"part_number":    []string{strconv.Itoa(partNumber)},
	}
	return values
}

func startMultipartUpload(t *testing.T, client *http.Client, baseURL string, query url.Values) *string {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+moduleStartPath+"?"+query.Encode(), nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()

	var payload startMultipartResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	return payload.UploadID
}

func uploadPart(
	t *testing.T,
	client *http.Client,
	baseURL string,
	account string,
	project string,
	uploadID string,
	partNumber int,
	data []byte,
) {
	t.Helper()

	req, err := http.NewRequest(
		http.MethodPost,
		baseURL+modulePartPath+"?"+partQuery(account, project, uploadID, partNumber).Encode(),
		bytes.NewReader(data),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func completeMultipartUpload(
	t *testing.T,
	client *http.Client,
	baseURL string,
	account string,
	project string,
	uploadID string,
	parts []int,
	expectedStatus int,
) {
	t.Helper()

	body, err := json.Marshal(completeMultipartBody{Parts: parts})
	require.NoError(t, err)

	values := url.Values{
		"account_handle": []string{account},
		"project_handle": []string{project},
		"upload_id":      []string{uploadID},
	}

	req, err := http.NewRequest(
		http.MethodPost,
		baseURL+moduleCompletePath+"?"+values.Encode(),
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, expectedStatus, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}
