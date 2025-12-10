package storage_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	// S3 requires minimum 5MB for all parts except the last one
	minPartSize = 5 * 1024 * 1024
)

func TestMultipartUpload(t *testing.T) {
	ctx := context.Background()
	stor := testutil.NewMultipartStorage(t)

	key := "test-multipart/" + uuid.NewString()

	// Create multipart upload
	uploadID, err := stor.CreateMultipartUpload(ctx, key, nil)
	require.NoError(t, err)
	require.NotEmpty(t, uploadID)

	// Prepare test data - S3 requires minimum 5MB parts except for the last one
	part1Data := []byte(strings.Repeat("A", minPartSize))
	part2Data := []byte(strings.Repeat("B", minPartSize))
	part3Data := []byte(strings.Repeat("C", 512)) // Last part can be smaller

	var parts []storage.MultipartUploadPart

	// Upload part 1
	part1URL, err := stor.UploadPartURL(ctx, key, uploadID, 1, uint64(len(part1Data)))
	require.NoError(t, err)
	require.NotEmpty(t, part1URL.URL)

	etag1 := uploadPart(t, part1URL, part1Data)
	parts = append(parts, storage.MultipartUploadPart{PartNumber: 1, ETag: etag1})

	// Upload part 2
	part2URL, err := stor.UploadPartURL(ctx, key, uploadID, 2, uint64(len(part2Data)))
	require.NoError(t, err)
	require.NotEmpty(t, part2URL.URL)

	etag2 := uploadPart(t, part2URL, part2Data)
	parts = append(parts, storage.MultipartUploadPart{PartNumber: 2, ETag: etag2})

	// Upload part 3
	part3URL, err := stor.UploadPartURL(ctx, key, uploadID, 3, uint64(len(part3Data)))
	require.NoError(t, err)
	require.NotEmpty(t, part3URL.URL)

	etag3 := uploadPart(t, part3URL, part3Data)
	parts = append(parts, storage.MultipartUploadPart{PartNumber: 3, ETag: etag3})

	// Commit the multipart upload
	err = stor.CommitMultipartUpload(ctx, key, uploadID, parts)
	require.NoError(t, err)

	// Verify the object was created correctly by downloading it
	downloadURLs, err := stor.DownloadURLs(ctx, key)
	require.NoError(t, err)
	require.NotEmpty(t, downloadURLs)

	resp, err := http.Get(downloadURLs[0].URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	downloadedData, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	expectedData := append(append(part1Data, part2Data...), part3Data...)
	require.Equal(t, expectedData, downloadedData)
}

func TestMultipartUploadWithMetadata(t *testing.T) {
	ctx := context.Background()
	stor := testutil.NewMultipartStorage(t)

	key := "test-multipart-meta/" + uuid.NewString()
	metadata := map[string]string{
		"custom-key": "custom-value",
	}

	// Create multipart upload with metadata
	uploadID, err := stor.CreateMultipartUpload(ctx, key, metadata)
	require.NoError(t, err)
	require.NotEmpty(t, uploadID)

	// Upload a single part
	partData := []byte("test data with metadata")
	partURL, err := stor.UploadPartURL(ctx, key, uploadID, 1, uint64(len(partData)))
	require.NoError(t, err)

	etag := uploadPart(t, partURL, partData)

	// Commit
	err = stor.CommitMultipartUpload(ctx, key, uploadID, []storage.MultipartUploadPart{
		{PartNumber: 1, ETag: etag},
	})
	require.NoError(t, err)

	// Verify object exists
	downloadURLs, err := stor.DownloadURLs(ctx, key)
	require.NoError(t, err)
	require.NotEmpty(t, downloadURLs)
}

func TestMultipartUploadOutOfOrderParts(t *testing.T) {
	ctx := context.Background()
	stor := testutil.NewMultipartStorage(t)

	key := "test-multipart-ooo/" + uuid.NewString()

	uploadID, err := stor.CreateMultipartUpload(ctx, key, nil)
	require.NoError(t, err)

	// S3 requires minimum 5MB for all parts except the last one
	part1Data := []byte(strings.Repeat("1", minPartSize))
	part2Data := []byte(strings.Repeat("2", minPartSize))
	part3Data := []byte(strings.Repeat("3", 512)) // Last part can be smaller

	// Upload parts out of order: 3, 1, 2
	part3URL, err := stor.UploadPartURL(ctx, key, uploadID, 3, uint64(len(part3Data)))
	require.NoError(t, err)
	etag3 := uploadPart(t, part3URL, part3Data)

	part1URL, err := stor.UploadPartURL(ctx, key, uploadID, 1, uint64(len(part1Data)))
	require.NoError(t, err)
	etag1 := uploadPart(t, part1URL, part1Data)

	part2URL, err := stor.UploadPartURL(ctx, key, uploadID, 2, uint64(len(part2Data)))
	require.NoError(t, err)
	etag2 := uploadPart(t, part2URL, part2Data)

	// Commit - parts must be provided in ascending order for S3
	parts := []storage.MultipartUploadPart{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
		{PartNumber: 3, ETag: etag3},
	}
	err = stor.CommitMultipartUpload(ctx, key, uploadID, parts)
	require.NoError(t, err)

	// Verify content is in correct order
	downloadURLs, err := stor.DownloadURLs(ctx, key)
	require.NoError(t, err)

	resp, err := http.Get(downloadURLs[0].URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	downloadedData, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	expectedData := append(append(part1Data, part2Data...), part3Data...)
	require.Equal(t, expectedData, downloadedData)
}

func uploadPart(t *testing.T, urlInfo *storage.URLInfo, data []byte) string {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, urlInfo.URL, bytes.NewReader(data))
	require.NoError(t, err)

	req.ContentLength = int64(len(data))

	for key, value := range urlInfo.ExtraHeaders {
		req.Header.Set(key, value)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "failed to upload part")

	etag := resp.Header.Get("ETag")
	require.NotEmpty(t, etag, "ETag header should be present in response")

	return etag
}
