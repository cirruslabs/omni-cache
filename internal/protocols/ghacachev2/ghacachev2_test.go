package ghacachev2_test

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/cirruslabs/omni-cache/internal/api/gharesults"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/protocols/builtin"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGHACacheV2(t *testing.T) {
	httpCacheURL := startServer(t)

	client := gharesults.NewCacheServiceJSONClient(httpCacheURL, &http.Client{})

	cacheKey := uuid.NewString()
	cacheValue := []byte("Hello, World!\n")

	// Ensure that an entry for our cache key is not present
	getCacheEntryDownloadURLRes, err := client.GetCacheEntryDownloadURL(t.Context(), &gharesults.GetCacheEntryDownloadURLRequest{
		Key: cacheKey,
	})
	require.NoError(t, err)
	require.False(t, getCacheEntryDownloadURLRes.Ok)

	// Upload an entry for our cache key
	createCacheEntryRes, err := client.CreateCacheEntry(t.Context(), &gharesults.CreateCacheEntryRequest{
		Key: cacheKey,
	})
	require.NoError(t, err)
	require.True(t, createCacheEntryRes.Ok)

	// Feed the returned pre-signed upload URL to Azure Blob client
	//
	// Unfortunately azblob.ParseURL() just drops the rest of the path,
	// and has no ServiceURL() convenience method, so we have to manually
	// add the "://" and "/_azureblob" below.
	url, err := azblob.ParseURL(createCacheEntryRes.SignedUploadUrl)
	require.NoError(t, err)

	blockBlobClient, err := azblob.NewClientWithNoCredential(url.Scheme+"://"+url.Host+"/_azureblob", nil)
	require.NoError(t, err)

	_, err = blockBlobClient.UploadBuffer(t.Context(), url.ContainerName, url.BlobName, cacheValue, &azblob.UploadBufferOptions{})
	require.NoError(t, err)

	// Ensure that an entry for our cache key is present
	// and matches to what we've previously put in the cache
	getCacheEntryDownloadURLResp, err := client.GetCacheEntryDownloadURL(t.Context(), &gharesults.GetCacheEntryDownloadURLRequest{
		Key: cacheKey,
	})
	require.NoError(t, err)
	require.True(t, getCacheEntryDownloadURLResp.Ok)
	require.Equal(t, cacheKey, getCacheEntryDownloadURLResp.MatchedKey)

	downloadResp, err := http.Get(getCacheEntryDownloadURLResp.SignedDownloadUrl)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, downloadResp.StatusCode)

	downloadRespBodyBytes, err := io.ReadAll(downloadResp.Body)
	require.NoError(t, err)
	require.Equal(t, cacheValue, downloadRespBodyBytes)

	// Ensure that blob properties can be retrieved,
	// this is actively used by GitHub Actions Toolkit
	// to determine whether to enable parallel download
	// or not.
	resp, err := blockBlobClient.DownloadStream(t.Context(), url.ContainerName, url.BlobName, &azblob.DownloadStreamOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp.ContentLength)
	require.EqualValues(t, len(cacheValue), *resp.ContentLength)

	// Ensure that HTTP range requests are supported
	buf := make([]byte, 5)
	n, err := blockBlobClient.DownloadBuffer(t.Context(), url.ContainerName, url.BlobName, buf, &azblob.DownloadBufferOptions{
		Range: azblob.HTTPRange{
			Offset: 7,
			Count:  5,
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 5, n)
	require.Equal(t, []byte("World"), buf)
}

func TestGHACacheV2UploadStream(t *testing.T) {
	httpCacheURL := startServer(t)

	client := gharesults.NewCacheServiceJSONClient(httpCacheURL, &http.Client{})

	testCases := []struct {
		Name      string
		BlockSize int64
	}{
		{
			Name:      "normal-chunks",
			BlockSize: 5 * humanize.MiByte,
		},
		{
			Name:      "small-chunks",
			BlockSize: 1 * humanize.MiByte,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			cacheKey := uuid.NewString()
			cacheValue := make([]byte, 50*humanize.MiByte)
			_, err := cryptorand.Read(cacheValue)
			require.NoError(t, err)

			// Ensure that an entry for our cache key is not present
			getCacheEntryDownloadURLRes, err := client.GetCacheEntryDownloadURL(t.Context(),
				&gharesults.GetCacheEntryDownloadURLRequest{
					Key: cacheKey,
				},
			)
			require.NoError(t, err)
			require.False(t, getCacheEntryDownloadURLRes.Ok)

			// Upload an entry for our cache key
			createCacheEntryRes, err := client.CreateCacheEntry(t.Context(), &gharesults.CreateCacheEntryRequest{
				Key: cacheKey,
			})
			require.NoError(t, err)
			require.True(t, createCacheEntryRes.Ok)

			// Feed the returned pre-signed upload URL to Azure Blob client
			//
			// Unfortunately azblob.ParseURL() just drops the rest of the path,
			// and has no ServiceURL() convenience method, so we have to manually
			// add the "://" and "/_azureblob" below.
			url, err := azblob.ParseURL(createCacheEntryRes.SignedUploadUrl)
			require.NoError(t, err)

			blockBlobClient, err := azblob.NewClientWithNoCredential(
				url.Scheme+"://"+url.Host+"/_azureblob",
				nil,
			)
			require.NoError(t, err)

			r := bytes.NewReader(cacheValue)

			_, err = blockBlobClient.UploadStream(t.Context(), url.ContainerName, url.BlobName, r,
				&azblob.UploadStreamOptions{
					BlockSize: testCase.BlockSize,
				},
			)
			require.NoError(t, err)

			// Ensure that an entry for our cache key is present
			// and matches to what we've previously put in the cache
			getCacheEntryDownloadURLResp, err := client.GetCacheEntryDownloadURL(t.Context(),
				&gharesults.GetCacheEntryDownloadURLRequest{
					Key: cacheKey,
				},
			)
			require.NoError(t, err)
			require.True(t, getCacheEntryDownloadURLResp.Ok)
			require.Equal(t, cacheKey, getCacheEntryDownloadURLResp.MatchedKey)

			downloadResp, err := http.Get(getCacheEntryDownloadURLResp.SignedDownloadUrl)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, downloadResp.StatusCode)

			downloadRespBodyBytes, err := io.ReadAll(downloadResp.Body)
			require.NoError(t, err)
			require.Equal(t, cacheValue, downloadRespBodyBytes)
		})
	}
}

func startServer(t *testing.T) string {
	t.Helper()

	storage := testutil.NewMultipartStorage(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := server.Start(t.Context(), []net.Listener{listener}, storage, builtin.Factories()...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	return "http://" + listener.Addr().String()
}
