package bazel_remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	"github.com/stretchr/testify/require"
)

type staticDownloadBackend struct {
	downloadInfos []*storage.URLInfo
}

func (b *staticDownloadBackend) DownloadURLs(context.Context, string) ([]*storage.URLInfo, error) {
	return b.downloadInfos, nil
}

func (b *staticDownloadBackend) UploadURL(context.Context, string, map[string]string) (*storage.URLInfo, error) {
	return nil, errors.New("not implemented")
}

func (b *staticDownloadBackend) CacheInfo(context.Context, string, []string) (*storage.CacheInfo, error) {
	return nil, storage.ErrCacheNotFound
}

type partialErrorReader struct {
	chunk []byte
	err   error
	done  bool
}

func (r *partialErrorReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	n := copy(p, r.chunk)
	return n, r.err
}

type sequenceTransport struct {
	firstBody []byte
	finalBody []byte
	calls     int
}

func (t *sequenceTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	t.calls++
	var reader io.Reader
	if t.calls == 1 {
		reader = &partialErrorReader{
			chunk: t.firstBody,
			err:   io.ErrUnexpectedEOF,
		}
	} else {
		reader = bytes.NewReader(t.finalBody)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(reader),
		Header:     make(http.Header),
	}, nil
}

func TestCASStoreDownloadToWriterRetriesDoNotAppendPartialData(t *testing.T) {
	expected := []byte("clean download payload")
	transport := &sequenceTransport{
		firstBody: []byte("partial"),
		finalBody: expected,
	}
	backend := &staticDownloadBackend{
		downloadInfos: []*storage.URLInfo{
			{URL: "http://cache.local/broken"},
			{URL: "http://cache.local/healthy"},
		},
	}

	proxy := urlproxy.NewProxy(urlproxy.WithHTTPClient(&http.Client{
		Transport: transport,
	}))
	store := newCASStore(backend, proxy)

	var result bytes.Buffer
	err := store.DownloadToWriter(
		t.Context(),
		"instance",
		digestForData(expected),
		&result,
	)
	require.NoError(t, err)
	require.Equal(t, expected, result.Bytes())
	require.Equal(t, 2, transport.calls)
}

func TestCASStoreExistsRecordsHitMiss(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	cas, _ := newTestStores(t)

	missingDigest := digestForData([]byte("missing"))
	exists, err := cas.Exists(t.Context(), "instance", missingDigest)
	require.NoError(t, err)
	require.False(t, exists)

	snapshot := stats.Default().Snapshot()
	require.EqualValues(t, 0, snapshot.CacheHits)
	require.EqualValues(t, 1, snapshot.CacheMisses)

	stats.Default().Reset()

	data := []byte("present")
	digest := digestForData(data)
	require.NoError(t, cas.UploadBytes(t.Context(), "instance", digest, data))

	exists, err = cas.Exists(t.Context(), "instance", digest)
	require.NoError(t, err)
	require.True(t, exists)

	snapshot = stats.Default().Snapshot()
	require.EqualValues(t, 1, snapshot.CacheHits)
	require.EqualValues(t, 0, snapshot.CacheMisses)
}

func TestCASStoreDownloadToWriterRecordsHitMiss(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	cas, _ := newTestStores(t)

	data := []byte("downloadable")
	digest := digestForData(data)
	require.NoError(t, cas.UploadBytes(t.Context(), "instance", digest, data))

	stats.Default().Reset()

	var buffer bytes.Buffer
	err := cas.DownloadToWriter(t.Context(), "instance", digest, &buffer)
	require.NoError(t, err)
	require.Equal(t, data, buffer.Bytes())

	snapshot := stats.Default().Snapshot()
	require.EqualValues(t, 1, snapshot.CacheHits)
	require.EqualValues(t, 0, snapshot.CacheMisses)

	stats.Default().Reset()

	err = cas.DownloadToWriter(t.Context(), "instance", digestForData([]byte("missing")), &buffer)
	require.ErrorIs(t, err, storage.ErrCacheNotFound)

	snapshot = stats.Default().Snapshot()
	require.EqualValues(t, 0, snapshot.CacheHits)
	require.EqualValues(t, 1, snapshot.CacheMisses)
}

var _ storage.BlobStorageBackend = (*staticDownloadBackend)(nil)
