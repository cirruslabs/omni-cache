package urlproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type recordingRoundTripper struct {
	responseStatus int
	responseBody   []byte

	called  bool
	lastReq *http.Request
	body    []byte
}

func (rt *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.called = true
	rt.lastReq = req

	if req.Body != nil {
		defer req.Body.Close()
		body, _ := io.ReadAll(req.Body)
		rt.body = body
	}

	status := rt.responseStatus
	if status == 0 {
		status = http.StatusOK
	}

	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader(rt.responseBody)),
		Header:     http.Header{},
	}, nil
}

type failingRoundTripper struct{}

func (failingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("default client should not be used")
}

func TestProxyDownloadFromURL_CustomHTTPClient(t *testing.T) {
	recordingTransport := &recordingRoundTripper{
		responseBody: []byte("downloaded"),
	}
	proxy := NewProxy(WithHTTPClient(&http.Client{Transport: recordingTransport}))

	defaultClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: failingRoundTripper{}}
	t.Cleanup(func() {
		http.DefaultClient = defaultClient
	})

	rec := httptest.NewRecorder()
	ok := proxy.ProxyDownloadFromURL(context.Background(), rec, &storage.URLInfo{URL: "http://example.com/cache"}, "res")
	require.True(t, ok)

	require.True(t, recordingTransport.called)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "downloaded", rec.Body.String())
}

func TestProxyUploadToURL_CustomHTTPClient(t *testing.T) {
	recordingTransport := &recordingRoundTripper{
		responseStatus: http.StatusOK,
	}
	proxy := NewProxy(WithHTTPClient(&http.Client{Transport: recordingTransport}))

	defaultClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: failingRoundTripper{}}
	t.Cleanup(func() {
		http.DefaultClient = defaultClient
	})

	payload := []byte("upload body")
	rec := httptest.NewRecorder()
	ok := proxy.ProxyUploadToURL(context.Background(), rec, &storage.URLInfo{
		URL: "http://example.com/upload",
		ExtraHeaders: map[string]string{
			"X-Test": "value",
		},
	}, UploadResource{
		Body:          bytes.NewReader(payload),
		ContentLength: int64(len(payload)),
		ResourceName:  "res",
	})
	require.True(t, ok)

	require.True(t, recordingTransport.called)
	require.Equal(t, payload, recordingTransport.body)
	require.Equal(t, "value", recordingTransport.lastReq.Header.Get("X-Test"))
	require.Equal(t, "application/octet-stream", recordingTransport.lastReq.Header.Get("Content-Type"))
	require.Equal(t, http.StatusCreated, rec.Code)
}
