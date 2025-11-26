package urlproxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type staticRoundTripper struct {
	do func(*http.Request) (*http.Response, error)
}

func (s staticRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return s.do(req)
}

type unexpectedEOFReader struct {
	inner    io.Reader
	numReads int
}

func (f *unexpectedEOFReader) Read(p []byte) (int, error) {
	f.numReads += 1

	// Return 1, then 2, then 3 bytes of the actual content for the first three reads
	if f.numReads <= 3 {
		return f.inner.Read(p[:f.numReads])
	}

	// Then return unexpected EOF
	return 0, io.ErrUnexpectedEOF
}

func TestProxyDownloadFromURLRecoversAfterUnexpectedEOF(t *testing.T) {
	const (
		fullBody = "hello world"
		eTag     = `"test-etag"`
	)

	originalClient := http.DefaultClient
	t.Cleanup(func() {
		http.DefaultClient = originalClient
	})

	var requestCount int
	var firstResponse *http.Response
	mockReponsesFunction := func(req *http.Request) (*http.Response, error) {
		requestCount++

		switch requestCount {
		case 1:
			// Return a body that fails with io.ErrUnexpectedEOF after 6 bytes.
			header := make(http.Header)
			header.Set("ETag", eTag)

			firstResponse = &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: int64(len(fullBody)),
				Header:        header,
				Body:          io.NopCloser(&unexpectedEOFReader{inner: strings.NewReader(fullBody)}),
			}
			return firstResponse, nil
		case 2:
			require.Equal(t, "bytes=6-", req.Header.Get("Range"))
			require.Equal(t, eTag, req.Header.Get("If-Range"))
			require.Equal(t, "identity", req.Header.Get("Accept-Encoding"))

			return &http.Response{
				StatusCode:    http.StatusPartialContent,
				ContentLength: int64(len(fullBody) - 6),
				Body:          io.NopCloser(strings.NewReader(fullBody[6:])),
				Header:        http.Header{},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected request #%d", requestCount)
		}
	}

	http.DefaultClient = &http.Client{
		Transport: staticRoundTripper{do: mockReponsesFunction},
	}

	recorder := httptest.NewRecorder()
	info := &storage.URLInfo{
		URL: "http://example.com/cache-entry",
	}

	ok := ProxyDownloadFromURL(context.Background(), recorder, info, "")
	require.NotNil(t, firstResponse)
	require.Equal(t, eTag, firstResponse.Header.Get("ETag"))
	require.Truef(t, ok, "expected recovery to succeed, requestCount=%d", requestCount)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, fullBody, recorder.Body.String())
	require.Equal(t, 2, requestCount)
}
