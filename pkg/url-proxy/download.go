package urlproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
)

// proxyDownloadBufferSize is the buffer size used for copying response bodies.
// We use a larger buffer since we usually proxy large files.
const proxyDownloadBufferSize = 1024 * 1024

// ProxyDownloadFromURL proxies a download request to the provided URL and returns true if streaming succeeded.
// resourceName is used for ByteStream requests.
// incomingRequest is optional and used to forward Range headers and support retry on unexpected EOF.
func (p *Proxy) ProxyDownloadFromURL(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resourceName string, incomingRequest *http.Request) bool {
	scheme := info.Scheme()
	switch {
	case scheme == "" || isHTTPScheme(scheme):
		return p.proxyHTTPDownload(ctx, w, info, incomingRequest)
	case isGRPCScheme(scheme):
		return p.proxyGRPCDownload(ctx, w, info, resourceName)
	default:
		slog.ErrorContext(ctx, "unsupported download URL scheme", "url", info.URL, "scheme", scheme)
		return false
	}
}

func (p *Proxy) proxyHTTPDownload(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, incomingRequest *http.Request) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.URL, nil)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create cache proxy request", "url", info.URL, "err", err)
		return false
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}

	// Support HTTP range requests - forward Range header from incoming request
	var rangeHeaderToUse string
	if incomingRequest != nil {
		if rangeHeader := incomingRequest.Header.Get("Range"); rangeHeader != "" {
			rangeHeaderToUse = rangeHeader
		}
		// Also support X-Ms-Range for Azure Blob Storage compatibility
		if rangeHeader := incomingRequest.Header.Get("X-Ms-Range"); rangeHeader != "" {
			rangeHeaderToUse = rangeHeader
		}
	}
	if rangeHeaderToUse != "" {
		req.Header.Set("Range", rangeHeaderToUse)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "proxy cache request failed", "url", info.URL, "err", err)
		return false
	}
	defer resp.Body.Close()

	successfulStatus := resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent
	if !successfulStatus {
		slog.ErrorContext(ctx, "proxy cache request returned non-successful status", "url", info.URL, "statusCode", resp.StatusCode)
		return false
	}

	// Forward Content-Length header if present
	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		w.Header().Set("Content-Length", contentLength)
	}

	w.WriteHeader(resp.StatusCode)

	// Use a larger buffer since we usually proxy large files
	largeBuffer := make([]byte, proxyDownloadBufferSize)
	bytesRead, err := io.CopyBuffer(w, resp.Body, largeBuffer)
	if err != nil {
		slog.ErrorContext(ctx, "proxy cache download failed", "url", info.URL, "err", err, "bytesRead", bytesRead)

		// Try to recover by adjusting Range header and re-issuing the request
		if errors.Is(err, io.ErrUnexpectedEOF) {
			bytesRecovered, recoverErr := p.proxyRecover(ctx, rangeHeaderToUse, resp, info.URL, bytesRead, w)
			if recoverErr != nil {
				slog.ErrorContext(ctx, "failed to recover proxy cache entry download", "url", info.URL, "err", recoverErr)
			} else {
				slog.InfoContext(ctx, "successfully recovered proxy cache entry download", "url", info.URL, "bytesRecovered", bytesRecovered)
			}
		}

		return false
	}

	slog.InfoContext(ctx, "proxy cache succeeded", "url", info.URL, "bytesProxied", bytesRead)
	return true
}

// proxyRecover attempts to recover from an unexpected EOF by issuing a new request
// with an adjusted Range header to continue from where we left off.
func (p *Proxy) proxyRecover(
	ctx context.Context,
	rangeHeader string,
	upstreamResponse *http.Response,
	url string,
	bytesRead int64,
	writer http.ResponseWriter,
) (int64, error) {
	var start int64
	var end *int64
	var err error

	if rangeHeader != "" {
		// Take into account the Range header specified in a downstream request
		start, end, err = parseRange(rangeHeader)
		if err != nil {
			return 0, fmt.Errorf("failed to parse Range header %q: %w", rangeHeader, err)
		}
	}

	// Retrieve an identifier from the upstream response
	// to detect possible object modification
	var ifRangeValue string

	if eTag := upstreamResponse.Header.Get("ETag"); eTag != "" {
		ifRangeValue = eTag
	} else if lastModified := upstreamResponse.Header.Get("Last-Modified"); lastModified != "" {
		ifRangeValue = lastModified
	} else {
		return 0, fmt.Errorf("no ETag or Last-Modified header found to use for If-Range")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create an additional request: %w", err)
	}

	if end != nil {
		if start+bytesRead > *end {
			return 0, fmt.Errorf("range start + bytes read (%d) is larger than range end (%d)",
				start+bytesRead, *end)
		}

		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start+bytesRead, *end))
	} else {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start+bytesRead))
	}

	req.Header.Set("If-Range", ifRangeValue)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		// Proceed with proxying
	default:
		return 0, fmt.Errorf("got unexpected HTTP %d", resp.StatusCode)
	}

	return io.Copy(writer, resp.Body)
}

func (p *Proxy) proxyGRPCDownload(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resourceName string) bool {
	client, closer, err := newByteStreamClientFromURL(ctx, info, p.grpcDialOptions...)
	if err != nil {
		slog.ErrorContext(ctx, "failed to dial bytestream download", "url", info.URL, "err", err)
		return false
	}
	defer closer.Close()

	if resourceName == "" {
		slog.ErrorContext(ctx, "bytestream download requires non-empty resource name", "url", info.URL)
		return false
	}

	stream, err := client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: resourceName,
	})
	if err != nil {
		slog.ErrorContext(ctx, "failed to start bytestream download", "url", info.URL, "err", err)
		return false
	}

	var bytesRead int64
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.ErrorContext(ctx, "proxy cache gRPC download failed", "url", info.URL, "err", err)
			return false
		}

		if len(msg.GetData()) == 0 {
			continue
		}

		n, err := w.Write(msg.GetData())
		if err != nil {
			slog.ErrorContext(ctx, "failed to write proxied gRPC data", "url", info.URL, "err", err)
			return false
		}
		bytesRead += int64(n)
	}

	slog.InfoContext(ctx, "proxy cache gRPC download succeeded", "url", info.URL, "bytesProxied", bytesRead)
	return bytesRead > 0
}
