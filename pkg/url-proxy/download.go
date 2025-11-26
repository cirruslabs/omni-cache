package urlproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/url-proxy/simplerange"
	bytestream "google.golang.org/genproto/googleapis/bytestream"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

// ProxyDownloadFromURL proxies a download request to the provided URL and returns true if streaming succeeded.
// resourceName is used for ByteStream requests.
func ProxyDownloadFromURL(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resourceName string) bool {
	scheme := info.Scheme()
	switch {
	case scheme == "" || isHTTPScheme(scheme):
		return proxyHTTPDownload(ctx, w, info)
	case isGRPCScheme(scheme):
		return proxyGRPCDownload(ctx, w, info, resourceName)
	default:
		slog.ErrorContext(ctx, "unsupported download URL scheme", "url", info.URL, "scheme", scheme)
		return false
	}
}

func proxyHTTPDownload(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo) bool {
	req, err := info.NewGetRequestWithContext(ctx, nil)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create cache proxy request", "url", info.URL, "err", err)
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "proxy cache request failed", "url", info.URL, "err", err)
		return false
	}
	defer resp.Body.Close()
	successfulStatus := 100 <= resp.StatusCode && resp.StatusCode < 300
	if !successfulStatus {
		slog.ErrorContext(ctx, "proxy cache request returned non-successful status", "url", info.URL, "statusCode", resp.StatusCode)
		return false
	}
	w.WriteHeader(resp.StatusCode)
	bytesRead, err := io.Copy(w, resp.Body)

	if err == nil {
		slog.InfoContext(ctx, "proxy cache succeeded", "url", info.URL, "bytesProxied", bytesRead)
		return true
	}

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		slog.ErrorContext(ctx, "proxy cache failed", "url", info.URL, "err", err)
		return false
	}

	bytesRecovered, err := proxyHTTPRecover(ctx, resp, info, bytesRead, w)
	if err != nil {
		slog.Error("failed to recover proxy cache entry download",
			"err", err)
		return false
	}

	slog.Info("successfully recovered proxy cache entry download",
		"read", bytesRecovered, "url", info.URL)

	return true
}

func proxyHTTPRecover(
	ctx context.Context,
	upstreamResponse *http.Response,
	info *storage.URLInfo,
	bytesRead int64,
	writer http.ResponseWriter,
) (int64, error) {
	var start int64
	var end *int64
	var err error

	if rangeHeader := info.ExtraHeaders["Range"]; rangeHeader != "" {
		// Take i/nto account the Range header specified in a downstream request
		start, end, err = simplerange.Parse(rangeHeader)
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
		return 0, fmt.Errorf("no ETag or Last-Modifier header found to use for If-Range")
	}

	recoveryHeaders := map[string]string{}

	if end != nil {
		if start+bytesRead > *end {
			return 0, fmt.Errorf("range start + bytes read (%d) is larger than range end (%d)",
				start+bytesRead, *end)
		}

		recoveryHeaders["Range"] = fmt.Sprintf("bytes=%d-%d", start+bytesRead, *end)
	} else {
		recoveryHeaders["Range"] = fmt.Sprintf("bytes=%d-", start+bytesRead)
	}

	recoveryHeaders["If-Range"] = ifRangeValue
	recoveryHeaders["Accept-Encoding"] = "identity"

	req, err := info.NewGetRequestWithContext(ctx, recoveryHeaders)
	if err != nil {
		return 0, fmt.Errorf("failed to create an additional request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
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

func proxyGRPCDownload(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resourceName string) bool {
	client, closer, err := newByteStreamClientFromURL(ctx, info)
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

	w.WriteHeader(http.StatusOK)

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
	return true
}
