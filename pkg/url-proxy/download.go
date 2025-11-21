package urlproxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

// ProxyDownloadFromURL proxies a download request to the provided URL and returns true if streaming succeeded.
func ProxyDownloadFromURL(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.URL, nil)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create cache proxy request", "url", info.URL, "err", err)
		return false
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
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
	if err != nil {
		slog.ErrorContext(ctx, "proxy cache download failed", "url", info.URL, "err", err)
		return false
	}

	slog.InfoContext(ctx, "proxy cache succeeded", "url", info.URL, "bytesProxied", bytesRead)
	return true
}
