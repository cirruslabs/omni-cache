package urlproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type UploadResource struct {
	Body          io.Reader
	ContentLength int64
	ResourceName  string
}

// ProxyUploadToURL proxies an upload request to the provided URL and responds to w with the proxied status.
func ProxyUploadToURL(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resource UploadResource) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, info.URL, bufio.NewReader(resource.Body))
	if err != nil {
		slog.ErrorContext(ctx, "cache upload request creation failed", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = resource.ContentLength
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to proxy upload of %s cache! %s", resource.ResourceName, err)
		slog.ErrorContext(ctx, "failed to proxy cache upload", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(errorMsg))
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		slog.ErrorContext(
			ctx,
			"cache upload proxy returned error response",
			"resourceName", resource.ResourceName,
			"status", resp.Status,
			"statusCode", resp.StatusCode,
			"uploadURL", info.URL,
			"requestHeaders", req.Header,
		)

		body, bodyErr := io.ReadAll(resp.Body)
		switch {
		case bodyErr != nil:
			slog.ErrorContext(ctx, "failed to read cache upload error response body", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", bodyErr)
		case len(body) > 0:
			slog.ErrorContext(ctx, "cache upload error response body", "resourceName", resource.ResourceName, "uploadURL", info.URL, "responseBody", string(body))
		}
	}
	if resp.StatusCode == http.StatusOK {
		// our semantic is that if the object is created, then we return 201
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(resp.StatusCode)
	}

	return resp.StatusCode < 400
}
