package storage

import (
	"context"
	"maps"
	"net/http"
	"net/url"
	"strings"
)

type URLInfo struct {
	URL          string
	ExtraHeaders map[string]string
}

// Scheme returns the lower-case URL scheme or empty string if parsing fails.
func (info *URLInfo) Scheme() string {
	if info == nil {
		return ""
	}

	parsed, err := url.Parse(info.URL)
	if err != nil {
		return ""
	}

	return strings.ToLower(parsed.Scheme)
}

func (info *URLInfo) NewGetRequestWithContext(ctx context.Context, overrideHeaders map[string]string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.URL, nil)
	if err != nil {
		return req, err
	}
	combinedHeaders := map[string]string{}
	maps.Copy(combinedHeaders, info.ExtraHeaders)
	maps.Copy(combinedHeaders, overrideHeaders)

	for k, v := range combinedHeaders {
		req.Header.Set(k, v)
	}
	return req, nil
}

type BlobStorageBacked interface {
	DownloadURLs(ctx context.Context, key string) ([]*URLInfo, error)
	UploadURL(ctx context.Context, key string, metadate map[string]string) (*URLInfo, error)
}
