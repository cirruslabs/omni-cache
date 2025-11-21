package storage

import (
	"context"
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

type BlobStorageBacked interface {
	DownloadURLs(ctx context.Context, key string) ([]*URLInfo, error)
	UploadURL(ctx context.Context, key string, metadate map[string]string) (*URLInfo, error)
}
