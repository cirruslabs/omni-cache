package storage

import (
	"context"
	"errors"
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

// MultipartUploadPart represents a completed part in a multipart upload.
type MultipartUploadPart struct {
	PartNumber uint32
	ETag       string
}

// CacheInfo describes a cache entry stored in the backend.
type CacheInfo struct {
	Key       string
	SizeBytes int64
	Metadata  map[string]string
}

// ErrCacheNotFound is returned when a cache entry doesn't exist.
var ErrCacheNotFound = errors.New("cache entry not found")

type BlobStorageBackend interface {
	DownloadURLs(ctx context.Context, key string) ([]*URLInfo, error)
	UploadURL(ctx context.Context, key string, metadate map[string]string) (*URLInfo, error)
	CacheInfo(ctx context.Context, key string, prefixes []string) (*CacheInfo, error)
}

type MultipartBlobStorageBackend interface {
	BlobStorageBackend

	CreateMultipartUpload(ctx context.Context, key string, metadata map[string]string) (uploadID string, err error)
	UploadPartURL(ctx context.Context, key string, uploadID string, partNumber uint32, contentLength uint64) (*URLInfo, error)
	CommitMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartUploadPart) error
}
