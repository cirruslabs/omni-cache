package storage

import "context"

type URLInfo struct {
	URL          string
	ExtraHeaders map[string]string
}

type BlobStorageBacked interface {
	DownloadURLs(ctx context.Context, key string) ([]*URLInfo, error)
	UploadURL(ctx context.Context, key string, metadate map[string]string) (*URLInfo, error)
}
