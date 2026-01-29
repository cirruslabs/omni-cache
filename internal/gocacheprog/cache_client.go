package gocacheprog

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/stats"
)

type CacheClient interface {
	Download(ctx context.Context, key string, dst io.Writer) (bool, error)
	Upload(ctx context.Context, key string, body io.Reader, contentLength int64) error
}

type HTTPCacheClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPCacheClient(baseURL string, client *http.Client) (*HTTPCacheClient, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return nil, fmt.Errorf("cache base URL is empty")
	}
	baseURL = strings.TrimRight(baseURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		return nil, fmt.Errorf("cache base URL must start with http:// or https://")
	}
	if client == nil {
		client = http.DefaultClient
	}

	return &HTTPCacheClient{
		baseURL: baseURL,
		client:  client,
	}, nil
}

func NewUnixSocketCacheClient(socketPath string) (*HTTPCacheClient, error) {
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return nil, fmt.Errorf("unix socket path is empty")
	}

	dialer := &net.Dialer{}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Minute,
	}

	return NewHTTPCacheClient("http://unix", client)
}

func (c *HTTPCacheClient) Download(ctx context.Context, key string, dst io.Writer) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("download destination is nil")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.urlForKey(key), nil)
	if err != nil {
		return false, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, fmt.Errorf("unexpected cache download status %d", resp.StatusCode)
	}

	startedAt := time.Now()
	bytesRead, err := io.Copy(dst, resp.Body)
	if err != nil {
		return false, err
	}
	stats.Default().RecordDownload(bytesRead, time.Since(startedAt))
	return true, nil
}

func (c *HTTPCacheClient) Upload(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	if body == nil {
		return fmt.Errorf("upload body is nil")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.urlForKey(key), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if contentLength >= 0 {
		req.ContentLength = contentLength
	}

	startedAt := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected cache upload status %d", resp.StatusCode)
	}

	if contentLength >= 0 {
		stats.Default().RecordUpload(contentLength, time.Since(startedAt))
	}
	return nil
}

func (c *HTTPCacheClient) urlForKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	return c.baseURL + "/" + key
}
