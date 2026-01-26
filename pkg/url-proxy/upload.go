package urlproxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	bytestream "google.golang.org/genproto/googleapis/bytestream"

	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type UploadResource struct {
	Body          io.Reader
	ContentLength int64
	ResourceName  string
}

// ProxyUploadToURL proxies an upload request to the provided URL and responds to w with the proxied status.
func (p *Proxy) ProxyUploadToURL(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resource UploadResource) bool {
	scheme := info.Scheme()
	switch {
	case scheme == "" || isHTTPScheme(scheme):
		return p.proxyHTTPUpload(ctx, w, info, resource)
	case isGRPCScheme(scheme):
		return p.proxyGRPCUpload(ctx, w, info, resource)
	default:
		slog.ErrorContext(ctx, "unsupported upload URL scheme", "resourceName", resource.ResourceName, "uploadURL", info.URL, "scheme", scheme)
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}
}

func (p *Proxy) proxyHTTPUpload(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resource UploadResource) bool {
	bodyReader := &countingReader{reader: resource.Body}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, info.URL, bufio.NewReader(bodyReader))
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

	startedAt := time.Now()
	resp, err := p.httpClient.Do(req)
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

	if resp.StatusCode < http.StatusBadRequest {
		uploadedBytes := bodyReader.Bytes()
		if uploadedBytes == 0 && resource.ContentLength > 0 {
			uploadedBytes = resource.ContentLength
		}
		stats.Default().RecordUpload(uploadedBytes, time.Since(startedAt))
	}

	return resp.StatusCode < 400
}

func (p *Proxy) proxyGRPCUpload(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resource UploadResource) bool {
	client, closer, err := newByteStreamClientFromURL(ctx, info, p.grpcDialOptions...)
	if err != nil {
		slog.ErrorContext(ctx, "failed to dial bytestream upload", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}
	defer closer.Close()

	resourceName := resource.ResourceName
	if resourceName == "" {
		slog.ErrorContext(ctx, "bytestream upload requires non-empty resource name", "resourceName", resource.ResourceName, "uploadURL", info.URL)
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}

	stream, err := client.Write(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "failed to start bytestream upload", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}

	startedAt := time.Now()
	reader := bufio.NewReader(resource.Body)
	buffer := make([]byte, 64*1024)

	var written int64
	for {
		n, readErr := reader.Read(buffer)

		if n > 0 {
			if err := stream.Send(&bytestream.WriteRequest{
				ResourceName: resourceName,
				WriteOffset:  written,
				Data:         buffer[:n],
			}); err != nil {
				slog.ErrorContext(ctx, "failed to send bytestream chunk", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", err)
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
			written += int64(n)
		}

		if readErr != nil {
			if readErr != io.EOF {
				slog.ErrorContext(ctx, "failed to read upload body", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", readErr)
				w.WriteHeader(http.StatusInternalServerError)
				return false
			}
			break
		}
	}

	if err := stream.Send(&bytestream.WriteRequest{
		ResourceName: resourceName,
		WriteOffset:  written,
		FinishWrite:  true,
	}); err != nil {
		slog.ErrorContext(ctx, "failed to finish bytestream upload", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		slog.ErrorContext(ctx, "failed to close bytestream upload", "resourceName", resource.ResourceName, "uploadURL", info.URL, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}

	if resp.GetCommittedSize() != 0 && resp.GetCommittedSize() != written {
		slog.WarnContext(
			ctx,
			"bytestream upload committed size differs from bytes sent",
			"resourceName", resource.ResourceName,
			"uploadURL", info.URL,
			"bytesSent", written,
			"bytesCommitted", resp.GetCommittedSize(),
		)
	}

	w.WriteHeader(http.StatusCreated)
	stats.Default().RecordUpload(written, time.Since(startedAt))
	return true
}

// UploadFromReader streams the provided reader to the upload URL.
func (p *Proxy) UploadFromReader(ctx context.Context, info *storage.URLInfo, resourceName string, body io.Reader, contentLength int64) error {
	if body == nil {
		return fmt.Errorf("upload body is nil")
	}

	scheme := info.Scheme()
	switch {
	case scheme == "" || isHTTPScheme(scheme):
		return p.uploadHTTPFromReader(ctx, info, body, contentLength)
	case isGRPCScheme(scheme):
		return p.uploadGRPCFromReader(ctx, info, resourceName, body)
	default:
		return fmt.Errorf("unsupported upload URL scheme %q", scheme)
	}
}

func (p *Proxy) uploadHTTPFromReader(ctx context.Context, info *storage.URLInfo, body io.Reader, contentLength int64) error {
	bodyReader := &countingReader{reader: body}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, info.URL, bufio.NewReader(bodyReader))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if contentLength >= 0 {
		req.ContentLength = contentLength
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}

	startedAt := time.Now()
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("upload returned status %d", resp.StatusCode)
	}

	uploadedBytes := bodyReader.Bytes()
	if uploadedBytes == 0 && contentLength > 0 {
		uploadedBytes = contentLength
	}
	stats.Default().RecordUpload(uploadedBytes, time.Since(startedAt))
	return nil
}

func (p *Proxy) uploadGRPCFromReader(ctx context.Context, info *storage.URLInfo, resourceName string, body io.Reader) error {
	if resourceName == "" {
		return fmt.Errorf("bytestream upload requires non-empty resource name")
	}

	client, closer, err := newByteStreamClientFromURL(ctx, info, p.grpcDialOptions...)
	if err != nil {
		return err
	}
	defer closer.Close()

	stream, err := client.Write(ctx)
	if err != nil {
		return err
	}

	startedAt := time.Now()
	reader := bufio.NewReader(body)
	buffer := make([]byte, 64*1024)

	var written int64
	for {
		n, readErr := reader.Read(buffer)

		if n > 0 {
			if err := stream.Send(&bytestream.WriteRequest{
				ResourceName: resourceName,
				WriteOffset:  written,
				Data:         buffer[:n],
			}); err != nil {
				return err
			}
			written += int64(n)
		}

		if readErr != nil {
			if readErr != io.EOF {
				return readErr
			}
			break
		}
	}

	if err := stream.Send(&bytestream.WriteRequest{
		ResourceName: resourceName,
		WriteOffset:  written,
		FinishWrite:  true,
	}); err != nil {
		return err
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	if resp.GetCommittedSize() != 0 && resp.GetCommittedSize() != written {
		return fmt.Errorf("bytestream committed size differs from bytes sent")
	}

	stats.Default().RecordUpload(written, time.Since(startedAt))
	return nil
}
