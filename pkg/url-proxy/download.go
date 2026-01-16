package urlproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
)

// ProxyDownloadFromURL proxies a download request to the provided URL and returns true if streaming succeeded.
// resourceName is used for ByteStream requests.
func (p *Proxy) ProxyDownloadFromURL(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo, resourceName string) bool {
	scheme := info.Scheme()
	switch {
	case scheme == "" || isHTTPScheme(scheme):
		return p.proxyHTTPDownload(ctx, w, info)
	case isGRPCScheme(scheme):
		return p.proxyGRPCDownload(ctx, w, info, resourceName)
	default:
		slog.ErrorContext(ctx, "unsupported download URL scheme", "url", info.URL, "scheme", scheme)
		return false
	}
}

func (p *Proxy) proxyHTTPDownload(ctx context.Context, w http.ResponseWriter, info *storage.URLInfo) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.URL, nil)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create cache proxy request", "url", info.URL, "err", err)
		return false
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := p.httpClient.Do(req)
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

// DownloadToWriter streams content from the provided URL info into the writer.
func (p *Proxy) DownloadToWriter(ctx context.Context, info *storage.URLInfo, resourceName string, w io.Writer) error {
	if w == nil {
		return fmt.Errorf("download writer is nil")
	}

	scheme := info.Scheme()
	switch {
	case scheme == "" || isHTTPScheme(scheme):
		return p.downloadHTTPToWriter(ctx, info, w)
	case isGRPCScheme(scheme):
		return p.downloadGRPCToWriter(ctx, info, resourceName, w)
	default:
		return fmt.Errorf("unsupported download URL scheme %q", scheme)
	}
}

func (p *Proxy) downloadHTTPToWriter(ctx context.Context, info *storage.URLInfo, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.URL, nil)
	if err != nil {
		return err
	}
	for k, v := range info.ExtraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("download returned non-successful status %d", resp.StatusCode)
	}

	_, err = io.Copy(w, resp.Body)
	return err
}

func (p *Proxy) downloadGRPCToWriter(ctx context.Context, info *storage.URLInfo, resourceName string, w io.Writer) error {
	if resourceName == "" {
		return fmt.Errorf("bytestream download requires non-empty resource name")
	}

	client, closer, err := newByteStreamClientFromURL(ctx, info, p.grpcDialOptions...)
	if err != nil {
		return err
	}
	defer closer.Close()

	stream, err := client.Read(ctx, &bytestream.ReadRequest{
		ResourceName: resourceName,
	})
	if err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if len(msg.GetData()) == 0 {
			continue
		}

		if _, err := w.Write(msg.GetData()); err != nil {
			return err
		}
	}

	return nil
}
