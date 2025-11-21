package urlproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func isHTTPScheme(scheme string) bool {
	return scheme == "http" || scheme == "https"
}

func isGRPCScheme(scheme string) bool {
	return scheme == "grpc" || scheme == "grpcs"
}

func newByteStreamClientFromURL(ctx context.Context, info *storage.URLInfo) (bytestream.ByteStreamClient, io.Closer, error) {
	if info == nil {
		return nil, io.NopCloser(strings.NewReader("")), fmt.Errorf("url info is nil")
	}

	u, err := url.Parse(info.URL)
	if err != nil {
		return nil, io.NopCloser(strings.NewReader("")), err
	}

	scheme := strings.ToLower(u.Scheme)

	host := u.Hostname()
	if host == "" {
		return nil, io.NopCloser(strings.NewReader("")), fmt.Errorf("gRPC URL %q does not include host", u.String())
	}

	port := u.Port()
	if port == "" {
		if scheme == "grpcs" {
			port = "443"
		} else {
			port = "80"
		}
	}

	creds := insecure.NewCredentials()
	if scheme == "grpcs" {
		creds = credentials.NewClientTLSFromCert(nil, "")
	}
	address := net.JoinHostPort(host, port)

	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if md := metadata.New(info.ExtraHeaders); len(md) > 0 {
		opts = append(opts,
			grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
				return invoker(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, callOpts...)
			}),
			grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(metadata.NewOutgoingContext(ctx, md), desc, cc, method, callOpts...)
			}),
		)
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, io.NopCloser(strings.NewReader("")), err
	}

	client := bytestream.NewByteStreamClient(conn)

	return client, conn, nil
}
