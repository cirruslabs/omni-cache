package urlproxy

import (
	"net/http"

	"google.golang.org/grpc"
)

// Proxy routes download and upload requests through HTTP or gRPC using the provided clients/options.
type Proxy struct {
	httpClient      *http.Client
	grpcDialOptions []grpc.DialOption
}

type ProxyOption func(*Proxy)

// WithHTTPClient sets the HTTP client used for HTTP transfers. If omitted or nil, http.DefaultClient is used.
func WithHTTPClient(client *http.Client) ProxyOption {
	return func(p *Proxy) {
		p.httpClient = client
	}
}

// WithGRPCDialOptions appends custom gRPC DialOptions used when establishing ByteStream connections.
func WithGRPCDialOptions(opts ...grpc.DialOption) ProxyOption {
	return func(p *Proxy) {
		p.grpcDialOptions = append(p.grpcDialOptions, opts...)
	}
}

// NewProxy builds a Proxy configured via provided options.
func NewProxy(opts ...ProxyOption) *Proxy {
	p := &Proxy{}
	for _, opt := range opts {
		opt(p)
	}
	if p.httpClient == nil {
		p.httpClient = http.DefaultClient
	}
	return p
}
