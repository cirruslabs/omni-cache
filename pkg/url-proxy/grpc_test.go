package urlproxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	bytestream "google.golang.org/genproto/googleapis/bytestream"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type testByteStreamServer struct {
	readChunks  [][]byte
	readMD      metadata.MD
	readResName string

	writeMD      metadata.MD
	writeResName string
	written      bytes.Buffer
}

func (s *testByteStreamServer) Read(req *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.readMD = md
	}
	s.readResName = req.GetResourceName()

	for _, chunk := range s.readChunks {
		if err := stream.Send(&bytestream.ReadResponse{Data: chunk}); err != nil {
			return err
		}
	}

	return nil
}

func (s *testByteStreamServer) Write(stream bytestream.ByteStream_WriteServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.writeMD = md
	}

	var lastOffset int64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if req.GetResourceName() != "" {
			s.writeResName = req.GetResourceName()
		}

		s.written.Write(req.GetData())
		lastOffset = req.GetWriteOffset() + int64(len(req.GetData()))

		if req.GetFinishWrite() {
			break
		}
	}

	return stream.SendAndClose(&bytestream.WriteResponse{
		CommittedSize: lastOffset,
	})
}

func (s *testByteStreamServer) QueryWriteStatus(_ context.Context, _ *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return &bytestream.QueryWriteStatusResponse{
		CommittedSize: int64(s.written.Len()),
		Complete:      true,
	}, nil
}

func startByteStreamServerWithListener(t *testing.T, lis net.Listener, srv bytestream.ByteStreamServer) string {
	server := grpc.NewServer()
	bytestream.RegisterByteStreamServer(server, srv)

	go server.Serve(lis)

	t.Cleanup(func() {
		server.Stop()
		lis.Close()
	})

	return lis.Addr().String()
}

func startByteStreamServer(t *testing.T, srv bytestream.ByteStreamServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	return startByteStreamServerWithListener(t, lis, srv)
}

func startUnixByteStreamServer(t *testing.T, srv bytestream.ByteStreamServer) string {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("unix sockets are not supported on Windows")
	}

	// Keep the unix socket path short to avoid platform-specific length limits.
	socketPath := filepath.Join(shortTempDir(t), "bytestream.sock")
	lis, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	return startByteStreamServerWithListener(t, lis, srv)
}

func shortTempDir(t *testing.T) string {
	t.Helper()

	candidates := []string{"/tmp", os.TempDir()}
	for _, base := range candidates {
		if base == "" {
			continue
		}

		dir, err := os.MkdirTemp(base, "omni-cache-")
		if err != nil {
			continue
		}

		t.Cleanup(func() {
			_ = os.RemoveAll(dir)
		})

		return dir
	}

	return t.TempDir()
}

func TestProxyDownloadFromURL_GRPC(t *testing.T) {
	srv := &testByteStreamServer{
		readChunks: [][]byte{[]byte("hello "), []byte("world")},
	}
	address := startByteStreamServer(t, srv)
	proxy := NewProxy()

	info := &storage.URLInfo{
		URL: "grpc://" + address,
		ExtraHeaders: map[string]string{
			"X-Test-Meta": "download-md",
		},
	}

	rr := httptest.NewRecorder()
	ok := proxy.ProxyDownloadFromURL(context.Background(), rr, info, "cache-key")
	require.True(t, ok)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "hello world", rr.Body.String())
	require.Equal(t, "cache-key", srv.readResName)
	require.Equal(t, []string{"download-md"}, srv.readMD.Get("x-test-meta"))
}

func TestProxyUploadToURL_GRPC(t *testing.T) {
	srv := &testByteStreamServer{}
	address := startByteStreamServer(t, srv)
	proxy := NewProxy()

	payload := []byte("upload body data")
	info := &storage.URLInfo{
		URL: "grpc://" + address,
		ExtraHeaders: map[string]string{
			"X-Test-Meta": "upload-md",
		},
	}

	rr := httptest.NewRecorder()
	ok := proxy.ProxyUploadToURL(context.Background(), rr, info, UploadResource{
		Body:          bytes.NewReader(payload),
		ContentLength: int64(len(payload)),
		ResourceName:  "cache-key",
	})

	require.True(t, ok)
	require.Equal(t, http.StatusCreated, rr.Code)
	require.Equal(t, payload, srv.written.Bytes())
	require.Equal(t, "cache-key", srv.writeResName)
	require.Equal(t, []string{"upload-md"}, srv.writeMD.Get("x-test-meta"))
}

func TestProxyDownloadFromURL_GRPCCustomDialOption(t *testing.T) {
	srv := &testByteStreamServer{
		readChunks: [][]byte{[]byte("custom")},
	}
	address := startByteStreamServer(t, srv)

	var dialerCalled bool
	var dialerAddr string
	customDialer := grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		dialerCalled = true
		dialerAddr = addr
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	})

	proxy := NewProxy(WithGRPCDialOptions(customDialer))

	rr := httptest.NewRecorder()
	ok := proxy.ProxyDownloadFromURL(context.Background(), rr, &storage.URLInfo{URL: "grpc://" + address}, "cache-key")
	require.True(t, ok)
	require.Equal(t, http.StatusOK, rr.Code)
	require.True(t, dialerCalled)
	require.Equal(t, address, dialerAddr)
}

func TestProxyDownloadFromURL_UnixGRPC(t *testing.T) {
	srv := &testByteStreamServer{
		readChunks: [][]byte{[]byte("unix grpc")},
	}
	socketPath := startUnixByteStreamServer(t, srv)
	proxy := NewProxy()

	info := &storage.URLInfo{
		URL: "unix://" + socketPath,
		ExtraHeaders: map[string]string{
			"X-Test-Meta": "unix-download",
		},
	}

	rr := httptest.NewRecorder()
	ok := proxy.ProxyDownloadFromURL(context.Background(), rr, info, "cache-key")
	require.True(t, ok)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "unix grpc", rr.Body.String())
	require.Equal(t, "cache-key", srv.readResName)
	require.Equal(t, []string{"unix-download"}, srv.readMD.Get("x-test-meta"))
}

func TestProxyUploadToURL_UnixGRPC(t *testing.T) {
	srv := &testByteStreamServer{}
	socketPath := startUnixByteStreamServer(t, srv)
	proxy := NewProxy()

	payload := []byte("unix upload body")
	info := &storage.URLInfo{
		URL: "unix://" + socketPath,
		ExtraHeaders: map[string]string{
			"X-Test-Meta": "unix-upload",
		},
	}

	rr := httptest.NewRecorder()
	ok := proxy.ProxyUploadToURL(context.Background(), rr, info, UploadResource{
		Body:          bytes.NewReader(payload),
		ContentLength: int64(len(payload)),
		ResourceName:  "cache-key",
	})

	require.True(t, ok)
	require.Equal(t, http.StatusCreated, rr.Code)
	require.Equal(t, payload, srv.written.Bytes())
	require.Equal(t, "cache-key", srv.writeResName)
	require.Equal(t, []string{"unix-upload"}, srv.writeMD.Get("x-test-meta"))
}

// errorByteStreamServer returns an error on the first Recv() call
type errorByteStreamServer struct {
	err error
}

func (s *errorByteStreamServer) Read(_ *bytestream.ReadRequest, _ bytestream.ByteStream_ReadServer) error {
	return s.err
}

func (s *errorByteStreamServer) Write(_ bytestream.ByteStream_WriteServer) error {
	return s.err
}

func (s *errorByteStreamServer) QueryWriteStatus(_ context.Context, _ *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, s.err
}

// TestProxyDownloadFromURL_GRPC_Error tests that when gRPC Read fails,
// no HTTP status is written, allowing the caller to write an appropriate error status.
func TestProxyDownloadFromURL_GRPC_Error(t *testing.T) {
	srv := &errorByteStreamServer{
		err: status.Error(codes.NotFound, "cache blob not found"),
	}
	address := startByteStreamServer(t, srv)
	proxy := NewProxy()

	info := &storage.URLInfo{
		URL: "grpc://" + address,
	}

	rr := httptest.NewRecorder()
	ok := proxy.ProxyDownloadFromURL(context.Background(), rr, info, "cache-key")

	// The proxy should return false to indicate failure
	require.False(t, ok)

	// Importantly, no status should have been written yet,
	// so the caller can write an appropriate error status.
	// httptest.ResponseRecorder starts with Code=200, but if WriteHeader wasn't called,
	// we can detect this by checking if the header was explicitly written.
	// Since we returned false without writing, the Code should still be default 200
	// but no bytes should have been written to the body.
	require.Empty(t, rr.Body.String())
}
