package urlproxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

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

	socketPath := filepath.Join(t.TempDir(), "bytestream.sock")
	lis, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	return startByteStreamServerWithListener(t, lis, srv)
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
