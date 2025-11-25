package urlproxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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

func startByteStreamServer(t *testing.T, srv bytestream.ByteStreamServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	bytestream.RegisterByteStreamServer(server, srv)

	go server.Serve(lis)

	t.Cleanup(func() {
		server.Stop()
		lis.Close()
	})

	return lis.Addr().String()
}

func TestProxyDownloadFromURL_GRPC(t *testing.T) {
	srv := &testByteStreamServer{
		readChunks: [][]byte{[]byte("hello "), []byte("world")},
	}
	address := startByteStreamServer(t, srv)

	info := &storage.URLInfo{
		URL: "grpc://" + address,
		ExtraHeaders: map[string]string{
			"X-Test-Meta": "download-md",
		},
	}

	rr := httptest.NewRecorder()
	ok := ProxyDownloadFromURL(context.Background(), rr, info, "cache-key")
	require.True(t, ok)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "hello world", rr.Body.String())
	require.Equal(t, "cache-key", srv.readResName)
	require.Equal(t, []string{"download-md"}, srv.readMD.Get("x-test-meta"))
}

func TestProxyUploadToURL_GRPC(t *testing.T) {
	srv := &testByteStreamServer{}
	address := startByteStreamServer(t, srv)

	payload := []byte("upload body data")
	info := &storage.URLInfo{
		URL: "grpc://" + address,
		ExtraHeaders: map[string]string{
			"X-Test-Meta": "upload-md",
		},
	}

	rr := httptest.NewRecorder()
	ok := ProxyUploadToURL(context.Background(), rr, info, UploadResource{
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
