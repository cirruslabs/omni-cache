package server_test

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type testProtocol struct{}

func (p *testProtocol) Register(registrar *protocols.Registrar) error {
	registrar.HTTP().HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong"))
	})
	return nil
}

type testFactory struct{}

func (testFactory) ID() string {
	return "test"
}

func (testFactory) New(_ protocols.Dependencies) (protocols.Protocol, error) {
	return &testProtocol{}, nil
}

func TestUnixSocketHTTPAndGRPC(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets are not supported on Windows")
	}

	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	socketPath := filepath.Join(shortTempDir(t), "omni-cache.sock")
	unixListener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	srv, err := server.Start(t.Context(), []net.Listener{tcpListener, unixListener}, nil, testFactory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	httpClient := &http.Client{
		Timeout: 2 * time.Second,
	}
	t.Cleanup(func() {
		httpClient.CloseIdleConnections()
	})

	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+tcpListener.Addr().String()+"/ping", nil)
		if err != nil {
			return false
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, time.Minute, time.Second)

	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	healthClient := healthpb.NewHealthClient(conn)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		return err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	}, time.Minute, time.Second)
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
