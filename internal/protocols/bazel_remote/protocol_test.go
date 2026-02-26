package bazel_remote

import (
	"net"
	"net/http"
	"strings"
	"testing"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestProtocolRegistersActionCacheService(t *testing.T) {
	backend := newMemoryHTTPBackend(t)
	proto, err := Factory{}.New(protocols.Dependencies{
		Storage: backend,
	}.WithDefaults())
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	require.NoError(t, proto.Register(protocols.NewRegistrar(http.NewServeMux(), grpcServer)))
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	client := remoteexecution.NewActionCacheClient(conn)
	_, err = client.GetActionResult(t.Context(), &remoteexecution.GetActionResultRequest{
		InstanceName:   "instance",
		ActionDigest:   digestForData([]byte("missing")),
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
	require.False(t, strings.Contains(strings.ToLower(st.Message()), "unknown service"))
}
