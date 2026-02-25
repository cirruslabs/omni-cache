package bazel_remote

import (
	"testing"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestActionCacheUpdateAndGetRoundTrip(t *testing.T) {
	client := newActionCacheTestClient(t)

	actionDigest := digestForData([]byte("action-digest"))
	expected := &remoteexecution.ActionResult{
		ExitCode:     17,
		StdoutDigest: digestForData([]byte("stdout")),
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path:         "out.txt",
				Digest:       digestForData([]byte("output")),
				IsExecutable: true,
			},
		},
	}

	updated, err := client.UpdateActionResult(t.Context(), &remoteexecution.UpdateActionResultRequest{
		InstanceName:   "instance",
		ActionDigest:   actionDigest,
		ActionResult:   expected,
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.NoError(t, err)
	require.True(t, proto.Equal(expected, updated))

	got, err := client.GetActionResult(t.Context(), &remoteexecution.GetActionResultRequest{
		InstanceName:   "instance",
		ActionDigest:   actionDigest,
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.NoError(t, err)
	require.True(t, proto.Equal(expected, got))
}

func TestActionCacheGetActionResultMissing(t *testing.T) {
	client := newActionCacheTestClient(t)

	_, err := client.GetActionResult(t.Context(), &remoteexecution.GetActionResultRequest{
		InstanceName:   "instance",
		ActionDigest:   digestForData([]byte("missing")),
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestActionCacheGetActionResultInvalidDigest(t *testing.T) {
	client := newActionCacheTestClient(t)

	_, err := client.GetActionResult(t.Context(), &remoteexecution.GetActionResultRequest{
		InstanceName:   "instance",
		ActionDigest:   &remoteexecution.Digest{Hash: "abc", SizeBytes: 3},
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestActionCacheGetActionResultInvalidDigestFunction(t *testing.T) {
	client := newActionCacheTestClient(t)

	_, err := client.GetActionResult(t.Context(), &remoteexecution.GetActionResultRequest{
		InstanceName:   "instance",
		ActionDigest:   digestForData([]byte("action")),
		DigestFunction: remoteexecution.DigestFunction_MD5,
	})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestActionCacheUpdateActionResultRequiresResult(t *testing.T) {
	client := newActionCacheTestClient(t)

	_, err := client.UpdateActionResult(t.Context(), &remoteexecution.UpdateActionResultRequest{
		InstanceName:   "instance",
		ActionDigest:   digestForData([]byte("action")),
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func newActionCacheTestClient(t *testing.T) remoteexecution.ActionCacheClient {
	t.Helper()

	cas, _ := newTestStores(t)
	store := newActionCacheStore(cas.backend, cas.proxy)
	conn := newGRPCConn(t, func(server *grpc.Server) {
		remoteexecution.RegisterActionCacheServer(server, newActionCacheServer(store))
	})

	return remoteexecution.NewActionCacheClient(conn)
}
