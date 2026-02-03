package bazel_remote_asset_test

import (
	"context"
	"net"
	"testing"
	"time"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bazelremoteasset "github.com/cirruslabs/omni-cache/internal/protocols/bazel_remote_asset"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRemoteAssetPushFetchBlob(t *testing.T) {
	conn := setupGRPCConn(t)
	pushClient := remoteasset.NewPushClient(conn)
	fetchClient := remoteasset.NewFetchClient(conn)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	digest := &remoteexecution.Digest{Hash: "deadbeef", SizeBytes: 42}
	qualifiers := []*remoteasset.Qualifier{
		{Name: "checksum", Value: "abc"},
		{Name: "resource_type", Value: "tar"},
	}

	_, err := pushClient.PushBlob(ctx, &remoteasset.PushBlobRequest{
		InstanceName:   "main",
		Uris:           []string{"urn:uuid:1234", "https://example.com/foo"},
		Qualifiers:     qualifiers,
		BlobDigest:     digest,
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.NoError(t, err)

	fetchResp, err := fetchClient.FetchBlob(ctx, &remoteasset.FetchBlobRequest{
		InstanceName: "main",
		Uris:         []string{"https://example.com/foo"},
		Qualifiers: []*remoteasset.Qualifier{
			{Name: "resource_type", Value: "tar"},
			{Name: "checksum", Value: "abc"},
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, codes.OK, fetchResp.GetStatus().GetCode())
	require.Equal(t, "https://example.com/foo", fetchResp.GetUri())
	require.Equal(t, digest.Hash, fetchResp.GetBlobDigest().GetHash())
	require.EqualValues(t, digest.SizeBytes, fetchResp.GetBlobDigest().GetSizeBytes())
	require.Equal(t, remoteexecution.DigestFunction_SHA256, fetchResp.GetDigestFunction())

	qualifierValues := map[string]string{}
	for _, qualifier := range fetchResp.GetQualifiers() {
		qualifierValues[qualifier.GetName()] = qualifier.GetValue()
	}
	require.Equal(t, map[string]string{"checksum": "abc", "resource_type": "tar"}, qualifierValues)

	missingResp, err := fetchClient.FetchBlob(ctx, &remoteasset.FetchBlobRequest{
		InstanceName: "main",
		Uris:         []string{"https://example.com/foo"},
		Qualifiers: []*remoteasset.Qualifier{
			{Name: "checksum", Value: "mismatch"},
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, codes.NotFound, missingResp.GetStatus().GetCode())
}

func TestRemoteAssetPushFetchDirectory(t *testing.T) {
	conn := setupGRPCConn(t)
	pushClient := remoteasset.NewPushClient(conn)
	fetchClient := remoteasset.NewFetchClient(conn)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	digest := &remoteexecution.Digest{Hash: "feedface", SizeBytes: 123}

	_, err := pushClient.PushDirectory(ctx, &remoteasset.PushDirectoryRequest{
		InstanceName:        "secondary",
		Uris:                []string{"urn:uuid:5678"},
		Qualifiers:          []*remoteasset.Qualifier{{Name: "resource_type", Value: "directory"}},
		RootDirectoryDigest: digest,
		DigestFunction:      remoteexecution.DigestFunction_SHA256,
	})
	require.NoError(t, err)

	fetchResp, err := fetchClient.FetchDirectory(ctx, &remoteasset.FetchDirectoryRequest{
		InstanceName: "secondary",
		Uris:         []string{"urn:uuid:5678"},
		Qualifiers:   []*remoteasset.Qualifier{{Name: "resource_type", Value: "directory"}},
	})
	require.NoError(t, err)
	require.EqualValues(t, codes.OK, fetchResp.GetStatus().GetCode())
	require.Equal(t, "urn:uuid:5678", fetchResp.GetUri())
	require.Equal(t, digest.Hash, fetchResp.GetRootDirectoryDigest().GetHash())
	require.EqualValues(t, digest.SizeBytes, fetchResp.GetRootDirectoryDigest().GetSizeBytes())
	require.Equal(t, remoteexecution.DigestFunction_SHA256, fetchResp.GetDigestFunction())
}

func setupGRPCConn(t *testing.T) *grpc.ClientConn {
	t.Helper()

	storage := testutil.NewStorage(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := server.Start(t.Context(), []net.Listener{listener}, storage, bazelremoteasset.Factory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}
