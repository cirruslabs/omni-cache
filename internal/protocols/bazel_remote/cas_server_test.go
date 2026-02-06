package bazel_remote

import (
	"testing"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestCASBatchUpdateBlobsRejectsHashMismatch(t *testing.T) {
	cas, _ := newTestStores(t)
	server := newCASServer(cas)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName:   "test-instance",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest: &remoteexecution.Digest{Hash: emptySHA256Hash, SizeBytes: 3},
				Data:   []byte("abc"),
			},
		},
	}

	response, err := server.BatchUpdateBlobs(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, response.GetResponses(), 1)
	require.Equal(t, int32(codes.InvalidArgument), response.GetResponses()[0].GetStatus().GetCode())
}

func TestCASFindMissingBlobs(t *testing.T) {
	cas, _ := newTestStores(t)
	server := newCASServer(cas)

	data := []byte("existing")
	digest := digestForData(data)
	require.NoError(t, cas.UploadBytes(t.Context(), "instance", digest, data))

	missingDigest := digestForData([]byte("missing"))
	response, err := server.FindMissingBlobs(t.Context(), &remoteexecution.FindMissingBlobsRequest{
		InstanceName:   "instance",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
		BlobDigests:    []*remoteexecution.Digest{digest, missingDigest},
	})
	require.NoError(t, err)
	require.Len(t, response.GetMissingBlobDigests(), 1)
	require.Equal(t, missingDigest.GetHash(), response.GetMissingBlobDigests()[0].GetHash())
}
