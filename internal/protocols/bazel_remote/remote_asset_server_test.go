package bazel_remote

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	remoteasset "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/asset/v1"
	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestRemoteAssetFetchBlobCachesOriginResult(t *testing.T) {
	cas, assets := newTestStores(t)

	var originHits atomic.Int64
	originData := []byte("origin payload")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits.Add(1)
		_, _ = w.Write(originData)
	}))
	t.Cleanup(origin.Close)

	server := newRemoteAssetServer(cas, assets, origin.Client())

	request := &remoteasset.FetchBlobRequest{
		InstanceName:   "instance",
		Uris:           []string{origin.URL},
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	first, err := server.FetchBlob(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, int32(codes.OK), first.GetStatus().GetCode())
	require.EqualValues(t, 1, originHits.Load())

	second, err := server.FetchBlob(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, int32(codes.OK), second.GetStatus().GetCode())
	require.EqualValues(t, 1, originHits.Load())
	require.Equal(t, first.GetBlobDigest().GetHash(), second.GetBlobDigest().GetHash())
}

func TestRemoteAssetPushBlobAndFetchWithExactQualifiers(t *testing.T) {
	cas, assets := newTestStores(t)

	var originHits atomic.Int64
	originData := []byte("origin payload")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits.Add(1)
		_, _ = w.Write(originData)
	}))
	t.Cleanup(origin.Close)

	server := newRemoteAssetServer(cas, assets, origin.Client())

	pushedData := []byte("pushed payload")
	pushedDigest := digestForData(pushedData)
	require.NoError(t, cas.UploadBytes(t.Context(), "instance", pushedDigest, pushedData))

	_, err := server.PushBlob(t.Context(), &remoteasset.PushBlobRequest{
		InstanceName:   "instance",
		Uris:           []string{origin.URL},
		Qualifiers:     []*remoteasset.Qualifier{{Name: "platform", Value: "linux"}},
		BlobDigest:     pushedDigest,
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.NoError(t, err)

	first, err := server.FetchBlob(t.Context(), &remoteasset.FetchBlobRequest{
		InstanceName:   "instance",
		Uris:           []string{origin.URL},
		Qualifiers:     []*remoteasset.Qualifier{{Name: "platform", Value: "linux"}},
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.NoError(t, err)
	require.Equal(t, pushedDigest.GetHash(), first.GetBlobDigest().GetHash())
	require.EqualValues(t, 0, originHits.Load())

	second, err := server.FetchBlob(t.Context(), &remoteasset.FetchBlobRequest{
		InstanceName:   "instance",
		Uris:           []string{origin.URL},
		Qualifiers:     []*remoteasset.Qualifier{{Name: "platform", Value: "darwin"}},
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	})
	require.NoError(t, err)
	require.Equal(t, int32(codes.OK), second.GetStatus().GetCode())
	require.EqualValues(t, 1, originHits.Load())
	require.NotEqual(t, pushedDigest.GetHash(), second.GetBlobDigest().GetHash())
}
