package bazel_remote

import (
	"testing"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestActionCacheStoreGetActionResultRecordsHitMiss(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	cas, _ := newTestStores(t)
	store := newActionCacheStore(cas.backend, cas.proxy)

	missingDigest := digestForData([]byte("missing"))
	missingResult, ok, err := store.GetActionResult(t.Context(), "instance", missingDigest)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, missingResult)

	snapshot := stats.Default().Snapshot()
	require.EqualValues(t, 0, snapshot.CacheHits)
	require.EqualValues(t, 1, snapshot.CacheMisses)

	stats.Default().Reset()

	actionDigest := digestForData([]byte("action"))
	expected := &remoteexecution.ActionResult{ExitCode: 0}
	require.NoError(t, store.PutActionResult(t.Context(), "instance", actionDigest, expected))

	result, ok, err := store.GetActionResult(t.Context(), "instance", actionDigest)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, proto.Equal(expected, result))

	snapshot = stats.Default().Snapshot()
	require.EqualValues(t, 1, snapshot.CacheHits)
	require.EqualValues(t, 0, snapshot.CacheMisses)
}

func TestActionCacheStoreGetActionResultMissWhenReferencedBlobMissing(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	cas, _ := newTestStores(t)
	store := newActionCacheStore(cas.backend, cas.proxy)

	actionDigest := digestForData([]byte("action"))
	resultWithMissingBlob := &remoteexecution.ActionResult{
		ExitCode:     0,
		StdoutDigest: digestForData([]byte("stdout")),
	}
	require.NoError(t, store.PutActionResult(t.Context(), "instance", actionDigest, resultWithMissingBlob))

	got, ok, err := store.GetActionResult(t.Context(), "instance", actionDigest)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, got)

	snapshot := stats.Default().Snapshot()
	require.EqualValues(t, 0, snapshot.CacheHits)
	require.EqualValues(t, 1, snapshot.CacheMisses)
}
