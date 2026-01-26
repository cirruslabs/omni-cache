package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCollectorReset(t *testing.T) {
	collector := &Collector{}
	collector.RecordCacheHit()
	collector.RecordCacheMiss()
	collector.RecordDownload(128, 2*time.Second)
	collector.RecordUpload(64, 500*time.Millisecond)

	collector.Reset()

	snapshot := collector.Snapshot()
	require.Equal(t, int64(0), snapshot.CacheHits)
	require.Equal(t, int64(0), snapshot.CacheMisses)
	require.Equal(t, int64(0), snapshot.Downloads.Count)
	require.Equal(t, int64(0), snapshot.Downloads.Bytes)
	require.Equal(t, time.Duration(0), snapshot.Downloads.Duration)
	require.Equal(t, int64(0), snapshot.Uploads.Count)
	require.Equal(t, int64(0), snapshot.Uploads.Bytes)
	require.Equal(t, time.Duration(0), snapshot.Uploads.Duration)
}
