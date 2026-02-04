package stats

import (
	"fmt"
	"strings"
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

func TestSnapshotHasActivity(t *testing.T) {
	require.False(t, Snapshot{}.HasActivity())
	require.True(t, Snapshot{CacheHits: 1}.HasActivity())
	require.True(t, Snapshot{CacheMisses: 1}.HasActivity())
	require.True(t, Snapshot{Downloads: TransferSnapshot{Count: 1}}.HasActivity())
	require.True(t, Snapshot{Uploads: TransferSnapshot{Count: 1}}.HasActivity())
}

func TestFormatGithubActionsSummary(t *testing.T) {
	snapshot := Snapshot{
		CacheHits:   2,
		CacheMisses: 1,
		Downloads: TransferSnapshot{
			Count:    1,
			Bytes:    1024,
			Duration: time.Second,
		},
		Uploads: TransferSnapshot{
			Count:    2,
			Bytes:    2048,
			Duration: 2 * time.Second,
		},
	}

	expectedMessage := escapeGithubActionsMessage(strings.Join([]string{
		fmt.Sprintf("Cache hits: %d", snapshot.CacheHits),
		fmt.Sprintf("Cache misses: %d", snapshot.CacheMisses),
		fmt.Sprintf("Hit rate: %s", formatPercent(snapshot.CacheHits, snapshot.CacheHits+snapshot.CacheMisses)),
		fmt.Sprintf("Downloads: %s", formatTransferSummary(snapshot.Downloads)),
		fmt.Sprintf("Uploads: %s", formatTransferSummary(snapshot.Uploads)),
	}, "\n"))
	expected := fmt.Sprintf("::notice title=Omni Cache::%s", expectedMessage)

	require.Equal(t, expected, FormatGithubActionsSummary(snapshot))
}
