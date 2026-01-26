package stats

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

const skipHitMissQueryParam = "omni_cache_skip_hit_miss"

type Collector struct {
	cacheHits atomic.Int64
	cacheMiss atomic.Int64
	downloads transferCounter
	uploads   transferCounter
}

type transferCounter struct {
	count    atomic.Int64
	bytes    atomic.Int64
	duration atomic.Int64
}

type Snapshot struct {
	CacheHits   int64
	CacheMisses int64
	Downloads   TransferSnapshot
	Uploads     TransferSnapshot
}

type Summary struct {
	CacheHits           int64           `json:"cache_hits"`
	CacheMisses         int64           `json:"cache_misses"`
	CacheHitRatePercent float64         `json:"cache_hit_rate_percent"`
	Downloads           TransferSummary `json:"downloads"`
	Uploads             TransferSummary `json:"uploads"`
}

type TransferSnapshot struct {
	Count    int64
	Bytes    int64
	Duration time.Duration
}

type TransferSummary struct {
	Count         int64   `json:"count"`
	Bytes         int64   `json:"bytes"`
	DurationMs    int64   `json:"duration_ms"`
	AvgBytes      int64   `json:"avg_bytes"`
	AvgDurationMs int64   `json:"avg_duration_ms"`
	BytesPerSec   float64 `json:"bytes_per_sec"`
}

var defaultCollector Collector

func Default() *Collector {
	return &defaultCollector
}

func (c *Collector) RecordCacheHit() {
	c.cacheHits.Add(1)
}

func (c *Collector) RecordCacheMiss() {
	c.cacheMiss.Add(1)
}

func (c *Collector) RecordDownload(bytes int64, duration time.Duration) {
	c.downloads.record(bytes, duration)
}

func (c *Collector) RecordUpload(bytes int64, duration time.Duration) {
	c.uploads.record(bytes, duration)
}

func (c *Collector) Reset() {
	c.cacheHits.Store(0)
	c.cacheMiss.Store(0)
	c.downloads.reset()
	c.uploads.reset()
}

func (c *Collector) Snapshot() Snapshot {
	return Snapshot{
		CacheHits:   c.cacheHits.Load(),
		CacheMisses: c.cacheMiss.Load(),
		Downloads:   c.downloads.snapshot(),
		Uploads:     c.uploads.snapshot(),
	}
}

func (c *Collector) Summary() Summary {
	snapshot := c.Snapshot()
	totalLookups := snapshot.CacheHits + snapshot.CacheMisses

	var hitRate float64
	if totalLookups > 0 {
		hitRate = (float64(snapshot.CacheHits) / float64(totalLookups)) * 100
	}

	return Summary{
		CacheHits:           snapshot.CacheHits,
		CacheMisses:         snapshot.CacheMisses,
		CacheHitRatePercent: hitRate,
		Downloads:           summarizeTransfer(snapshot.Downloads),
		Uploads:             summarizeTransfer(snapshot.Uploads),
	}
}

func (c *Collector) LogSummary() {
	snapshot := c.Snapshot()
	totalLookups := snapshot.CacheHits + snapshot.CacheMisses

	slog.Info(
		"omni-cache stats",
		"cacheHits", snapshot.CacheHits,
		"cacheMisses", snapshot.CacheMisses,
		"cacheHitRate", formatPercent(snapshot.CacheHits, totalLookups),
		"downloads", formatTransferSummary(snapshot.Downloads),
		"uploads", formatTransferSummary(snapshot.Uploads),
	)
}

func (c *Collector) SummaryText() string {
	snapshot := c.Snapshot()
	totalLookups := snapshot.CacheHits + snapshot.CacheMisses

	var builder strings.Builder
	builder.WriteString("omni-cache stats\n")
	fmt.Fprintf(&builder, "cache hits: %d\n", snapshot.CacheHits)
	fmt.Fprintf(&builder, "cache misses: %d\n", snapshot.CacheMisses)
	fmt.Fprintf(&builder, "cache hit rate: %s\n", formatPercent(snapshot.CacheHits, totalLookups))
	fmt.Fprintf(&builder, "downloads: %s\n", formatTransferSummary(snapshot.Downloads))
	fmt.Fprintf(&builder, "uploads: %s\n", formatTransferSummary(snapshot.Uploads))
	return builder.String()
}

func (c *transferCounter) record(bytes int64, duration time.Duration) {
	if bytes < 0 {
		bytes = 0
	}
	if duration < 0 {
		duration = 0
	}
	c.count.Add(1)
	c.bytes.Add(bytes)
	c.duration.Add(duration.Nanoseconds())
}

func (c *transferCounter) snapshot() TransferSnapshot {
	return TransferSnapshot{
		Count:    c.count.Load(),
		Bytes:    c.bytes.Load(),
		Duration: time.Duration(c.duration.Load()),
	}
}

func (c *transferCounter) reset() {
	c.count.Store(0)
	c.bytes.Store(0)
	c.duration.Store(0)
}

func summarizeTransfer(snapshot TransferSnapshot) TransferSummary {
	if snapshot.Count == 0 {
		return TransferSummary{}
	}

	avgBytes := snapshot.Bytes / snapshot.Count
	avgDuration := time.Duration(snapshot.Duration.Nanoseconds() / snapshot.Count)

	return TransferSummary{
		Count:         snapshot.Count,
		Bytes:         snapshot.Bytes,
		DurationMs:    snapshot.Duration.Milliseconds(),
		AvgBytes:      avgBytes,
		AvgDurationMs: avgDuration.Milliseconds(),
		BytesPerSec:   rateBytes(snapshot.Bytes, snapshot.Duration),
	}
}

func formatTransferSummary(snapshot TransferSnapshot) string {
	if snapshot.Count == 0 {
		return "none"
	}

	avgBytes := snapshot.Bytes / snapshot.Count
	avgDuration := time.Duration(snapshot.Duration.Nanoseconds() / snapshot.Count)

	return fmt.Sprintf(
		"count=%d total=%s avg=%s avgTime=%s avgSpeed=%s",
		snapshot.Count,
		humanize.IBytes(uint64(snapshot.Bytes)),
		humanize.IBytes(uint64(avgBytes)),
		formatDuration(avgDuration),
		formatRate(snapshot.Bytes, snapshot.Duration),
	)
}

func formatRate(totalBytes int64, duration time.Duration) string {
	if totalBytes <= 0 || duration <= 0 {
		return "0 B/s"
	}
	return fmt.Sprintf("%s/s", humanize.Bytes(uint64(rateBytes(totalBytes, duration))))
}

func formatDuration(duration time.Duration) string {
	if duration <= 0 {
		return "0s"
	}
	return duration.Round(time.Millisecond).String()
}

func formatPercent(numerator, denominator int64) string {
	if denominator <= 0 {
		return "0%"
	}
	value := (float64(numerator) / float64(denominator)) * 100
	return fmt.Sprintf("%.1f%%", value)
}

func rateBytes(totalBytes int64, duration time.Duration) float64 {
	if totalBytes <= 0 || duration <= 0 {
		return 0
	}
	return float64(totalBytes) / duration.Seconds()
}

func AddSkipHitMissQuery(target *url.URL) {
	if target == nil {
		return
	}
	values := target.Query()
	values.Set(skipHitMissQueryParam, "1")
	target.RawQuery = values.Encode()
}

func ShouldSkipHitMiss(request *http.Request) bool {
	if request == nil || request.URL == nil {
		return false
	}
	return request.URL.Query().Get(skipHitMissQueryParam) == "1"
}
