package stats

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
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

type TransferSnapshot struct {
	Count    int64
	Bytes    int64
	Duration time.Duration
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

func (c *Collector) Snapshot() Snapshot {
	return Snapshot{
		CacheHits:   c.cacheHits.Load(),
		CacheMisses: c.cacheMiss.Load(),
		Downloads:   c.downloads.snapshot(),
		Uploads:     c.uploads.snapshot(),
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
	rate := float64(totalBytes) / duration.Seconds()
	return fmt.Sprintf("%s/s", humanize.Bytes(uint64(rate)))
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
