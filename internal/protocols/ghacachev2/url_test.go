package ghacachev2

import (
	"net/http/httptest"
	"testing"

	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/stretchr/testify/require"
)

func TestAzureBlobURLSkipHitMiss(t *testing.T) {
	cache := &Cache{cacheHost: "cache.local"}

	downloadURL := cache.azureBlobURL("v-key", true)
	downloadReq := httptest.NewRequest("GET", downloadURL, nil)
	require.True(t, stats.ShouldSkipHitMiss(downloadReq))

	uploadURL := cache.azureBlobURL("v-key", false)
	uploadReq := httptest.NewRequest("GET", uploadURL, nil)
	require.False(t, stats.ShouldSkipHitMiss(uploadReq))
}
