package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cirruslabs/omni-cache/pkg/stats"
	"github.com/stretchr/testify/require"
)

func TestStatsHandlerGithubActionsNoActivity(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Accept", "text/vnd.github-actions")
	recorder := httptest.NewRecorder()

	writeStatsResponse(recorder, req)

	require.Equal(t, http.StatusNoContent, recorder.Code)
	require.Empty(t, recorder.Body.String())
}

func TestStatsHandlerGithubActionsWithActivity(t *testing.T) {
	stats.Default().Reset()
	t.Cleanup(func() {
		stats.Default().Reset()
	})

	stats.Default().RecordCacheHit()
	snapshot := stats.Default().Snapshot()

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Accept", "text/vnd.github-actions")
	recorder := httptest.NewRecorder()

	writeStatsResponse(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, stats.FormatGithubActionsSummary(snapshot), recorder.Body.String())
}
