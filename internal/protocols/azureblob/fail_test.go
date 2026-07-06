package azureblob

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFailWithoutSentryHubDoesNotPanic guards the regression where fail()
// dereferenced the Sentry hub unconditionally. When Sentry is not configured,
// sentry.GetHubFromContext returns nil, so the call panicked the handler
// goroutine and net/http closed the connection mid-response (the client saw a
// "socket hang up"). With no hub attached to the request context, fail() must
// skip Sentry reporting and still write the error response.
func TestFailWithoutSentryHubDoesNotPanic(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	require.NotPanics(t, func() {
		fail(recorder, request, http.StatusInternalServerError, "boom", "key", "value")
	})

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "boom")
}
