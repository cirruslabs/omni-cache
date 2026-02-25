package bazel_remote

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapabilitiesEnableActionCacheUpdates(t *testing.T) {
	response, err := newCapabilitiesServer().GetCapabilities(t.Context(), nil)
	require.NoError(t, err)
	require.NotNil(t, response.GetCacheCapabilities())
	require.NotNil(t, response.GetCacheCapabilities().GetActionCacheUpdateCapabilities())
	require.True(t, response.GetCacheCapabilities().GetActionCacheUpdateCapabilities().GetUpdateEnabled())
}
