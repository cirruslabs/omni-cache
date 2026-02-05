package bazel_remote

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseReadResourceName(t *testing.T) {
	resource := "instance/path/blobs/" + emptySHA256Hash + "/0"
	parsed, err := parseReadResourceName(resource)
	require.NoError(t, err)
	require.Equal(t, "instance/path", parsed.instanceName)
	require.Equal(t, emptySHA256Hash, parsed.digest.GetHash())
	require.EqualValues(t, 0, parsed.digest.GetSizeBytes())
}

func TestParseWriteResourceNameRejectsCompressed(t *testing.T) {
	_, err := parseWriteResourceName("instance/uploads/u/compressed-blobs/zstd/" + emptySHA256Hash + "/0")
	require.ErrorIs(t, err, errCompressedBlobsUnsupported)
}
