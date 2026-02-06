package bazel_remote

import (
	"testing"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/require"
)

func TestNormalizeDigestRejectsUnsupportedFunction(t *testing.T) {
	_, err := normalizeDigest(&remoteexecution.Digest{Hash: emptySHA256Hash, SizeBytes: 0}, remoteexecution.DigestFunction_SHA1)
	require.Error(t, err)
}

func TestDigestMatchesData(t *testing.T) {
	data := []byte("hello")
	digest := digestForData(data)
	require.True(t, digestMatchesData(digest, data))
	require.False(t, digestMatchesData(digest, []byte("world")))
}
