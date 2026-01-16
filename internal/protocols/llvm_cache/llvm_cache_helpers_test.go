package llvm_cache

import (
	"bytes"
	"encoding/base64"
	"os"
	"strings"
	"testing"

	casv1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/cas/v1"
	"github.com/stretchr/testify/require"
)

func TestParseCASID(t *testing.T) {
	digest := bytes.Repeat([]byte{0xAB}, casHashBytes)

	t.Run("raw-digest", func(t *testing.T) {
		parsed, id, err := parseCASID(digest)
		require.NoError(t, err)
		require.Equal(t, digest, parsed)
		require.Equal(t, casIDPrefix+strings.Repeat("ab", casHashBytes), id)
	})

	t.Run("prefixed-hex", func(t *testing.T) {
		rawID := casIDFromDigest(digest)
		parsed, id, err := parseCASID([]byte(rawID))
		require.NoError(t, err)
		require.Equal(t, digest, parsed)
		require.Equal(t, rawID, id)
	})

	t.Run("empty", func(t *testing.T) {
		_, _, err := parseCASID(nil)
		require.Error(t, err)
	})

	t.Run("invalid-length", func(t *testing.T) {
		_, _, err := parseCASID([]byte("llvmcas://abc"))
		require.Error(t, err)
	})

	t.Run("invalid-hex", func(t *testing.T) {
		_, _, err := parseCASID([]byte("llvmcas://" + strings.Repeat("zz", casHashBytes)))
		require.Error(t, err)
	})
}

func TestNormalizeRefs(t *testing.T) {
	digest := bytes.Repeat([]byte{0x10}, casHashBytes)
	rawRef := &casv1.CASDataID{Id: digest}
	hexRef := &casv1.CASDataID{Id: []byte(casIDFromDigest(digest))}

	digests, normalized, err := normalizeRefs([]*casv1.CASDataID{rawRef, hexRef})
	require.NoError(t, err)
	require.Len(t, digests, 2)
	require.Equal(t, digest, digests[0])
	require.Equal(t, digest, digests[1])

	require.Len(t, normalized, 2)
	for _, ref := range normalized {
		require.Equal(t, casIDFromDigest(digest), string(ref.GetId()))
	}

	_, _, err = normalizeRefs([]*casv1.CASDataID{nil})
	require.Error(t, err)
}

func TestHashObjectDeterminism(t *testing.T) {
	ref := bytes.Repeat([]byte{0x01}, casHashBytes)
	digest1, err := hashObject([][]byte{ref}, []byte("blob"))
	require.NoError(t, err)

	digest2, err := hashObject([][]byte{ref}, []byte("blob"))
	require.NoError(t, err)
	require.Equal(t, digest1, digest2)

	digest3, err := hashObject([][]byte{ref}, []byte("blob2"))
	require.NoError(t, err)
	require.NotEqual(t, digest1, digest3)

	_, err = hashObject([][]byte{[]byte("short")}, []byte("blob"))
	require.Error(t, err)
}

func TestCASBlobData(t *testing.T) {
	t.Run("inline-data", func(t *testing.T) {
		blob := &casv1.CASBytes{Contents: &casv1.CASBytes_Data{Data: []byte("data")}}
		data, err := casBlobData(blob)
		require.NoError(t, err)
		require.Equal(t, []byte("data"), data)
	})

	t.Run("file-path", func(t *testing.T) {
		path := writeTempFile(t, []byte("blob"))
		blob := &casv1.CASBytes{Contents: &casv1.CASBytes_FilePath{FilePath: path}}
		data, err := casBlobData(blob)
		require.NoError(t, err)
		require.Equal(t, []byte("blob"), data)
	})

	t.Run("empty", func(t *testing.T) {
		_, err := casBlobData(nil)
		require.Error(t, err)
	})
}

func TestCASBytesForResponse(t *testing.T) {
	data := []byte("payload")

	inline, err := casBytesForResponse(data, false)
	require.NoError(t, err)
	require.Equal(t, data, inline.GetData())

	onDisk, err := casBytesForResponse(data, true)
	require.NoError(t, err)
	path := onDisk.GetFilePath()
	require.NotEmpty(t, path)

	read, err := casBlobData(onDisk)
	require.NoError(t, err)
	require.Equal(t, data, read)
}

func TestKVStorageKey(t *testing.T) {
	key := []byte("key")
	expected := kvPrefix + base64.RawURLEncoding.EncodeToString(key)
	require.Equal(t, expected, kvStorageKey(key))
}

func writeTempFile(t *testing.T, data []byte) string {
	t.Helper()

	path, err := writeTempBlob(data)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.Remove(path)
	})
	return path
}
