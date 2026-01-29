package gocacheprog

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/cirruslabs/omni-cache/internal/protocols/http_cache"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestHandlerPutGet(t *testing.T) {
	backend := testutil.NewStorage(t)
	socketDir := t.TempDir()
	socketPath := filepath.Join(socketDir, "omni-cache.sock")
	_ = os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	srv, err := server.Start(t.Context(), []net.Listener{listener}, backend, http_cache.Factory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	cacheClient, err := NewUnixSocketCacheClient(socketPath)
	require.NoError(t, err)

	handler, err := NewHandler(Config{
		CacheClient: cacheClient,
		CacheDir:    t.TempDir(),
		Strict:      true,
	})
	require.NoError(t, err)

	actionID := bytes.Repeat([]byte{0x01}, 32)
	body := []byte("hello cache")
	outputSum := sha256.Sum256(body)
	outputID := outputSum[:]

	var input bytes.Buffer
	bw := bufio.NewWriter(&input)
	writeRequest(t, bw, request{ID: 1, Command: cmdPut, ActionID: actionID, OutputID: outputID}, body)
	writeRequest(t, bw, request{ID: 2, Command: cmdGet, ActionID: actionID}, nil)
	writeRequest(t, bw, request{ID: 3, Command: cmdClose}, nil)
	require.NoError(t, bw.Flush())

	var output bytes.Buffer
	require.NoError(t, handler.Serve(context.Background(), &input, &output))

	dec := json.NewDecoder(&output)

	var res response
	require.NoError(t, dec.Decode(&res))
	require.Equal(t, int64(0), res.ID)
	require.ElementsMatch(t, []string{cmdGet, cmdPut, cmdClose}, res.KnownCommands)

	require.NoError(t, dec.Decode(&res))
	require.Equal(t, int64(1), res.ID)
	require.Empty(t, res.Err)
	require.NotEmpty(t, res.DiskPath)
	fileBytes, err := os.ReadFile(res.DiskPath)
	require.NoError(t, err)
	require.Equal(t, body, fileBytes)

	require.NoError(t, dec.Decode(&res))
	require.Equal(t, int64(2), res.ID)
	require.False(t, res.Miss)
	require.Equal(t, outputID, res.OutputID)
	require.NotEmpty(t, res.DiskPath)
	require.Equal(t, int64(len(body)), res.Size)

	require.NoError(t, dec.Decode(&res))
	require.Equal(t, int64(3), res.ID)

	info, err := backend.CacheInfo(context.Background(), cacheKey(hex.EncodeToString(actionID)), nil)
	require.NoError(t, err)
	require.Equal(t, cacheKey(hex.EncodeToString(actionID)), info.Key)
}

func writeRequest(t *testing.T, w *bufio.Writer, req request, body []byte) {
	t.Helper()
	if len(body) > 0 {
		req.BodySize = int64(len(body))
	}
	enc := json.NewEncoder(w)
	require.NoError(t, enc.Encode(req))
	require.NoError(t, w.WriteByte('\n'))

	if len(body) == 0 {
		return
	}

	require.NoError(t, w.WriteByte('"'))
	enc64 := base64.NewEncoder(base64.StdEncoding, w)
	_, err := enc64.Write(body)
	require.NoError(t, err)
	require.NoError(t, enc64.Close())
	_, err = w.WriteString("\"\n")
	require.NoError(t, err)
}
