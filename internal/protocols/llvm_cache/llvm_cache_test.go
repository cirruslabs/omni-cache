package llvm_cache_test

import (
	"context"
	"encoding/hex"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	casv1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/cas/v1"
	keyvaluev1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/keyvalue/v1"
	llvmcache "github.com/cirruslabs/omni-cache/internal/protocols/llvm_cache"
	"github.com/cirruslabs/omni-cache/internal/testutil"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	casIDPrefix  = "llvmcas://"
	casHashBytes = 32
)

func setupGRPCConn(t *testing.T) *grpc.ClientConn {
	t.Helper()

	storage := testutil.NewStorage(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := server.Start(t.Context(), []net.Listener{listener}, storage, llvmcache.Factory{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}

func TestLLVMCacheKeyValueRoundTrip(t *testing.T) {
	conn := setupGRPCConn(t)
	client := keyvaluev1.NewKeyValueDBClient(conn)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	key := []byte("cache-key")
	missingResp, err := client.GetValue(ctx, &keyvaluev1.GetValueRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, keyvaluev1.GetValueResponse_KEY_NOT_FOUND, missingResp.GetOutcome())

	putResp, err := client.PutValue(ctx, &keyvaluev1.PutValueRequest{
		Key: key,
		Value: &keyvaluev1.Value{
			Entries: map[string][]byte{
				"foo": []byte("bar"),
				"baz": []byte("qux"),
			},
		},
	})
	require.NoError(t, err)
	require.Nil(t, putResp.GetError())

	getResp, err := client.GetValue(ctx, &keyvaluev1.GetValueRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, keyvaluev1.GetValueResponse_SUCCESS, getResp.GetOutcome())
	require.Equal(t, []byte("bar"), getResp.GetValue().GetEntries()["foo"])
	require.Equal(t, []byte("qux"), getResp.GetValue().GetEntries()["baz"])
}

func TestLLVMCacheCASRoundTrip(t *testing.T) {
	conn := setupGRPCConn(t)
	client := casv1.NewCASDBServiceClient(conn)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	missingDigest := make([]byte, casHashBytes)
	missingResp, err := client.Get(ctx, &casv1.CASGetRequest{
		CasId: &casv1.CASDataID{Id: missingDigest},
	})
	require.NoError(t, err)
	require.Equal(t, casv1.CASGetResponse_OBJECT_NOT_FOUND, missingResp.GetOutcome())

	saveResp, err := client.Save(ctx, &casv1.CASSaveRequest{
		Data: &casv1.CASBlob{
			Blob: casBytesData([]byte("blob")),
		},
	})
	require.NoError(t, err)

	savedID := string(saveResp.GetCasId().GetId())
	require.True(t, strings.HasPrefix(savedID, casIDPrefix))

	loadResp, err := client.Load(ctx, &casv1.CASLoadRequest{
		CasId: &casv1.CASDataID{Id: saveResp.GetCasId().GetId()},
	})
	require.NoError(t, err)
	require.Equal(t, casv1.CASLoadResponse_SUCCESS, loadResp.GetOutcome())
	require.Equal(t, []byte("blob"), loadResp.GetData().GetBlob().GetData())

	loadDiskResp, err := client.Load(ctx, &casv1.CASLoadRequest{
		CasId:       &casv1.CASDataID{Id: saveResp.GetCasId().GetId()},
		WriteToDisk: true,
	})
	require.NoError(t, err)
	require.Equal(t, casv1.CASLoadResponse_SUCCESS, loadDiskResp.GetOutcome())
	loadPath := loadDiskResp.GetData().GetBlob().GetFilePath()
	require.NotEmpty(t, loadPath)
	data, err := os.ReadFile(loadPath)
	require.NoError(t, err)
	require.Equal(t, []byte("blob"), data)
	require.NoError(t, os.Remove(loadPath))

	getResp, err := client.Get(ctx, &casv1.CASGetRequest{
		CasId: &casv1.CASDataID{Id: saveResp.GetCasId().GetId()},
	})
	require.NoError(t, err)
	require.Equal(t, casv1.CASGetResponse_SUCCESS, getResp.GetOutcome())
	require.Equal(t, []byte("blob"), getResp.GetData().GetBlob().GetData())
	require.Empty(t, getResp.GetData().GetReferences())

	refDigest := digestFromCASID(t, savedID)
	putResp, err := client.Put(ctx, &casv1.CASPutRequest{
		Data: &casv1.CASObject{
			Blob:       casBytesData([]byte("object")),
			References: []*casv1.CASDataID{{Id: refDigest}},
		},
	})
	require.NoError(t, err)
	putID := string(putResp.GetCasId().GetId())
	require.True(t, strings.HasPrefix(putID, casIDPrefix))

	getObjResp, err := client.Get(ctx, &casv1.CASGetRequest{
		CasId: &casv1.CASDataID{Id: putResp.GetCasId().GetId()},
	})
	require.NoError(t, err)
	require.Equal(t, casv1.CASGetResponse_SUCCESS, getObjResp.GetOutcome())
	require.Equal(t, []byte("object"), getObjResp.GetData().GetBlob().GetData())
	require.Len(t, getObjResp.GetData().GetReferences(), 1)
	require.Equal(t, savedID, string(getObjResp.GetData().GetReferences()[0].GetId()))
}

func casBytesData(data []byte) *casv1.CASBytes {
	return &casv1.CASBytes{Contents: &casv1.CASBytes_Data{Data: data}}
}

func digestFromCASID(t *testing.T, casID string) []byte {
	t.Helper()
	require.True(t, strings.HasPrefix(casID, casIDPrefix))
	value := strings.TrimPrefix(casID, casIDPrefix)
	data, err := hex.DecodeString(value)
	require.NoError(t, err)
	require.Len(t, data, casHashBytes)
	return data
}
