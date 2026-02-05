package bazel_remote

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestByteStreamWriteReadRoundTrip(t *testing.T) {
	cas, _ := newTestStores(t)
	conn := newGRPCConn(t, func(server *grpc.Server) {
		bytestream.RegisterByteStreamServer(server, newByteStreamServer(cas))
	})

	client := bytestream.NewByteStreamClient(conn)
	ctx := context.Background()

	data := []byte("hello bytestream")
	digest := digestForData(data)
	resourceName := fmt.Sprintf("instance/uploads/u-1/blobs/%s/%d", digest.GetHash(), digest.GetSizeBytes())

	writeStream, err := client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, writeStream.Send(&bytestream.WriteRequest{ResourceName: resourceName, WriteOffset: 0, Data: data, FinishWrite: true}))
	writeResponse, err := writeStream.CloseAndRecv()
	require.NoError(t, err)
	require.EqualValues(t, len(data), writeResponse.GetCommittedSize())

	statusResponse, err := client.QueryWriteStatus(ctx, &bytestream.QueryWriteStatusRequest{ResourceName: resourceName})
	require.NoError(t, err)
	require.True(t, statusResponse.GetComplete())
	require.EqualValues(t, len(data), statusResponse.GetCommittedSize())

	readResource := fmt.Sprintf("instance/blobs/%s/%d", digest.GetHash(), digest.GetSizeBytes())
	readStream, err := client.Read(ctx, &bytestream.ReadRequest{ResourceName: readResource})
	require.NoError(t, err)

	var downloaded []byte
	for {
		msg, err := readStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		downloaded = append(downloaded, msg.GetData()...)
	}
	require.Equal(t, data, downloaded)
}

func TestByteStreamWriteRejectsNonSequentialOffsets(t *testing.T) {
	cas, _ := newTestStores(t)
	conn := newGRPCConn(t, func(server *grpc.Server) {
		bytestream.RegisterByteStreamServer(server, newByteStreamServer(cas))
	})

	client := bytestream.NewByteStreamClient(conn)
	ctx := context.Background()

	data := []byte("abcdef")
	digest := digestForData(data)
	resourceName := fmt.Sprintf("instance/uploads/u-2/blobs/%s/%d", digest.GetHash(), digest.GetSizeBytes())

	writeStream, err := client.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, writeStream.Send(&bytestream.WriteRequest{ResourceName: resourceName, WriteOffset: 0, Data: data[:3]}))
	require.NoError(t, writeStream.Send(&bytestream.WriteRequest{ResourceName: resourceName, WriteOffset: 0, Data: data[3:], FinishWrite: true}))

	_, err = writeStream.CloseAndRecv()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}
