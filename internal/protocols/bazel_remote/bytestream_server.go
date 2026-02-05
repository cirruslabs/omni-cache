package bazel_remote

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const bytestreamChunkSize = 64 * 1024

type byteStreamServer struct {
	bytestream.UnimplementedByteStreamServer
	store *casStore
}

func newByteStreamServer(store *casStore) *byteStreamServer {
	return &byteStreamServer{store: store}
}

func (s *byteStreamServer) Read(req *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	parsed, err := parseReadResourceName(req.GetResourceName())
	if err != nil {
		if errors.Is(err, errCompressedBlobsUnsupported) {
			return status.Error(codes.Unimplemented, err.Error())
		}
		return status.Errorf(codes.InvalidArgument, "invalid read resource name: %v", err)
	}

	data, err := s.store.DownloadBytes(stream.Context(), parsed.instanceName, parsed.digest)
	if err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			return status.Error(codes.NotFound, "blob not found")
		}
		return status.Errorf(codes.Internal, "download blob: %v", err)
	}

	offset := req.GetReadOffset()
	if offset < 0 {
		return status.Error(codes.InvalidArgument, "read_offset must be non-negative")
	}
	if offset > int64(len(data)) {
		return status.Error(codes.InvalidArgument, "read_offset is beyond blob size")
	}

	limit := req.GetReadLimit()
	end := int64(len(data))
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	for current := offset; current < end; current += bytestreamChunkSize {
		next := current + bytestreamChunkSize
		if next > end {
			next = end
		}

		if err := stream.Send(&bytestream.ReadResponse{Data: data[current:next]}); err != nil {
			return err
		}
	}

	return nil
}

func (s *byteStreamServer) Write(stream bytestream.ByteStream_WriteServer) error {
	first, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "empty write stream")
		}
		return err
	}

	resourceName := first.GetResourceName()
	if resourceName == "" {
		return status.Error(codes.InvalidArgument, "resource_name is required on first message")
	}

	parsed, err := parseWriteResourceName(resourceName)
	if err != nil {
		if errors.Is(err, errCompressedBlobsUnsupported) {
			return status.Error(codes.Unimplemented, err.Error())
		}
		return status.Errorf(codes.InvalidArgument, "invalid write resource name: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "omni-cache-bazel-upload-*")
	if err != nil {
		return status.Errorf(codes.Internal, "create temp file: %v", err)
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	hasher := sha256.New()
	written := int64(0)
	finished := false

	for current := first; ; {
		if rn := current.GetResourceName(); rn != "" && rn != resourceName {
			return status.Error(codes.InvalidArgument, "resource_name cannot change within a write stream")
		}
		if current.GetWriteOffset() != written {
			return status.Errorf(codes.InvalidArgument, "invalid write_offset %d, expected %d", current.GetWriteOffset(), written)
		}

		chunk := current.GetData()
		if len(chunk) > 0 {
			if _, err := tmpFile.Write(chunk); err != nil {
				return status.Errorf(codes.Internal, "write temp file: %v", err)
			}
			if _, err := hasher.Write(chunk); err != nil {
				return status.Errorf(codes.Internal, "hash chunk: %v", err)
			}
			written += int64(len(chunk))
		}

		if current.GetFinishWrite() {
			finished = true
			break
		}

		next, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		current = next
	}

	if !finished {
		return status.Error(codes.InvalidArgument, "finish_write was not set")
	}
	if written != parsed.digest.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "uploaded size %d does not match expected %d", written, parsed.digest.GetSizeBytes())
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	if sum != parsed.digest.GetHash() {
		return status.Error(codes.InvalidArgument, "uploaded digest does not match resource name digest")
	}

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return status.Errorf(codes.Internal, "seek temp file: %v", err)
	}
	if err := s.store.Upload(stream.Context(), parsed.instanceName, parsed.digest, tmpFile); err != nil {
		return status.Errorf(codes.Internal, "upload blob: %v", err)
	}

	return stream.SendAndClose(&bytestream.WriteResponse{CommittedSize: written})
}

func (s *byteStreamServer) QueryWriteStatus(ctx context.Context, req *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	parsed, err := parseWriteResourceName(req.GetResourceName())
	if err != nil {
		if errors.Is(err, errCompressedBlobsUnsupported) {
			return nil, status.Error(codes.Unimplemented, err.Error())
		}
		return nil, status.Errorf(codes.InvalidArgument, "invalid write resource name: %v", err)
	}

	exists, err := s.store.Exists(ctx, parsed.instanceName, parsed.digest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check blob existence: %v", err)
	}
	if !exists {
		return &bytestream.QueryWriteStatusResponse{CommittedSize: 0, Complete: false}, nil
	}

	return &bytestream.QueryWriteStatusResponse{CommittedSize: parsed.digest.GetSizeBytes(), Complete: true}, nil
}

var _ bytestream.ByteStreamServer = (*byteStreamServer)(nil)
