package bazel_remote_asset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	bytestream "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type byteStreamService struct {
	bytestream.UnimplementedByteStreamServer
	cas *casStore
}

func newByteStreamService(cas *casStore) *byteStreamService {
	return &byteStreamService{cas: cas}
}

func (s *byteStreamService) Read(req *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	resourceName := req.GetResourceName()
	if resourceName == "" {
		return status.Error(codes.InvalidArgument, "resource name is required")
	}

	resource, err := parseBlobResourceName(resourceName)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if req.GetReadOffset() < 0 {
		return status.Error(codes.OutOfRange, "read_offset must be non-negative")
	}
	if req.GetReadLimit() < 0 {
		return status.Error(codes.OutOfRange, "read_limit must be non-negative")
	}
	if resource.size >= 0 && req.GetReadOffset() > resource.size {
		return status.Error(codes.OutOfRange, "read_offset exceeds resource size")
	}

	writer := &bytestreamWriter{stream: stream}
	if err := s.cas.stream(stream.Context(), assetDigest{Hash: resource.hash, SizeBytes: resource.size}, req.GetReadOffset(), req.GetReadLimit(), writer); err != nil {
		if err == io.EOF || err == errLimitReached {
			return nil
		}
		if errors.Is(err, storage.ErrCacheNotFound) {
			return status.Error(codes.NotFound, "blob not found")
		}
		return status.Errorf(codes.Internal, "read failed: %v", err)
	}
	return nil
}

func (s *byteStreamService) Write(stream bytestream.ByteStream_WriteServer) error {
	ctx := stream.Context()

	tmp, err := os.CreateTemp("", "omni-cache-bytestream-*")
	if err != nil {
		return status.Errorf(codes.Internal, "create temp file: %v", err)
	}
	defer func() {
		_ = os.Remove(tmp.Name())
	}()
	defer tmp.Close()

	hasher := sha256.New()
	var (
		resource     *blobResource
		totalWritten int64
	)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if resource == nil {
			if req.GetResourceName() == "" {
				return status.Error(codes.InvalidArgument, "resource name is required")
			}
			parsed, parseErr := parseBlobResourceName(req.GetResourceName())
			if parseErr != nil {
				return status.Error(codes.InvalidArgument, parseErr.Error())
			}
			resource = &parsed
		}

		if req.GetWriteOffset() != totalWritten {
			return status.Errorf(codes.InvalidArgument, "unexpected write offset %d (expected %d)", req.GetWriteOffset(), totalWritten)
		}

		data := req.GetData()
		if len(data) > 0 {
			n, err := tmp.Write(data)
			if err != nil {
				return status.Errorf(codes.Internal, "write temp file: %v", err)
			}
			if _, err := hasher.Write(data[:n]); err != nil {
				return status.Errorf(codes.Internal, "hash data: %v", err)
			}
			totalWritten += int64(n)
		}

		if req.GetFinishWrite() {
			break
		}
	}

	if resource == nil {
		return status.Error(codes.InvalidArgument, "resource name is required")
	}

	computedHash := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(computedHash, resource.hash) {
		return status.Errorf(codes.InvalidArgument, "digest mismatch: expected %s, got %s", resource.hash, computedHash)
	}
	if resource.size >= 0 && totalWritten != resource.size {
		return status.Errorf(codes.InvalidArgument, "size mismatch: expected %d, got %d", resource.size, totalWritten)
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return status.Errorf(codes.Internal, "rewind temp file: %v", err)
	}

	if err := s.cas.save(ctx, assetDigest{Hash: computedHash, SizeBytes: totalWritten}, tmp, totalWritten); err != nil {
		return status.Errorf(codes.Internal, "store blob: %v", err)
	}

	return stream.SendAndClose(&bytestream.WriteResponse{CommittedSize: totalWritten})
}

func (s *byteStreamService) QueryWriteStatus(ctx context.Context, req *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	resourceName := req.GetResourceName()
	if resourceName == "" {
		return nil, status.Error(codes.InvalidArgument, "resource name is required")
	}

	resource, err := parseBlobResourceName(resourceName)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	exists, err := s.cas.has(ctx, assetDigest{Hash: resource.hash, SizeBytes: resource.size})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query blob: %v", err)
	}

	if !exists {
		return &bytestream.QueryWriteStatusResponse{CommittedSize: 0, Complete: false}, nil
	}

	return &bytestream.QueryWriteStatusResponse{CommittedSize: resource.size, Complete: true}, nil
}

type bytestreamWriter struct {
	stream bytestream.ByteStream_ReadServer
}

func (w *bytestreamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	data := append([]byte(nil), p...)
	if err := w.stream.Send(&bytestream.ReadResponse{Data: data}); err != nil {
		return 0, err
	}
	return len(p), nil
}

type blobResource struct {
	hash string
	size int64
}

func parseBlobResourceName(name string) (blobResource, error) {
	if name == "" {
		return blobResource{}, fmt.Errorf("resource name is empty")
	}

	if strings.HasPrefix(name, "blobs/") {
		return parseBlobParts(strings.Split(name[len("blobs/"):], "/"))
	}
	if strings.HasPrefix(name, "compressed-blobs/") {
		return parseCompressedParts(strings.Split(name[len("compressed-blobs/"):], "/"))
	}

	if idx := strings.LastIndex(name, "/blobs/"); idx != -1 {
		return parseBlobParts(strings.Split(name[idx+len("/blobs/"):], "/"))
	}
	if idx := strings.LastIndex(name, "/compressed-blobs/"); idx != -1 {
		return parseCompressedParts(strings.Split(name[idx+len("/compressed-blobs/"):], "/"))
	}

	return blobResource{}, fmt.Errorf("unsupported resource name %q", name)
}

func parseBlobParts(parts []string) (blobResource, error) {
	if len(parts) != 2 {
		return blobResource{}, fmt.Errorf("invalid blob resource name")
	}
	size, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return blobResource{}, fmt.Errorf("invalid blob size")
	}
	return blobResource{hash: parts[0], size: size}, nil
}

func parseCompressedParts(parts []string) (blobResource, error) {
	if len(parts) != 3 {
		return blobResource{}, fmt.Errorf("invalid compressed blob resource name")
	}
	if parts[0] != "identity" {
		return blobResource{}, fmt.Errorf("unsupported compressor %q", parts[0])
	}
	size, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return blobResource{}, fmt.Errorf("invalid blob size")
	}
	return blobResource{hash: parts[1], size: size}, nil
}
