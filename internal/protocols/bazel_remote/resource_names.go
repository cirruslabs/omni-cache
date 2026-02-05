package bazel_remote

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
)

var errCompressedBlobsUnsupported = errors.New("compressed blobs are not supported")

type parsedBlobResource struct {
	instanceName string
	digest       *remoteexecution.Digest
	compressed   bool
}

func parseReadResourceName(resourceName string) (*parsedBlobResource, error) {
	segments := splitResourceName(resourceName)
	if len(segments) == 0 {
		return nil, fmt.Errorf("resource name is empty")
	}

	blobsIndex, compressed, err := locateBlobKind(segments)
	if err != nil {
		return nil, err
	}

	rest := segments[blobsIndex+1:]
	if compressed {
		return nil, errCompressedBlobsUnsupported
	}

	digest, err := parseResourceDigest(rest)
	if err != nil {
		return nil, err
	}

	return &parsedBlobResource{
		instanceName: strings.Join(segments[:blobsIndex], "/"),
		digest:       digest,
		compressed:   false,
	}, nil
}

func parseWriteResourceName(resourceName string) (*parsedBlobResource, error) {
	segments := splitResourceName(resourceName)
	if len(segments) < 4 {
		return nil, fmt.Errorf("invalid write resource name %q", resourceName)
	}

	uploadsIndex := -1
	for i, segment := range segments {
		if segment == "uploads" {
			uploadsIndex = i
			break
		}
	}
	if uploadsIndex == -1 {
		return nil, fmt.Errorf("invalid write resource name %q", resourceName)
	}

	afterUploads := segments[uploadsIndex+1:]
	if len(afterUploads) < 3 {
		return nil, fmt.Errorf("invalid write resource name %q", resourceName)
	}

	resourceKind := afterUploads[1]
	if resourceKind == "compressed-blobs" {
		return nil, errCompressedBlobsUnsupported
	}
	if resourceKind != "blobs" {
		return nil, fmt.Errorf("invalid write resource name %q", resourceName)
	}

	digest, err := parseResourceDigest(afterUploads[2:])
	if err != nil {
		return nil, err
	}

	return &parsedBlobResource{
		instanceName: strings.Join(segments[:uploadsIndex], "/"),
		digest:       digest,
		compressed:   false,
	}, nil
}

func splitResourceName(resourceName string) []string {
	trimmed := strings.Trim(strings.TrimSpace(resourceName), "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func locateBlobKind(segments []string) (index int, compressed bool, err error) {
	for i, segment := range segments {
		switch segment {
		case "blobs":
			return i, false, nil
		case "compressed-blobs":
			return i, true, nil
		}
	}
	return -1, false, fmt.Errorf("resource name does not reference blobs")
}

func parseResourceDigest(rest []string) (*remoteexecution.Digest, error) {
	if len(rest) < 2 {
		return nil, fmt.Errorf("resource name does not include digest")
	}

	hash := ""
	sizeToken := ""

	switch {
	case len(rest) >= 3 && rest[0] == "sha256":
		hash = rest[1]
		sizeToken = rest[2]
	case rest[0] == "sha256":
		return nil, fmt.Errorf("resource name does not include digest size")
	default:
		hash = rest[0]
		sizeToken = rest[1]
	}

	size, err := strconv.ParseInt(sizeToken, 10, 64)
	if err != nil || size < 0 {
		return nil, fmt.Errorf("invalid digest size %q", sizeToken)
	}

	digest, err := normalizeDigest(
		&remoteexecution.Digest{Hash: hash, SizeBytes: size},
		remoteexecution.DigestFunction_SHA256,
	)
	if err != nil {
		return nil, err
	}

	return digest, nil
}
