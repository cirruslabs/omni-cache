package bazel_remote

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	remoteexecution "github.com/cirruslabs/omni-cache/internal/api/build/bazel/remote/execution/v2"
)

const (
	sha256HexLen    = 64
	emptySHA256Hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func normalizeDigestFunction(value remoteexecution.DigestFunction_Value, hash string) (remoteexecution.DigestFunction_Value, error) {
	switch value {
	case remoteexecution.DigestFunction_UNKNOWN, remoteexecution.DigestFunction_SHA256:
		// Supported.
	default:
		return 0, fmt.Errorf("unsupported digest function %s", value.String())
	}

	if hash != "" && len(hash) != sha256HexLen {
		return 0, fmt.Errorf("unsupported hash length %d; only SHA256 is supported", len(hash))
	}

	return remoteexecution.DigestFunction_SHA256, nil
}

func normalizeDigest(digest *remoteexecution.Digest, value remoteexecution.DigestFunction_Value) (*remoteexecution.Digest, error) {
	if digest == nil {
		return nil, fmt.Errorf("missing digest")
	}
	if digest.GetSizeBytes() < 0 {
		return nil, fmt.Errorf("digest size must be non-negative")
	}

	hash := strings.ToLower(strings.TrimSpace(digest.GetHash()))
	if hash == "" {
		return nil, fmt.Errorf("digest hash is empty")
	}
	if len(hash) != sha256HexLen {
		return nil, fmt.Errorf("unsupported hash length %d; only SHA256 is supported", len(hash))
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return nil, fmt.Errorf("digest hash must be lower-case hex: %w", err)
	}

	if _, err := normalizeDigestFunction(value, hash); err != nil {
		return nil, err
	}

	return &remoteexecution.Digest{
		Hash:      hash,
		SizeBytes: digest.GetSizeBytes(),
	}, nil
}

func digestForData(data []byte) *remoteexecution.Digest {
	sum := sha256.Sum256(data)
	return &remoteexecution.Digest{
		Hash:      hex.EncodeToString(sum[:]),
		SizeBytes: int64(len(data)),
	}
}

func digestMatchesData(digest *remoteexecution.Digest, data []byte) bool {
	normalized, err := normalizeDigest(digest, remoteexecution.DigestFunction_SHA256)
	if err != nil {
		return false
	}

	computed := digestForData(data)
	return normalized.Hash == computed.Hash && normalized.SizeBytes == computed.SizeBytes
}

func isEmptyDigest(digest *remoteexecution.Digest) bool {
	if digest == nil {
		return false
	}
	return digest.GetSizeBytes() == 0 && strings.EqualFold(digest.GetHash(), emptySHA256Hash)
}

func cloneDigest(digest *remoteexecution.Digest) *remoteexecution.Digest {
	if digest == nil {
		return nil
	}
	return &remoteexecution.Digest{
		Hash:      digest.GetHash(),
		SizeBytes: digest.GetSizeBytes(),
	}
}
