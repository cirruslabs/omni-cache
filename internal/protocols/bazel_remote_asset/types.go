package bazel_remote_asset

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type assetKind string

const (
	assetKindBlob      assetKind = "blob"
	assetKindDirectory assetKind = "directory"

	assetPrefix = "bazel-remote-asset"
)

type assetRecord struct {
	Kind         assetKind        `json:"kind"`
	InstanceName string           `json:"instance_name,omitempty"`
	URI          string           `json:"uri,omitempty"`
	Qualifiers   []assetQualifier `json:"qualifiers,omitempty"`
	Digest       assetDigest      `json:"digest"`
	DigestFunc   int32            `json:"digest_function"`
	ExpiresAt    *time.Time       `json:"expires_at,omitempty"`
	PushedAt     time.Time        `json:"pushed_at"`
}

type assetQualifier struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type assetDigest struct {
	Hash      string `json:"hash"`
	SizeBytes int64  `json:"size_bytes"`
}

func normalizeQualifiers(qualifiers []*remoteasset.Qualifier) ([]assetQualifier, error) {
	if len(qualifiers) == 0 {
		return nil, nil
	}

	normalized := make([]assetQualifier, 0, len(qualifiers))
	seen := make(map[string]struct{}, len(qualifiers))

	for _, qualifier := range qualifiers {
		if qualifier == nil {
			return nil, fmt.Errorf("qualifier is nil")
		}

		name := strings.TrimSpace(qualifier.GetName())
		if name == "" {
			return nil, fmt.Errorf("qualifier name is required")
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("duplicate qualifier name %q", name)
		}

		seen[name] = struct{}{}
		normalized = append(normalized, assetQualifier{
			Name:  name,
			Value: qualifier.GetValue(),
		})
	}

	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Name == normalized[j].Name {
			return normalized[i].Value < normalized[j].Value
		}
		return normalized[i].Name < normalized[j].Name
	})

	return normalized, nil
}

func qualifiersToProto(qualifiers []assetQualifier) []*remoteasset.Qualifier {
	if len(qualifiers) == 0 {
		return nil
	}

	result := make([]*remoteasset.Qualifier, 0, len(qualifiers))
	for _, qualifier := range qualifiers {
		result = append(result, &remoteasset.Qualifier{
			Name:  qualifier.Name,
			Value: qualifier.Value,
		})
	}
	return result
}

func digestFromProto(digest *remoteexecution.Digest) (assetDigest, error) {
	if digest == nil {
		return assetDigest{}, fmt.Errorf("digest is required")
	}
	if digest.GetHash() == "" {
		return assetDigest{}, fmt.Errorf("digest hash is required")
	}
	if digest.GetSizeBytes() < 0 {
		return assetDigest{}, fmt.Errorf("digest size_bytes must be non-negative")
	}

	return assetDigest{
		Hash:      digest.GetHash(),
		SizeBytes: digest.GetSizeBytes(),
	}, nil
}

func (digest assetDigest) toProto() *remoteexecution.Digest {
	return &remoteexecution.Digest{
		Hash:      digest.Hash,
		SizeBytes: digest.SizeBytes,
	}
}

func normalizeDigestFunction(fn remoteexecution.DigestFunction_Value) remoteexecution.DigestFunction_Value {
	if fn == remoteexecution.DigestFunction_UNKNOWN || fn == 0 {
		return remoteexecution.DigestFunction_SHA256
	}
	return fn
}

func (record *assetRecord) digestFunction() remoteexecution.DigestFunction_Value {
	if record == nil || record.DigestFunc == 0 {
		return remoteexecution.DigestFunction_SHA256
	}
	return remoteexecution.DigestFunction_Value(record.DigestFunc)
}

func (record *assetRecord) expiresAtProto() *timestamppb.Timestamp {
	if record == nil || record.ExpiresAt == nil || record.ExpiresAt.IsZero() {
		return nil
	}
	return timestamppb.New(record.ExpiresAt.UTC())
}

func (record *assetRecord) isExpired(now time.Time) bool {
	if record == nil || record.ExpiresAt == nil || record.ExpiresAt.IsZero() {
		return false
	}
	return now.After(record.ExpiresAt.UTC())
}

func (record *assetRecord) isTooOld(oldest time.Time) bool {
	if record == nil || record.PushedAt.IsZero() || oldest.IsZero() {
		return false
	}
	return record.PushedAt.Before(oldest)
}

func validateURIs(uris []string) ([]string, error) {
	if len(uris) == 0 {
		return nil, fmt.Errorf("at least one uri is required")
	}

	for _, uri := range uris {
		if strings.TrimSpace(uri) == "" {
			return nil, fmt.Errorf("uri must be non-empty")
		}
	}
	return uris, nil
}

func assetStorageKey(kind assetKind, instanceName, uri string, qualifiers []assetQualifier) string {
	hasher := sha256.New()
	hasher.Write([]byte(kind))
	hasher.Write([]byte{0})
	hasher.Write([]byte(instanceName))
	hasher.Write([]byte{0})
	hasher.Write([]byte(uri))
	for _, qualifier := range qualifiers {
		hasher.Write([]byte{0})
		hasher.Write([]byte(qualifier.Name))
		hasher.Write([]byte{0})
		hasher.Write([]byte(qualifier.Value))
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	return path.Join(assetPrefix, string(kind), sum)
}
