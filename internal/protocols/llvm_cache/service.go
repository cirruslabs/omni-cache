package llvm_cache

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	casv1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/cas/v1"
	keyvaluev1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/keyvalue/v1"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	urlproxy "github.com/cirruslabs/omni-cache/pkg/url-proxy"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"
)

const (
	kvPrefix     = "llvm-cache/kv/"
	casPrefix    = "llvm-cache/cas/"
	casIDPrefix  = "llvmcas://"
	casHashBytes = 32
)

type cacheStore struct {
	backend storage.BlobStorageBackend
	proxy   *urlproxy.Proxy
}

func newCacheStore(backend storage.BlobStorageBackend, proxy *urlproxy.Proxy) *cacheStore {
	return &cacheStore{
		backend: backend,
		proxy:   proxy,
	}
}

func (s *cacheStore) download(ctx context.Context, key string) ([]byte, error) {
	if s.backend == nil {
		return nil, fmt.Errorf("storage backend is nil")
	}

	if _, err := s.backend.CacheInfo(ctx, key, nil); err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			return nil, storage.ErrCacheNotFound
		}
		return nil, err
	}

	infos, err := s.backend.DownloadURLs(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, fmt.Errorf("no download URLs returned")
	}

	var lastErr error
	for _, info := range infos {
		var buffer bytes.Buffer
		if err := s.proxy.DownloadToWriter(ctx, info, key, &buffer); err == nil {
			return buffer.Bytes(), nil
		} else {
			lastErr = err
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("download failed")
}

func (s *cacheStore) upload(ctx context.Context, key string, data []byte) error {
	if s.backend == nil {
		return fmt.Errorf("storage backend is nil")
	}
	info, err := s.backend.UploadURL(ctx, key, nil)
	if err != nil {
		return err
	}
	return s.proxy.UploadFromReader(ctx, info, key, bytes.NewReader(data), int64(len(data)))
}

type kvService struct {
	keyvaluev1.UnimplementedKeyValueDBServer
	store *cacheStore
}

func newKVService(store *cacheStore) *kvService {
	return &kvService{store: store}
}

func (s *kvService) GetValue(ctx context.Context, req *keyvaluev1.GetValueRequest) (*keyvaluev1.GetValueResponse, error) {
	data, err := s.store.download(ctx, kvStorageKey(req.GetKey()))
	if err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			return &keyvaluev1.GetValueResponse{Outcome: keyvaluev1.GetValueResponse_KEY_NOT_FOUND}, nil
		}
		return kvGetValueError(err), nil
	}

	var value keyvaluev1.Value
	if err := proto.Unmarshal(data, &value); err != nil {
		return kvGetValueError(err), nil
	}

	return &keyvaluev1.GetValueResponse{
		Outcome:  keyvaluev1.GetValueResponse_SUCCESS,
		Contents: &keyvaluev1.GetValueResponse_Value{Value: &value},
	}, nil
}

func (s *kvService) PutValue(ctx context.Context, req *keyvaluev1.PutValueRequest) (*keyvaluev1.PutValueResponse, error) {
	value := req.GetValue()
	if value == nil {
		value = &keyvaluev1.Value{}
	}

	data, err := proto.Marshal(value)
	if err != nil {
		return kvPutValueError(err), nil
	}

	if err := s.store.upload(ctx, kvStorageKey(req.GetKey()), data); err != nil {
		return kvPutValueError(err), nil
	}

	return &keyvaluev1.PutValueResponse{}, nil
}

type casService struct {
	casv1.UnimplementedCASDBServiceServer
	store *cacheStore
}

func newCASService(store *cacheStore) *casService {
	return &casService{store: store}
}

func (s *casService) Get(ctx context.Context, req *casv1.CASGetRequest) (*casv1.CASGetResponse, error) {
	casID := req.GetCasId()
	if casID == nil {
		return casGetError(fmt.Errorf("missing CAS id")), nil
	}

	digest, _, err := parseCASID(casID.GetId())
	if err != nil {
		return casGetError(err), nil
	}

	obj, err := s.loadCASObject(ctx, hex.EncodeToString(digest))
	if err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			return &casv1.CASGetResponse{Outcome: casv1.CASGetResponse_OBJECT_NOT_FOUND}, nil
		}
		return casGetError(err), nil
	}

	blobData, err := casBlobData(obj.GetBlob())
	if err != nil {
		return casGetError(err), nil
	}

	blob, err := casBytesForResponse(blobData, req.GetWriteToDisk())
	if err != nil {
		return casGetError(err), nil
	}

	return &casv1.CASGetResponse{
		Outcome: casv1.CASGetResponse_SUCCESS,
		Contents: &casv1.CASGetResponse_Data{Data: &casv1.CASObject{
			Blob:       blob,
			References: obj.GetReferences(),
		}},
	}, nil
}

func (s *casService) Put(ctx context.Context, req *casv1.CASPutRequest) (*casv1.CASPutResponse, error) {
	obj := req.GetData()
	if obj == nil {
		return casPutError(fmt.Errorf("missing object data")), nil
	}

	blobData, err := casBlobData(obj.GetBlob())
	if err != nil {
		return casPutError(err), nil
	}

	refDigests, normalizedRefs, err := normalizeRefs(obj.GetReferences())
	if err != nil {
		return casPutError(err), nil
	}

	digest, err := hashObject(refDigests, blobData)
	if err != nil {
		return casPutError(err), nil
	}

	casID := casIDFromDigest(digest[:])
	stored := &casv1.CASObject{
		Blob:       &casv1.CASBytes{Contents: &casv1.CASBytes_Data{Data: blobData}},
		References: normalizedRefs,
	}
	payload, err := proto.Marshal(stored)
	if err != nil {
		return casPutError(err), nil
	}

	if err := s.store.upload(ctx, casStorageKey(hex.EncodeToString(digest[:])), payload); err != nil {
		return casPutError(err), nil
	}

	return &casv1.CASPutResponse{Contents: &casv1.CASPutResponse_CasId{CasId: &casv1.CASDataID{Id: []byte(casID)}}}, nil
}

func (s *casService) Load(ctx context.Context, req *casv1.CASLoadRequest) (*casv1.CASLoadResponse, error) {
	casID := req.GetCasId()
	if casID == nil {
		return casLoadError(fmt.Errorf("missing CAS id")), nil
	}

	digest, _, err := parseCASID(casID.GetId())
	if err != nil {
		return casLoadError(err), nil
	}

	obj, err := s.loadCASObject(ctx, hex.EncodeToString(digest))
	if err != nil {
		if errors.Is(err, storage.ErrCacheNotFound) {
			return &casv1.CASLoadResponse{Outcome: casv1.CASLoadResponse_OBJECT_NOT_FOUND}, nil
		}
		return casLoadError(err), nil
	}

	blobData, err := casBlobData(obj.GetBlob())
	if err != nil {
		return casLoadError(err), nil
	}

	blob, err := casBytesForResponse(blobData, req.GetWriteToDisk())
	if err != nil {
		return casLoadError(err), nil
	}

	return &casv1.CASLoadResponse{
		Outcome:  casv1.CASLoadResponse_SUCCESS,
		Contents: &casv1.CASLoadResponse_Data{Data: &casv1.CASBlob{Blob: blob}},
	}, nil
}

func (s *casService) Save(ctx context.Context, req *casv1.CASSaveRequest) (*casv1.CASSaveResponse, error) {
	data := req.GetData()
	if data == nil {
		return casSaveError(fmt.Errorf("missing CAS blob")), nil
	}

	blobData, err := casBlobData(data.GetBlob())
	if err != nil {
		return casSaveError(err), nil
	}

	digest, err := hashObject(nil, blobData)
	if err != nil {
		return casSaveError(err), nil
	}

	casID := casIDFromDigest(digest[:])
	stored := &casv1.CASObject{
		Blob: &casv1.CASBytes{Contents: &casv1.CASBytes_Data{Data: blobData}},
	}
	payload, err := proto.Marshal(stored)
	if err != nil {
		return casSaveError(err), nil
	}

	if err := s.store.upload(ctx, casStorageKey(hex.EncodeToString(digest[:])), payload); err != nil {
		return casSaveError(err), nil
	}

	return &casv1.CASSaveResponse{Contents: &casv1.CASSaveResponse_CasId{CasId: &casv1.CASDataID{Id: []byte(casID)}}}, nil
}

func (s *casService) loadCASObject(ctx context.Context, digestHex string) (*casv1.CASObject, error) {
	data, err := s.store.download(ctx, casStorageKey(digestHex))
	if err != nil {
		return nil, err
	}

	var obj casv1.CASObject
	if err := proto.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

func kvStorageKey(key []byte) string {
	return kvPrefix + base64.RawURLEncoding.EncodeToString(key)
}

func casStorageKey(digestHex string) string {
	return casPrefix + digestHex
}

func parseCASID(raw []byte) ([]byte, string, error) {
	if len(raw) == 0 {
		return nil, "", fmt.Errorf("empty CAS id")
	}

	if len(raw) == casHashBytes {
		id := casIDFromDigest(raw)
		return append([]byte(nil), raw...), id, nil
	}

	value := strings.TrimPrefix(string(raw), casIDPrefix)

	if len(value) != casHashBytes*2 {
		return nil, "", fmt.Errorf("invalid CAS id length")
	}

	digest, err := hex.DecodeString(value)
	if err != nil {
		return nil, "", fmt.Errorf("invalid CAS id")
	}
	if len(digest) != casHashBytes {
		return nil, "", fmt.Errorf("invalid CAS id size")
	}

	id := casIDFromDigest(digest)
	return digest, id, nil
}

func casIDFromDigest(digest []byte) string {
	return casIDPrefix + hex.EncodeToString(digest)
}

func normalizeRefs(refs []*casv1.CASDataID) ([][]byte, []*casv1.CASDataID, error) {
	if len(refs) == 0 {
		return nil, nil, nil
	}

	digests := make([][]byte, 0, len(refs))
	normalized := make([]*casv1.CASDataID, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			return nil, nil, fmt.Errorf("missing CAS reference")
		}
		digest, id, err := parseCASID(ref.GetId())
		if err != nil {
			return nil, nil, err
		}
		digests = append(digests, digest)
		normalized = append(normalized, &casv1.CASDataID{Id: []byte(id)})
	}

	return digests, normalized, nil
}

func hashObject(refDigests [][]byte, data []byte) ([casHashBytes]byte, error) {
	for _, ref := range refDigests {
		if len(ref) != casHashBytes {
			return [casHashBytes]byte{}, fmt.Errorf("invalid reference size")
		}
	}

	hasher := blake3.New()
	var sizeBuf [8]byte
	binary.LittleEndian.PutUint64(sizeBuf[:], uint64(len(refDigests)))
	_, _ = hasher.Write(sizeBuf[:])

	for _, ref := range refDigests {
		_, _ = hasher.Write(ref)
	}

	binary.LittleEndian.PutUint64(sizeBuf[:], uint64(len(data)))
	_, _ = hasher.Write(sizeBuf[:])
	_, _ = hasher.Write(data)

	sum := hasher.Sum(nil)
	var digest [casHashBytes]byte
	copy(digest[:], sum)
	return digest, nil
}

func casBlobData(blob *casv1.CASBytes) ([]byte, error) {
	if blob == nil {
		return nil, fmt.Errorf("missing CAS blob")
	}

	switch value := blob.GetContents().(type) {
	case *casv1.CASBytes_Data:
		return value.Data, nil
	case *casv1.CASBytes_FilePath:
		if value.FilePath == "" {
			return nil, fmt.Errorf("empty CAS blob file path")
		}
		return os.ReadFile(value.FilePath)
	default:
		return nil, fmt.Errorf("missing CAS blob contents")
	}
}

func casBytesForResponse(data []byte, writeToDisk bool) (*casv1.CASBytes, error) {
	if !writeToDisk {
		return &casv1.CASBytes{Contents: &casv1.CASBytes_Data{Data: data}}, nil
	}

	path, err := writeTempBlob(data)
	if err != nil {
		return nil, err
	}
	return &casv1.CASBytes{Contents: &casv1.CASBytes_FilePath{FilePath: path}}, nil
}

func writeTempBlob(data []byte) (string, error) {
	file, err := os.CreateTemp("", "omni-cache-*.blob")
	if err != nil {
		return "", err
	}
	name := file.Name()

	if _, err := file.Write(data); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return name, nil
}

func kvGetValueError(err error) *keyvaluev1.GetValueResponse {
	return &keyvaluev1.GetValueResponse{
		Outcome:  keyvaluev1.GetValueResponse_ERROR,
		Contents: &keyvaluev1.GetValueResponse_Error{Error: &keyvaluev1.ResponseError{Description: err.Error()}},
	}
}

func kvPutValueError(err error) *keyvaluev1.PutValueResponse {
	return &keyvaluev1.PutValueResponse{Error: &keyvaluev1.ResponseError{Description: err.Error()}}
}

func casGetError(err error) *casv1.CASGetResponse {
	return &casv1.CASGetResponse{
		Outcome:  casv1.CASGetResponse_ERROR,
		Contents: &casv1.CASGetResponse_Error{Error: &casv1.ResponseError{Description: err.Error()}},
	}
}

func casPutError(err error) *casv1.CASPutResponse {
	return &casv1.CASPutResponse{Contents: &casv1.CASPutResponse_Error{Error: &casv1.ResponseError{Description: err.Error()}}}
}

func casLoadError(err error) *casv1.CASLoadResponse {
	return &casv1.CASLoadResponse{
		Outcome:  casv1.CASLoadResponse_ERROR,
		Contents: &casv1.CASLoadResponse_Error{Error: &casv1.ResponseError{Description: err.Error()}},
	}
}

func casSaveError(err error) *casv1.CASSaveResponse {
	return &casv1.CASSaveResponse{Contents: &casv1.CASSaveResponse_Error{Error: &casv1.ResponseError{Description: err.Error()}}}
}
