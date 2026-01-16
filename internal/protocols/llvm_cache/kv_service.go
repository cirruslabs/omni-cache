package llvm_cache

import (
	"context"
	"encoding/base64"
	"errors"

	keyvaluev1 "github.com/cirruslabs/omni-cache/internal/api/compilation_cache_service/keyvalue/v1"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"google.golang.org/protobuf/proto"
)

const kvPrefix = "llvm-cache/kv/"

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

func kvStorageKey(key []byte) string {
	return kvPrefix + base64.RawURLEncoding.EncodeToString(key)
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
