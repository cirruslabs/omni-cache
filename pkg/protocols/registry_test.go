package protocols_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cirruslabs/omni-cache/pkg/protocols"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestRegistryHandleFunc(t *testing.T) {
	mux := http.NewServeMux()
	registry := protocols.NewRegistry(mux, grpc.NewServer())
	registry.HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/ok", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
}

func TestRegistryRegisterGRPC(t *testing.T) {
	mux := http.NewServeMux()
	registry := protocols.NewRegistry(mux, grpc.NewServer())

	require.NoError(t, registry.RegisterGRPC(func(*grpc.Server) {}))
	require.True(t, registry.HasGRPC())
}

func TestRegistryRegisterGRPCRequiresServer(t *testing.T) {
	mux := http.NewServeMux()
	registry := protocols.NewRegistry(mux, nil)

	err := registry.RegisterGRPC(func(*grpc.Server) {})
	require.ErrorIs(t, err, protocols.ErrGRPCUnavailable)
}
