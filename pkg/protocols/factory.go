package protocols

import (
	"net/http"

	"github.com/cirruslabs/omni-cache/pkg/storage"
)

type CachingServerFactory struct {
	Pattern string
	Create  func(httpClient *http.Client, storagBackend storage.BlobStorageBacked) http.Handler
}
