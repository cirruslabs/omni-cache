package builtin

import (
	"github.com/cirruslabs/omni-cache/internal/protocols/ghacache"
	"github.com/cirruslabs/omni-cache/internal/protocols/http_cache"
	"github.com/cirruslabs/omni-cache/internal/protocols/llvm_cache"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
)

func Factories() []protocols.Factory {
	return []protocols.Factory{
		http_cache.Factory{},
		ghacache.Factory{},
		llvm_cache.Factory{},
	}
}
