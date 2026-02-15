package builtin

import (
	"github.com/cirruslabs/omni-cache/internal/protocols/azureblob"
	"github.com/cirruslabs/omni-cache/internal/protocols/bazel_remote"
	"github.com/cirruslabs/omni-cache/internal/protocols/ghacache"
	"github.com/cirruslabs/omni-cache/internal/protocols/ghacachev2"
	"github.com/cirruslabs/omni-cache/internal/protocols/http_cache"
	"github.com/cirruslabs/omni-cache/internal/protocols/llvm_cache"
	"github.com/cirruslabs/omni-cache/internal/protocols/tuist_cache"
	"github.com/cirruslabs/omni-cache/pkg/protocols"
)

func Factories() []protocols.Factory {
	return []protocols.Factory{
		azureblob.Factory{},
		tuist_cache.Factory{},
		http_cache.Factory{},
		bazel_remote.Factory{},
		ghacache.Factory{},
		ghacachev2.Factory{},
		llvm_cache.Factory{},
	}
}
