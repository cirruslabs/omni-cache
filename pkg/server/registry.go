package server

import (
	"github.com/cirruslabs/omni-cache/pkg/protocols"
)

var registry = make([]*protocols.CachingServerFactory, 0)

// RegisterDefaultCachingServerFactory allow each protocol to register itself for auto-discovery.
// This way in tests we'll automatically check for conflicts between different protocols because each will test agains each other.
func RegisterDefaultCachingServerFactory(factory *protocols.CachingServerFactory) {
	registry = append(registry, factory)
}
