package httpsync

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
)

type AllowPeerFunc func(peerID peer.ID) (bool, error)

// syncerConfig contains all options for configuring Syncer.
type syncerConfig struct {
	allowPeer AllowPeerFunc
}

type SyncerOption func(*syncerConfig) error

// apply applies the given options to this config.
func (c *syncerConfig) apply(opts []SyncerOption) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

// AllowedPeerId configures the http syncer to only accept head updates from the
// given peer.
func AllowedPeerId(allowedPeerId peer.ID) SyncerOption {
	return func(c *syncerConfig) error {
		c.allowPeer = func(peerID peer.ID) (bool, error) {
			return peerID == allowedPeerId, nil
		}
		return nil
	}
}
