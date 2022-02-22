package legs

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ipld/go-ipld-prime/traversal/selector"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// config contains all options for configuring Subscriber.
type config struct {
	addrTTL   time.Duration
	allowPeer AllowPeerFunc

	topic *pubsub.Topic
	// TODO We can re-enable this when we figure out how to register a block hook with an existing dtManager
	// dtManager  dt.Manager
	blockHook  BlockHookFunc
	httpClient *http.Client

	syncRecLimit selector.RecursionLimit
}

type Option func(*config) error

// apply applies the given options to this config.
func (c *config) apply(opts []Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

// AllowPeer sets the function that determines whether to allow or reject
// messages from a peer.
func AllowPeer(allowPeer AllowPeerFunc) Option {
	return func(c *config) error {
		c.allowPeer = allowPeer
		return nil
	}
}

// AddrTTL sets the peerstore address time-to-live for addresses discovered
// from pubsub messages.
func AddrTTL(addrTTL time.Duration) Option {
	return func(c *config) error {
		c.addrTTL = addrTTL
		return nil
	}
}

// Topic provides an existing pubsub topic.
func Topic(topic *pubsub.Topic) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}

// HttpClient provides Subscriber with an existing http client.
func HttpClient(client *http.Client) Option {
	return func(c *config) error {
		c.httpClient = client
		return nil
	}
}

// BlockHook adds a hook that runs when a block is received.
func BlockHook(blockHook BlockHookFunc) Option {
	return func(c *config) error {
		c.blockHook = blockHook
		return nil
	}
}

// SyncRecursionLimit sets the recursion limit of the background syncing process.
// Defaults to selector.RecursionLimitNone if not specified.
func SyncRecursionLimit(limit selector.RecursionLimit) Option {
	return func(c *config) error {
		c.syncRecLimit = limit
		return nil
	}
}

type syncCfg struct {
	scopedBlockHook    BlockHookFunc
	alwaysUpdateLatest bool
}

type SyncOption func(*syncCfg)

func ScopedBlockHook(hook BlockHookFunc) SyncOption {
	return func(sc *syncCfg) {
		sc.scopedBlockHook = hook
	}
}

func AlwaysUpdateLatest() SyncOption {
	return func(sc *syncCfg) {
		sc.alwaysUpdateLatest = true
	}
}
