package legs

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// config contains all options for configuring Subscriber.
type config struct {
	addrTTL   time.Duration
	allowPeer AllowPeerFunc

	topic *pubsub.Topic

	dtManager     dt.Manager
	graphExchange graphsync.GraphExchange

	blockHook  BlockHookFunc
	httpClient *http.Client

	syncRecLimit selector.RecursionLimit

	latestSyncHandler LatestSyncHandler
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

// DtManager provides an existing datatransfer manager.
func DtManager(dtManager dt.Manager, gs graphsync.GraphExchange) Option {
	return func(c *config) error {
		c.dtManager = dtManager
		c.graphExchange = gs
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

// LatestSyncHandler defines how to store the latest synced cid for a given peer
// and how to fetch it. Legs gaurantees this will not be called concurrently for
// the same peer, but it may be called concurrently for different peers.
type LatestSyncHandler interface {
	SetLatestSync(peer peer.ID, cid cid.Cid)
	GetLatestSync(peer peer.ID) (cid.Cid, bool)
}

type DefaultLatestSyncHandler struct {
	m sync.Map
}

func (h *DefaultLatestSyncHandler) SetLatestSync(p peer.ID, c cid.Cid) {
	h.m.Store(p, c)
}

func (h *DefaultLatestSyncHandler) GetLatestSync(p peer.ID) (cid.Cid, bool) {
	v, ok := h.m.Load(p)
	if !ok {
		return cid.Undef, false
	}
	return v.(cid.Cid), true
}

// UseLatestSyncHandler sets the latest sync handler to use.
func UseLatestSyncHandler(h LatestSyncHandler) Option {
	return func(c *config) error {
		c.latestSyncHandler = h
		return nil
	}
}

type syncCfg struct {
	alwaysUpdateLatest bool
	scopedBlockHook    BlockHookFunc
}

type SyncOption func(*syncCfg)

func AlwaysUpdateLatest() SyncOption {
	return func(sc *syncCfg) {
		sc.alwaysUpdateLatest = true
	}
}

func ScopedBlockHook(hook BlockHookFunc) SyncOption {
	return func(sc *syncCfg) {
		sc.scopedBlockHook = hook
	}
}
