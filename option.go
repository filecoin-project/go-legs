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
	"golang.org/x/time/rate"
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

	rateLimiterFor RateLimiterFor

	segDepthLimit int64
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

// BlockHook adds a hook that is run when a block is received via Subscriber.Sync along with a
// SegmentSyncActions to control the sync flow if segmented sync is enabled.
// Note that if segmented sync is disabled, calls on SegmentSyncActions will have no effect.
// See: SegmentSyncActions, SegmentDepthLimit, ScopedBlockHook.
func BlockHook(blockHook BlockHookFunc) Option {
	return func(c *config) error {
		c.blockHook = blockHook
		return nil
	}
}

// SegmentDepthLimit sets the maximum recursion depth limit for a segmented sync.
// Setting the depth to a value less than zero disables segmented sync completely.
// Disabled by default.
// Note that for segmented sync to function at least one of BlockHook or ScopedBlockHook must be
// set.
func SegmentDepthLimit(depth int64) Option {
	return func(c *config) error {
		c.segDepthLimit = depth
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

type RateLimiterFor func(publisher peer.ID) *rate.Limiter

// RateLimiter configures a function that is called for each sync to get the
// rate limiter for a specific peer.
func RateLimiter(limiterFor RateLimiterFor) Option {
	return func(c *config) error {
		c.rateLimiterFor = limiterFor
		return nil
	}
}

// LatestSyncHandler defines how to store the latest synced cid for a given peer
// and how to fetch it. Legs guarantees this will not be called concurrently for
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
	segDepthLimit      int64
}

type SyncOption func(*syncCfg)

func AlwaysUpdateLatest() SyncOption {
	return func(sc *syncCfg) {
		sc.alwaysUpdateLatest = true
	}
}

// ScopedBlockHook is the equivalent of BlockHook option but only applied to a single sync.
// If not specified, the Subscriber BlockHook option is used instead.
// Specifying the ScopedBlockHook will override the Subscriber level BlockHook for the current
// sync.
// Note that calls to SegmentSyncActions from bloc hook will have no impact if segmented sync is
// disabled.
// See: BlockHook, SegmentDepthLimit, ScopedSegmentDepthLimit.
func ScopedBlockHook(hook BlockHookFunc) SyncOption {
	return func(sc *syncCfg) {
		sc.scopedBlockHook = hook
	}
}

// ScopedSegmentDepthLimit is the equivalent of SegmentDepthLimit option but only applied to a
// single sync.
// If not specified, the Subscriber SegmentDepthLimit option is used instead.
// Note that for segmented sync to function at least one of BlockHook or ScopedBlockHook must be
// set.
// See: SegmentDepthLimit.
func ScopedSegmentDepthLimit(depth int64) SyncOption {
	return func(sc *syncCfg) {
		sc.segDepthLimit = depth
	}
}
