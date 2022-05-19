package legs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/gpubsub"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
)

var log = logging.Logger("go-legs")

// defaultAddrTTL is the default amount of time that addresses discovered from
// pubsub messages will remain in the peerstore.  This is twice the default
// provider poll interval.
const (
	defaultAddrTTL       = 48 * time.Hour
	tempAddrTTL          = 24 * time.Hour // must be long enough for ad chain to sync
	defaultSegDepthLimit = -1             // Segmented sync disabled.
)

// errSourceNotAllowed is the error returned when a message source peer's
// messages is not allowed to be processed.  This is only used internally, and
// pre-allocated here as it may occur frequently.
var errSourceNotAllowed = errors.New("message source not allowed")

// AllowPeerFunc is the signature of a function given to Subscriber that
// determines whether to allow or reject messages originating from a peer
// passed into the function.  Returning true or false indicates that messages
// from that peer are allowed rejected, respectively.
type AllowPeerFunc func(peer.ID) bool

// BlockHookFunc is the signature of a function that is called when a received.
type BlockHookFunc func(peer.ID, cid.Cid, SegmentSyncActions)

// Subscriber creates a single pubsub subscriber that receives messages from a
// gossip pubsub topic, and creates a stateful message handler for each message
// source peer.  An optional externally-defined AllowPeerFunc determines
// whether to allow or deny messages from specific peers.
//
// Messages from separate peers are handled concurrently, and multiple messages
// from the same peer are handled serially.  If a handler is busy handling a
// message, and more messages arrive from the same peer, then the last message
// replaces the previous unhandled message to avoid having to maintain queues
// of messages.  Handlers do not have persistent goroutines, but start a new
// goroutine to handle a single message.
type Subscriber struct {
	// dss captures the default selector sequence passed to
	// ExploreRecursiveWithStopNode.
	dss  ipld.Node
	host host.Host

	addrTTL   time.Duration
	psub      *pubsub.Subscription
	topic     *pubsub.Topic
	topicName string

	allowPeer     AllowPeerFunc
	handlers      map[peer.ID]*handler
	handlersMutex sync.Mutex

	// A map of block hooks to call for a specific peer id if the generalBlockHook is overridden
	// within a sync via ScopedBlockHook sync option.
	scopedBlockHook      map[peer.ID]func(peer.ID, cid.Cid)
	scopedBlockHookMutex *sync.RWMutex
	generalBlockHook     BlockHookFunc

	// inEvents is used to send a SyncFinished from a peer handler to the
	// distributeEvents goroutine.
	inEvents chan SyncFinished

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of a SyncFinished to an OnSyncFinished reader.
	outEventsChans []chan SyncFinished
	outEventsMutex sync.Mutex

	// closing signals that the Subscriber is closing.
	closing chan struct{}
	// cancelps cancels pubsub.
	cancelps context.CancelFunc
	// closeOnce ensures that the Close only happens once.
	closeOnce sync.Once
	// watchDone signals that the pubsub watch function exited.
	watchDone chan struct{}
	asyncWG   sync.WaitGroup

	dtSync       *dtsync.Sync
	httpSync     *httpsync.Sync
	syncRecLimit selector.RecursionLimit

	httpPeerstore peerstore.Peerstore

	latestSyncHander LatestSyncHandler

	// limiterFor defines the rate limits for each publisher
	limiterFor RateLimiterFor

	segDepthLimit int64
}

// SyncFinished notifies an OnSyncFinished reader that a specified peer
// completed a sync.  The channel receives events from providers that are
// manually synced to the latest, as well as those auto-discovered.
type SyncFinished struct {
	// Cid is the CID identifying the link that finished and is now the latest
	// sync for a specific peer.
	Cid cid.Cid
	// PeerID identifies the peer this SyncFinished event pertains to.
	PeerID peer.ID
	// A list of cids that this sync acquired. In order from latest to
	// oldest. The latest cid will always be at the beginning.
	SyncedCids []cid.Cid
}

// handler holds state that is specific to a peer
type handler struct {
	subscriber *Subscriber
	// syncMutex serializes the handling of individual syncs. This should only
	// guard the actual handling of a sync, nothing else.
	syncMutex sync.Mutex
	// If this sync will update the latestSync state (via latestSyncHandler) then
	// it should grab this lock to insure no other process updates that state
	// concurrently.
	latestSyncMu sync.Mutex
	// peerID is the ID of the peer this handler is responsible for.
	peerID peer.ID
	// pendingCid is a CID queued for async handling.
	pendingCid cid.Cid
	// pendingSyncer is a syncer queued for handling pendingCid.
	pendingSyncer Syncer
	// qlock protects the lendingCid and pendingSyncer.
	qlock sync.Mutex
}

// wrapBlockHook wraps a possibly nil block hook func to allow a for dispatching
// to a blockhook func that is scoped within a .Sync call.
func wrapBlockHook() (*sync.RWMutex, map[peer.ID]func(peer.ID, cid.Cid), func(peer.ID, cid.Cid)) {
	var scopedBlockHookMutex sync.RWMutex
	scopedBlockHook := make(map[peer.ID]func(peer.ID, cid.Cid))
	return &scopedBlockHookMutex, scopedBlockHook, func(peerID peer.ID, cid cid.Cid) {
		scopedBlockHookMutex.RLock()
		f, ok := scopedBlockHook[peerID]
		scopedBlockHookMutex.RUnlock()
		if ok {
			f(peerID, cid)
		}
	}
}

// NewSubscriber creates a new Subscriber that process pubsub messages.
func NewSubscriber(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, dss ipld.Node, options ...Option) (*Subscriber, error) {
	cfg := config{
		addrTTL:       defaultAddrTTL,
		segDepthLimit: defaultSegDepthLimit,
	}
	err := cfg.apply(options)
	if err != nil {
		return nil, err
	}

	ctx, cancelPubsub := context.WithCancel(context.Background())

	var pubsubTopic *pubsub.Topic
	if cfg.topic == nil {
		pubsubTopic, err = gpubsub.MakePubsub(ctx, host, topic)
		if err != nil {
			cancelPubsub()
			return nil, err
		}
		cfg.topic = pubsubTopic
	}
	psub, err := cfg.topic.Subscribe()
	if err != nil {
		cancelPubsub()
		return nil, err
	}

	scopedBlockHookMutex, scopedBlockHook, blockHook := wrapBlockHook()

	if cfg.rateLimiterFor == nil {
		// Allows all events. No rate limit.
		limiter := rate.NewLimiter(rate.Inf, 0)
		cfg.rateLimiterFor = func(publisher peer.ID) *rate.Limiter {
			return limiter
		}
	}

	var dtSync *dtsync.Sync
	if cfg.dtManager != nil {
		if ds != nil {
			cancelPubsub()
			return nil, fmt.Errorf("datastore cannot be used with DtManager option")
		}
		dtSync, err = dtsync.NewSyncWithDT(host, cfg.dtManager, cfg.graphExchange, blockHook, cfg.rateLimiterFor)
	} else {
		dtSync, err = dtsync.NewSync(host, ds, lsys, blockHook, cfg.rateLimiterFor)
	}
	if err != nil {
		cancelPubsub()
		return nil, err
	}

	httpPeerstore, err := pstoremem.NewPeerstore()
	if err != nil {
		cancelPubsub()
		return nil, err
	}

	latestSyncHandler := cfg.latestSyncHandler
	if latestSyncHandler == nil {
		latestSyncHandler = &DefaultLatestSyncHandler{}
	}

	s := &Subscriber{
		dss:  dss,
		host: host,

		addrTTL:   cfg.addrTTL,
		psub:      psub,
		topic:     cfg.topic,
		topicName: cfg.topic.String(),
		closing:   make(chan struct{}),
		cancelps:  cancelPubsub,
		watchDone: make(chan struct{}),

		allowPeer: cfg.allowPeer,
		handlers:  make(map[peer.ID]*handler),
		inEvents:  make(chan SyncFinished, 1),

		dtSync:       dtSync,
		httpSync:     httpsync.NewSync(lsys, cfg.httpClient, blockHook),
		syncRecLimit: cfg.syncRecLimit,

		httpPeerstore: httpPeerstore,

		scopedBlockHookMutex: scopedBlockHookMutex,
		scopedBlockHook:      scopedBlockHook,
		generalBlockHook:     cfg.blockHook,

		latestSyncHander: latestSyncHandler,

		limiterFor: cfg.rateLimiterFor,

		segDepthLimit: cfg.segDepthLimit,
	}

	// Start watcher to read pubsub messages.
	go s.watch(ctx)
	// Start distributor to send SyncFinished messages to interested parties.
	go s.distributeEvents()

	return s, nil
}

// HttpPeerStore returns the subscriber's HTTP peer store.
func (s *Subscriber) HttpPeerStore() peerstore.Peerstore {
	return s.httpPeerstore
}

// GetLatestSync returns the latest synced CID for the specified peer. If there
// is not handler for the peer, then nil is returned.  This does not mean that
// no data is synced with that peer, it means that the Subscriber does not know
// about it.  Calling Sync() first may be necessary.
func (s *Subscriber) GetLatestSync(peerID peer.ID) ipld.Link {
	v, ok := s.latestSyncHander.GetLatestSync(peerID)
	if !ok || v == cid.Undef {
		return nil
	}
	return cidlink.Link{Cid: v}
}

// SetLatestSync sets the latest synced CID for a specified peer.  If there is
// no handler for the peer, then one is created without consulting any
// AllowPeerFunc.
func (s *Subscriber) SetLatestSync(peerID peer.ID, latestSync cid.Cid) error {
	if latestSync == cid.Undef {
		return errors.New("cannot set latest sync to undefined value")
	}
	hnd, err := s.getOrCreateHandler(peerID, true)
	if err != nil {
		return err
	}
	hnd.latestSyncMu.Lock()
	defer hnd.latestSyncMu.Unlock()

	s.latestSyncHander.SetLatestSync(peerID, latestSync)
	return nil
}

// SetAllowPeer configures Subscriber with a function to evaluate whether to
// allow or reject messages from a peer.  Setting nil removes any filtering and
// allows messages from all peers.  Calling SetAllowPeer replaces any
// previously configured AllowPeerFunc.
func (s *Subscriber) SetAllowPeer(allowPeer AllowPeerFunc) {
	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()
	s.allowPeer = allowPeer
}

// Close shuts down the Subscriber.
func (s *Subscriber) Close() error {
	var err error
	s.closeOnce.Do(func() {
		err = s.doClose()
	})
	return err
}

func (s *Subscriber) doClose() error {
	// Cancel pubsub and Wait for pubsub watcher to exit.
	s.psub.Cancel()
	<-s.watchDone
	s.asyncWG.Wait()

	var err, errs error
	if err = s.dtSync.Close(); err != nil {
		errs = multierror.Append(errs, err)
	}

	// If Subscriber owns the pubsub topic, then close it.
	if s.topic != nil {
		if err = s.topic.Close(); err != nil {
			log.Errorw("Failed to close pubsub topic", "err", err)
			errs = multierror.Append(errs, err)
		}
	}

	// Dismiss any event readers.
	s.outEventsMutex.Lock()
	for _, ch := range s.outEventsChans {
		close(ch)
	}
	s.outEventsChans = nil
	s.outEventsMutex.Unlock()

	// Shutdown pubsub services.
	s.cancelps()

	// Stop the distribution goroutine.
	close(s.inEvents)

	s.httpPeerstore.Close()

	return errs
}

// OnSyncFinished creates a channel that receives change notifications, and
// adds that channel to the list of notification channels.
//
// Calling the returned cancel function removes the notification channel from
// the list of channels to be notified on changes, and it closes the channel to
// allow any reading goroutines to stop waiting on the channel.
func (s *Subscriber) OnSyncFinished() (<-chan SyncFinished, context.CancelFunc) {
	// Channel is buffered to prevent distribute() from blocking if a reader is
	// not reading the channel immediately.
	ch := make(chan SyncFinished, 1)
	s.outEventsMutex.Lock()
	defer s.outEventsMutex.Unlock()

	s.outEventsChans = append(s.outEventsChans, ch)
	cncl := func() {
		s.outEventsMutex.Lock()
		defer s.outEventsMutex.Unlock()
		for i, ca := range s.outEventsChans {
			if ca == ch {
				s.outEventsChans[i] = s.outEventsChans[len(s.outEventsChans)-1]
				s.outEventsChans[len(s.outEventsChans)-1] = nil
				s.outEventsChans = s.outEventsChans[:len(s.outEventsChans)-1]
				close(ch)
				break
			}
		}
	}
	return ch, cncl
}

// Sync performs a one-off explicit sync with the given peer for a specific CID
// and updates the latest synced link to it.  Completing sync may take a
// significant amount of time, so Sync should generally be run in its own
// goroutine.
//
// If given cid.Undef, the latest root CID is queried from the peer directly
// and used instead. Note that in an event where there is no latest root, i.e.
// querying the latest CID returns cid.Undef, this function returns cid.Undef
// with nil error.
//
// The latest synced CID is returned when this sync is complete.  Any
// OnSyncFinished readers will also get a SyncFinished when the sync succeeds,
// but only if syncing to the latest, using `cid.Undef`, and using the default
// selector.  This is because when specifying a CID, it is usually for an
// entries sync, not an advertisements sync.
//
// It is the responsibility of the caller to make sure the given CID appears
// after the latest sync in order to avid re-syncing of content that may have
// previously been synced.
//
// The selector sequence, sel, can optionally be specified to customize the
// selection sequence during traversal.  If unspecified, the default selector
// sequence is used.
//
// Note that the selector sequence is wrapped with a selector logic that will
// stop traversal when the latest synced link is reached. Therefore, it must
// only specify the selection sequence itself.
//
// See: ExploreRecursiveWithStopNode.
func (s *Subscriber) Sync(ctx context.Context, peerID peer.ID, nextCid cid.Cid, sel ipld.Node, peerAddr multiaddr.Multiaddr, opts ...SyncOption) (cid.Cid, error) {
	cfg := &syncCfg{
		// Fall back on general block hook if scoped block hook is not specified.
		scopedBlockHook: s.generalBlockHook,
		segDepthLimit:   s.segDepthLimit,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if peerID == "" {
		return cid.Undef, errors.New("empty peer id")
	}

	log := log.With("peer", peerID)

	var err error
	var syncer Syncer

	isHttpPeerAddr := false
	if peerAddr != nil {
		for _, p := range peerAddr.Protocols() {
			if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
				isHttpPeerAddr = true
				break
			}
		}
	} else {
		// Check if we have an http url for this peer since we didn't get a peerAddr.
		// Note that this gives a preference to use httpSync over dtsync if we have
		// seen http address and we called sync with no explicit peerAddr.
		possibleAddrs := s.httpPeerstore.Addrs(peerID)
		if len(possibleAddrs) > 0 {
			peerAddr = possibleAddrs[0]
			isHttpPeerAddr = true
		}
	}

	if isHttpPeerAddr {
		var limiter *rate.Limiter
		if cfg.rateLimiter != nil {
			// If we have an explicit rate limiter, we'll use it instead of the
			// default
			limiter = cfg.rateLimiter
		} else {
			limiter = s.limiterFor(peerID)
		}
		syncer, err = s.httpSync.NewSyncer(peerID, peerAddr, limiter)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot create http sync handler: %w", err)
		}
	} else {
		// Not an httpPeerAddr, so use the dtSync. We'll add it with a small TTL
		// first, and extend it when we discover we can actually sync from it.
		// In case the peerstore already has this address and the existing TTL is
		// greater than this temp one, this is a no-op. In other words we never
		// decrease the TTL here.
		peerStore := s.host.Peerstore()
		if peerStore != nil && peerAddr != nil {
			peerStore.AddAddr(peerID, peerAddr, tempAddrTTL)
		}
		syncer = s.dtSync.NewSyncer(peerID, s.topicName, cfg.rateLimiter)
	}

	updateLatest := cfg.alwaysUpdateLatest
	if nextCid == cid.Undef {
		// Query the peer for the latest CID
		nextCid, err = syncer.GetHead(ctx)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot query head for sync: %w. Possibly incorrect topic configured", err)
		}

		// Check if there is a latest CID.
		if nextCid == cid.Undef {
			// There is no head; nothing to sync.
			log.Info("No head to sync")
			return cid.Undef, nil
		}

		log.Infow("Sync queried head CID", "cid", nextCid)
		if sel == nil {
			// Update the latestSync only if no CID and no selector given.
			updateLatest = true
		}
	}
	log = log.With("cid", nextCid)

	log.Info("Start sync")

	if ctx.Err() != nil {
		return cid.Undef, fmt.Errorf("sync canceled: %w", ctx.Err())
	}

	var wrapSel bool
	if sel == nil {
		// Fall back onto the default selector sequence if one is not
		// given.  Note that if selector is specified it is used as is
		// without any wrapping.
		sel = s.dss
		wrapSel = true
	}

	// Check for existing handler.  If none, create one if allowed.
	hnd, err := s.getOrCreateHandler(peerID, true)
	if err != nil {
		return cid.Undef, err
	}

	if updateLatest {
		// Grab the latestSyncMu lock so that an async handler doesn't update the
		// latestSync between when we call hnd.handle and when we actually updateLatest.
		hnd.latestSyncMu.Lock()
		defer hnd.latestSyncMu.Unlock()
	}

	syncedCids, err := hnd.handle(ctx, nextCid, sel, wrapSel, syncer, cfg.scopedBlockHook, cfg.segDepthLimit)
	if err != nil {
		return cid.Undef, fmt.Errorf("sync handler failed: %w", err)
	}

	if updateLatest {
		hnd.subscriber.latestSyncHander.SetLatestSync(hnd.peerID, nextCid)
		hnd.subscriber.inEvents <- SyncFinished{Cid: nextCid, PeerID: hnd.peerID, SyncedCids: syncedCids}
		log.Infow("Updating latest sync")
	}

	// The sync succeeded, so let's remember this address in the appropriate
	// peerstore. If the address was already in the peerstore, this will extend
	// its ttl.
	if peerAddr != nil {
		if isHttpPeerAddr {
			// Store this http address so that future calls to sync will work without a
			// peerAddr (given that it happens within the TTL)
			s.httpPeerstore.AddAddr(peerID, peerAddr, s.addrTTL)
		} else {
			// Not an http address, so add to the host's libp2p peerstore.
			peerStore := s.host.Peerstore()
			if peerStore != nil {
				peerStore.AddAddr(peerID, peerAddr, s.addrTTL)
			}
		}
	}

	return nextCid, nil
}

// distributeEvents reads a SyncFinished, sent by a peer handler, and copies
// the even to all channels in outEventsChans.  This delivers the SyncFinished
// to all OnSyncFinished channel readers.
func (s *Subscriber) distributeEvents() {
	for event := range s.inEvents {
		if !event.Cid.Defined() {
			panic("SyncFinished event with undefined cid")
		}
		// Send update to all change notification channels.
		s.outEventsMutex.Lock()
		for _, ch := range s.outEventsChans {
			ch <- event
		}
		s.outEventsMutex.Unlock()
	}
}

// getOrCreateHandler creates a handler for a specific peer
func (s *Subscriber) getOrCreateHandler(peerID peer.ID, force bool) (*handler, error) {
	s.handlersMutex.Lock()
	defer s.handlersMutex.Unlock()

	// Check callback, if needed, to see if peer ID allowed.
	if s.allowPeer != nil && !force {
		if !s.allowPeer(peerID) {
			return nil, errSourceNotAllowed
		}
	}

	// Check for existing handler, return if found.
	hnd, ok := s.handlers[peerID]
	if ok {
		return hnd, nil
	}

	log.Infow("Creating new handler for publisher", "peer", peerID)
	hnd = &handler{
		subscriber: s,
		peerID:     peerID,
	}
	s.handlers[peerID] = hnd

	return hnd, nil
}

// watch reads messages from a pubsub topic subscription and passes the message
// to the handler that is responsible for the peer that originally sent the
// message.  If the handler does not yet exist, then the allowPeer callback is
// consulted to determine if the peer's messages are allowed.  If allowed, a
// new handler is created.  Otherwise, the message is ignored.
func (s *Subscriber) watch(ctx context.Context) {
	for {
		msg, err := s.psub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil || err == pubsub.ErrSubscriptionCancelled {
				// This is a normal result of shutting down the Subscriber.
				log.Debug("Canceled watching pubsub subscription")
			} else {
				log.Errorw("Error reading from pubsub", "err", err)
				// TODO: restart subscription.
			}
			break
		}

		srcPeer, err := peer.IDFromBytes(msg.From)
		if err != nil {
			continue
		}

		// Decode CID and originator addresses from message.
		m := dtsync.Message{}
		if err = m.UnmarshalCBOR(bytes.NewBuffer(msg.Data)); err != nil {
			log.Errorw("Could not decode pubsub message", "err", err)
			continue
		}

		// Add the message originator's address to the peerstore.  This allows
		// a connection, back to that publisher that sent the message, to
		// retrieve advertisements.
		var addrs []multiaddr.Multiaddr
		if len(m.Addrs) != 0 {
			addrs, err = m.GetAddrs()
			if err != nil {
				log.Errorw("Could not decode pubsub message", "err", err)
				continue
			}
		}

		log.Infow("Handling pubsub announce", "peer", srcPeer)

		err = s.Announce(ctx, m.Cid, srcPeer, addrs)
		if err != nil {
			log.Errorw("Cannot process message", "err", err)
			continue
		}
	}

	close(s.watchDone)
}

func (s *Subscriber) Announce(ctx context.Context, nextCid cid.Cid, peerID peer.ID, peerAddrs []multiaddr.Multiaddr) error {
	hnd, err := s.getOrCreateHandler(peerID, false)
	if err != nil {
		if err == errSourceNotAllowed {
			log.Infow("Ignored announcement", "reason", err, "peer", peerID)
			return nil
		}
		return err
	}

	limiter := s.limiterFor(peerID)
	if !limiter.Allow() {
		log.Infow("Ignoring announcement because of rate limiting", "peer", peerID)
		return nil
	}

	var httpAddr multiaddr.Multiaddr
	if len(peerAddrs) == 0 {
		// Check if we have an http url for this peer since we didn't get a peerAddr.
		// Note that this gives a preference to use httpSync over dtsync if we have
		// seen http address and we called sync with no explicit peerAddr.
		possibleAddrs := s.httpPeerstore.Addrs(peerID)
		if len(possibleAddrs) > 0 {
			httpAddr = possibleAddrs[0]
		}
	} else {
	httpFound:
		for _, addr := range peerAddrs {
			for _, p := range addr.Protocols() {
				if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
					httpAddr = addr
					break httpFound
				}
			}
		}
	}

	var syncer Syncer
	if httpAddr != nil {
		syncer, err = s.httpSync.NewSyncer(peerID, httpAddr, limiter)
		if err != nil {
			return fmt.Errorf("cannot create http sync handler: %w", err)
		}
	} else {
		// Not an httpPeerAddr, so use the dtSync. We'll add it with a small TTL
		// first, and extend it when we discover we can actually sync from it.
		// In case the peerstore already has this address and the existing TTL is
		// greater than this temp one, this is a no-op. In other words we never
		// decrease the TTL here.
		peerStore := s.host.Peerstore()
		if peerStore != nil && len(peerAddrs) != 0 {
			peerStore.AddAddrs(peerID, peerAddrs, s.addrTTL)
		}
		syncer = s.dtSync.NewSyncer(peerID, s.topicName, limiter)
	}

	// Start new goroutine to handle this message instead of having
	// persistent goroutine for each peer.
	hnd.handleAsync(ctx, nextCid, syncer)

	return nil
}

// handleAsync starts a goroutine to process the latest announce message
// received over pubsub or HTTP.  If there is already a goroutine handling a
// sync, then there will be at most one more goroutine waiting to handle the
// pending sync.
func (h *handler) handleAsync(ctx context.Context, nextCid cid.Cid, syncer Syncer) {
	h.qlock.Lock()
	// If pendingSync is undef, then previous goroutine has already handled any
	// pendingSync, so start a new go routine to handle the pending sync.  If
	// pending sync is not undef, then there is an existing goroutine that has
	// not yet handled the pending sync.
	if h.pendingCid == cid.Undef {
		h.subscriber.asyncWG.Add(1)
		go func() {
			h.latestSyncMu.Lock()
			defer h.latestSyncMu.Unlock()
			defer h.subscriber.asyncWG.Done()

			h.qlock.Lock()
			c := h.pendingCid
			h.pendingCid = cid.Undef
			syncer := h.pendingSyncer
			h.pendingSyncer = nil
			h.qlock.Unlock()

			// Wait for this handler to become available.
			// Note this only wraps the handler. This is to free up the handler in
			// case someone else needs it while we wait to send on the events chan.
			syncedCids, err := h.handle(ctx, c, h.subscriber.dss, true, syncer, h.subscriber.generalBlockHook, h.subscriber.segDepthLimit)
			if err != nil {
				// Log error for now.
				log.Errorw("Cannot process message", "err", err, "peer", h.peerID)
				return
			}

			// Update latest head seen.
			log.Infow("Updating latest sync")
			h.subscriber.latestSyncHander.SetLatestSync(h.peerID, c)
			h.subscriber.inEvents <- SyncFinished{Cid: c, PeerID: h.peerID, SyncedCids: syncedCids}
		}()
	} else {
		log.Infow("Pending update replaced by new", "previous_cid", h.pendingCid, "new_cid", nextCid)
	}
	h.pendingCid = nextCid
	h.pendingSyncer = syncer
	h.qlock.Unlock()
}

var _ SegmentSyncActions = (*segmentedSync)(nil)

type (
	// SegmentSyncActions allows the user to control the flow of segmented sync by either choosing
	// which CID should be synced in the next sync cycle or setting the error that should mark the
	// sync as failed.
	SegmentSyncActions interface {
		// SetNextSyncCid sets the cid that will be synced in the next segmented sync. Note that
		// the last call to this function during a segmented sync cycle dictates which CID will be
		// synced in the next cycle.
		//
		// At least one call to this function must be made for the segmented sync cycles to continue.
		// Because, otherwise the CID that should be used in the next segmented sync cycle cannot
		// be known.
		//
		// If no calls are made to this function or next CID is set to cid.Undef, the sync will
		// terminate and any CIDs that are synced so far will be included in a SyncFinished event.
		SetNextSyncCid(cid.Cid)

		// FailSync fails the sync and returns the given error as soon as the current segment sync
		// finishes. Note that the last call to this function during a segmented sync cycle takes
		// dictates the error value.
		// Passing nil as error will cancel sync failure.
		FailSync(error)
	}
	// SegmentBlockHookFunc is called for each synced block, similarly to BlockHookFunc. Except that
	// it provides SegmentSyncActions to the hook allowing the user to contl the flow of segmented
	// sync by determining which CID should be used in the next segmented sync cycle by decoding the
	// synced block.
	// SegmentSyncActions also allows the user to signal any errors that may occur during the hook
	// execution to terminate the sync and mark it as failed.
	SegmentBlockHookFunc func(peer.ID, cid.Cid, SegmentSyncActions)
	segmentedSync        struct {
		nextSyncCid *cid.Cid
		err         error
	}
)

func (ss *segmentedSync) SetNextSyncCid(c cid.Cid) {
	ss.nextSyncCid = &c
}

func (ss *segmentedSync) FailSync(err error) {
	ss.err = err
}

func (ss *segmentedSync) reset() {
	ss.nextSyncCid = nil
	ss.err = nil
}

// handle processes a message from the peer that the handler is responsible for.
func (h *handler) handle(ctx context.Context, nextCid cid.Cid, sel ipld.Node, wrapSel bool, syncer Syncer, bh BlockHookFunc, segdl int64) ([]cid.Cid, error) {
	h.syncMutex.Lock()
	defer h.syncMutex.Unlock()
	log := log.With("cid", nextCid, "peer", h.peerID)

	segSync := &segmentedSync{
		nextSyncCid: &nextCid,
	}

	var syncedCids []cid.Cid
	hook := func(p peer.ID, c cid.Cid) {
		syncedCids = append(syncedCids, c)
		if bh != nil {
			bh(p, c, segSync)
		}
	}
	h.subscriber.scopedBlockHookMutex.Lock()
	h.subscriber.scopedBlockHook[h.peerID] = hook
	h.subscriber.scopedBlockHookMutex.Unlock()
	defer func() {
		h.subscriber.scopedBlockHookMutex.Lock()
		delete(h.subscriber.scopedBlockHook, h.peerID)
		h.subscriber.scopedBlockHookMutex.Unlock()
	}()

	if wrapSel {
		var latestSyncLink ipld.Link
		latestSync, ok := h.subscriber.latestSyncHander.GetLatestSync(h.peerID)
		if ok && latestSync != cid.Undef {
			latestSyncLink = cidlink.Link{Cid: latestSync}
		}
		sel = ExploreRecursiveWithStopNode(h.subscriber.syncRecLimit, sel, latestSyncLink)
	}

	stopNode, stopNodeOK := getStopNode(sel)
	if stopNodeOK && stopNode.(cidlink.Link).Cid == nextCid {
		log.Infow("cid to sync to is the stop node. Nothing to do")
		return nil, nil
	}

	var syncBySegment bool
	var origLimit selector.RecursionLimit
	// Only attempt to detect recursion limit in original selector if maximum segment depth is
	// larger than zero and there is a block hook set; either general or scoped.
	//
	// Not that we need at least one block hook to let the caller decide which CID to sync in next
	// segment. Therefore, it has to be set for segmented sync to function correctly.
	if segdl > 0 && bh != nil {
		origLimit, syncBySegment = getRecursionLimit(sel)
		// If the given selector has a incursion Do not sync using segments if the depth limit in the selector is already less than the
		// configured maximum segment depth limit.
		if syncBySegment &&
			origLimit.Mode() == selector.RecursionLimit_Depth &&
			origLimit.Depth() <= segdl {
			syncBySegment = false
		}
	}

	// Revert back to sync without segmentation if original limit was not detected, due to:
	// - segment depth limit being negative; meaning segmentation is explicitly disabled, or
	// - no block hook is configured; meaning we don't have a way to determine next
	//   CID during segmented sync,
	// - the original selector does not explore recursively, and therefore, has no top level
	//   recursion limit, or
	// - tje original selector has a recursion depth limit that is already less than the maximum
	//   segment depth limit.
	if !syncBySegment {
		log.Debugw("Falling back on sync in one go", "segDepthLimit", segdl)
		err := syncer.Sync(ctx, nextCid, sel)
		if err != nil {
			return nil, err
		}
		log.Infow("Sync completed")
		return syncedCids, nil
	}

	var nextDepth = segdl
	var depthSoFar int64

SegSyncLoop:
	for {
		segmentSel, ok := withRecursionLimit(sel, selector.RecursionLimitDepth(nextDepth))
		if !ok {
			// This should not happen if we were able to extract origLimit from sel.
			// If this happens there is likely a bug. Fail fast.
			return nil, fmt.Errorf("failed to construct segment selector with recursion depth limit of %d", nextDepth)
		}
		nextCid = *segSync.nextSyncCid
		segSync.reset()
		err := syncer.Sync(ctx, nextCid, segmentSel)
		if err != nil {
			return nil, err
		}
		depthSoFar += nextDepth

		if segSync.err != nil {
			return nil, segSync.err
		}

		// If hook action is not called, or next CID is set to cid.Undef then break out of the
		// segmented sync cycle.
		if segSync.nextSyncCid == nil || segSync.nextSyncCid == &cid.Undef {
			break
		}

		if stopNodeOK && stopNode.(cidlink.Link).Cid == *segSync.nextSyncCid {
			log.Debugw("Reached stop node in segmented sync; stopping.")
			break
		}

		switch origLimit.Mode() {
		case selector.RecursionLimit_None:
			continue
		case selector.RecursionLimit_Depth:
			if depthSoFar >= origLimit.Depth() {
				break SegSyncLoop
			}

			remainingDepth := origLimit.Depth() - depthSoFar
			if remainingDepth < segdl {
				nextDepth = remainingDepth
			}
		default:
			return nil, fmt.Errorf("unknown recursion limit mode: %v", origLimit.Mode())
		}
	}
	log.Infow("Segmented sync completed")
	return syncedCids, nil
}
