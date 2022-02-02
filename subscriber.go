package legs

import (
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
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("go-legs")

// defaultAddrTTL is the default amount of time that addresses discovered from
// pubsub messages will remain in the peerstore.  This is twice the default
// provider poll interval.
const defaultAddrTTL = 48 * time.Hour

// errSourceNotAllowed is the error returned when a message source peer's
// messages is not allowed to be processed.  This is only used internally, and
// pre-allocated here as it may occur frequently.
var errSourceNotAllowed = errors.New("message source not allowed")

// AllowPeerFunc is the signature of a function given to Subscriber that
// determines whether to allow or reject messages originating from a peer
// passed into the function.  Returning true or false indicates that messages
// from that peer are allowed rejected, respectively.  Returning an error
// indicates that there was a problem evaluating the function, and results in
// the messages being rejected.
type AllowPeerFunc func(peer.ID) (bool, error)

// BlockHookFunc is the signature of a function that is called when a received.
type BlockHookFunc func(peer.ID, cid.Cid)

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

	// A map of block hooks to call for a specific peer id, instead of the general
	// block hook func.
	scopedBlockHook      map[peer.ID]BlockHookFunc
	scopedBlockHookMutex *sync.RWMutex

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

	dtSync       *dtsync.Sync
	httpSync     *httpsync.Sync
	syncRecLimit selector.RecursionLimit
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
}

// handler holds state that is specific to a peer
type handler struct {
	subscriber *Subscriber
	latestSync ipld.Link
	msgChan    chan cid.Cid
	// peerID is the ID of the peer this handler is responsible for.
	peerID peer.ID
	// syncMutex serializes the handling of individual syncs
	syncMutex sync.Mutex
}

// wrapBlockHook wraps a possibly nil block hook func to allow a for dispatching
// to a blockhook func that is scoped within a .Sync call.
func wrapBlockHook(generalBlockHook BlockHookFunc) (*sync.RWMutex, map[peer.ID]BlockHookFunc, BlockHookFunc) {
	var scopedBlockHookMutex sync.RWMutex
	scopedBlockHook := make(map[peer.ID]BlockHookFunc)
	return &scopedBlockHookMutex, scopedBlockHook, func(peerID peer.ID, cid cid.Cid) {
		scopedBlockHookMutex.RLock()
		f, ok := scopedBlockHook[peerID]
		scopedBlockHookMutex.RUnlock()
		if ok {
			f(peerID, cid)
		} else if generalBlockHook != nil {
			generalBlockHook(peerID, cid)
		}
	}
}

// NewSubscriber creates a new Subscriber that process pubsub messages.
func NewSubscriber(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, dss ipld.Node, options ...Option) (*Subscriber, error) {
	cfg := config{
		addrTTL: defaultAddrTTL,
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

	scopedBlockHookMutex, scopedBlockHook, blockHook := wrapBlockHook(cfg.blockHook)

	var dtSync *dtsync.Sync
	if cfg.dtManager != nil {
		if ds != nil {
			log.Warn("Datastore cannot be used with DtManager option")
		}
		if cfg.blockHook != nil {
			log.Warn("BlockHook option cannot be used with DtManager option")
		}
		dtSync, err = dtsync.NewSyncWithDT(host, cfg.dtManager)
	} else {
		dtSync, err = dtsync.NewSync(host, ds, lsys, blockHook)
	}
	if err != nil {
		cancelPubsub()
		return nil, err
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

		scopedBlockHookMutex: scopedBlockHookMutex,
		scopedBlockHook:      scopedBlockHook,
	}

	// Start watcher to read pubsub messages.
	go s.watch(ctx)
	// Start distributor to send SyncFinished messages to interested parties.
	go s.distributeEvents()

	return s, nil
}

// GetLatestSync returns the latest synced CID for the specified peer. If there
// is not handler for the peer, then nil is returned.  This does not mean that
// no data is synced with that peer, it means that the Subscriber does not know
// about it.  Calling Sync() first may be necessary.
func (s *Subscriber) GetLatestSync(peerID peer.ID) ipld.Link {
	s.handlersMutex.Lock()
	hnd, ok := s.handlers[peerID]
	if !ok {
		s.handlersMutex.Unlock()
		return nil
	}
	s.handlersMutex.Unlock()

	return hnd.getLatestSync()
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

	hnd.setLatestSync(latestSync)
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
func (s *Subscriber) Sync(ctx context.Context, peerID peer.ID, nextCid cid.Cid, sel ipld.Node, peerAddr multiaddr.Multiaddr) (cid.Cid, error) {
	return s.SyncWithHook(ctx, peerID, nextCid, sel, peerAddr, nil)
}

func (s *Subscriber) SyncWithHook(ctx context.Context, peerID peer.ID, nextCid cid.Cid, sel ipld.Node, peerAddr multiaddr.Multiaddr, hook BlockHookFunc) (cid.Cid, error) {
	if peerID == "" {
		return cid.Undef, errors.New("empty peer id")
	}

	log := log.With("peer", peerID)

	var err error
	var syncer Syncer

	// If address specified, then get URL for HTTP sync, or add multiaddr to peerstore.
	if peerAddr != nil {
		for _, p := range peerAddr.Protocols() {
			if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
				syncer, err = s.httpSync.NewSyncer(peerID, peerAddr)
				if err != nil {
					return cid.Undef, fmt.Errorf("cannot create http sync handler: %w", err)
				}
				break
			}
		}

		// Not HTTP, so add multiaddr to peerstore.
		if syncer == nil {
			peerStore := s.host.Peerstore()
			if peerStore != nil {
				peerStore.AddAddr(peerID, peerAddr, s.addrTTL)
			}
		}
	}

	// No syncer yet, so create datatransfer syncer.
	if syncer == nil {
		syncer = s.dtSync.NewSyncer(peerID, s.topicName)
	}

	var updateLatest bool
	if nextCid == cid.Undef {
		// Query the peer for the latest CID
		nextCid, err = syncer.GetHead(ctx)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot query head for sync: %w", err)
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

	hnd.syncMutex.Lock()
	defer hnd.syncMutex.Unlock()

	if hook != nil {
		s.scopedBlockHookMutex.Lock()
		s.scopedBlockHook[peerID] = hook
		s.scopedBlockHookMutex.Unlock()
		defer func() {
			s.scopedBlockHookMutex.Lock()
			if s.scopedBlockHook == nil {
				s.scopedBlockHook = make(map[peer.ID]BlockHookFunc)
			}
			delete(s.scopedBlockHook, peerID)
			s.scopedBlockHookMutex.Unlock()
		}()
	}

	err = hnd.handle(ctx, nextCid, sel, wrapSel, updateLatest, syncer)
	if err != nil {
		return cid.Undef, fmt.Errorf("sync handler failed: %w", err)
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
		allow, err := s.allowPeer(peerID)
		if err != nil {
			return nil, fmt.Errorf("error checking if peer allowed: %w", err)
		}
		if !allow {
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
		msgChan:    make(chan cid.Cid, 1),
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
	watchWG := new(sync.WaitGroup)

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

		hnd, err := s.getOrCreateHandler(srcPeer, false)
		if err != nil {
			if err == errSourceNotAllowed {
				log.Infow("Ignored message", "reason", err, "peer", srcPeer)
			} else {
				log.Errorw("Cannot process message", "err", err)
			}
			continue
		}

		// Decode CID and originator addresses from message.
		m, err := dtsync.DecodeMessage(msg.Data)
		if err != nil {
			log.Errorw("Could not decode pubsub message", "err", err)
			continue
		}

		// Add the message originator's address to the peerstore.  This allows
		// a connection, back to that provider that sent the message, to
		// retrieve advertisements.
		if len(m.Addrs) != 0 {
			peerStore := s.host.Peerstore()
			if peerStore != nil {
				peerStore.AddAddrs(srcPeer, m.Addrs, s.addrTTL)
			}
		}

		log.Infow("Handling message", "peer", srcPeer)

		// Start new goroutine to handle this message instead of having
		// persistent goroutine for each peer.
		hnd.handleAsync(ctx, m.Cid, s.dss, watchWG)
	}

	watchWG.Wait()
	close(s.watchDone)
}

// handleAsync starts a goroutine to process the latest message received over
// pubsub.
func (h *handler) handleAsync(ctx context.Context, nextCid cid.Cid, ss ipld.Node, watchWG *sync.WaitGroup) {
	watchWG.Add(1)
	// Remove any previous message and replace it with the most recent.  Only
	// process the most recent message regardless of how many arrive while
	// waiting for a previous sync.
	select {
	case prevCid := <-h.msgChan:
		log.Infow("Pending update replaced by new", "previous_cid", prevCid, "new_cid", nextCid)
	default:
	}
	h.msgChan <- nextCid

	go func() {
		defer watchWG.Done()
		// Wait for this handler to become available.
		h.syncMutex.Lock()
		defer h.syncMutex.Unlock()

		select {
		case <-ctx.Done():
		case c := <-h.msgChan:
			err := h.handle(ctx, c, ss, true, true, h.subscriber.dtSync.NewSyncer(h.peerID, h.subscriber.topicName))
			if err != nil {
				// Log error for now.
				log.Errorw("Cannot process message", "err", err, "peer", h.peerID)
			}
		default:
			// A previous goroutine, that had its message replaced, read the
			// message.  Or, the message was removed and will be replaced by
			// another message and goroutine.  Either way, nothing to do in
			// this routine.
		}
	}()
}

// handle processes a message from the peer that the handler is responsible for.
// The caller is responsible for ensuring that this is called while h.syncMutex
// is locked.
func (h *handler) handle(ctx context.Context, nextCid cid.Cid, sel ipld.Node, wrapSel, updateLatest bool, syncer Syncer) error {
	log := log.With("cid", nextCid, "peer", h.peerID)

	if wrapSel {
		// Note this branch is nested under wrapSel because wrapSel adds the
		// semantics that we stop when we hit the `h.latestSync` node. This is a
		// special case where we are starting at the stop node so we can just
		// return.
		if h.latestSync != nil && h.latestSync.(cidlink.Link).Cid == nextCid {
			// Nothing to do. We've already synced to this cid because we have it as h.latestSync.
			log.Infow("Already synced")
			return nil
		}

		sel = ExploreRecursiveWithStopNode(h.subscriber.syncRecLimit, sel, h.latestSync)
	}

	err := syncer.Sync(ctx, nextCid, sel)
	if err != nil {
		return err
	}
	log.Infow("Sync completed")

	if !updateLatest {
		return nil
	}

	// Update latest head seen.
	//
	// NOTE: This is not persisted anywhere. Is the top-level user's
	// responsibility to persist if needed to initialize a partially-synced
	// subscriber.
	h.latestSync = cidlink.Link{Cid: nextCid}
	log.Infow("Updating latest sync")

	// Tell the Subscriber to distribute SyncFinished to all notification
	// destinations.
	h.subscriber.inEvents <- SyncFinished{Cid: nextCid, PeerID: h.peerID}

	return nil
}

// setLatestSync sets this handler's latest synced CID to the one specified.
func (h *handler) setLatestSync(latestSync cid.Cid) {
	h.syncMutex.Lock()
	defer h.syncMutex.Unlock()

	if latestSync != cid.Undef {
		h.latestSync = cidlink.Link{Cid: latestSync}
	}
}

func (h *handler) getLatestSync() ipld.Link {
	h.syncMutex.Lock()
	defer h.syncMutex.Unlock()
	return h.latestSync
}
