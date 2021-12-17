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
type AllowPeerFunc func(peerID peer.ID) (bool, error)

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
	lsys ipld.LinkSystem

	addrTTL   time.Duration
	psub      *pubsub.Subscription
	topic     *pubsub.Topic
	topicName string

	allowPeer     AllowPeerFunc
	handlers      map[peer.ID]*handler
	handlersMutex sync.Mutex

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
	// closeOnde ensures that the Close only happens once.
	closeOnce sync.Once

	dtSync   *dtsync.Sync
	httpSync *httpsync.Sync
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
	// distEvents is used to communicate a SyncFinished event back to the
	// Subscriber for distribution to OnSyncFinished readers.
	distEvents chan<- SyncFinished
	latestSync ipld.Link
	msgChan    chan cid.Cid
	// peerID is the ID of the peer this handler is responsible for.
	peerID peer.ID
	// syncMutex serializes the handling of individual syncs
	syncMutex sync.Mutex
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

	dtSync, err := dtsync.NewSync(host, ds, lsys, cfg.dtManager)
	if err != nil {
		cancelPubsub()
		return nil, err
	}

	lb := &Subscriber{
		dss:  dss,
		host: host,
		lsys: lsys,

		addrTTL:   cfg.addrTTL,
		psub:      psub,
		topic:     cfg.topic,
		topicName: cfg.topic.String(),
		closing:   make(chan struct{}),
		cancelps:  cancelPubsub,

		allowPeer: cfg.allowPeer,
		handlers:  make(map[peer.ID]*handler),
		inEvents:  make(chan SyncFinished, 1),

		dtSync:   dtSync,
		httpSync: httpsync.NewSync(lsys, cfg.httpClient),
	}

	// Start watcher to read pubsub messages.
	go lb.watch(ctx)
	// Start distributor to send SyncFinished messages to interested parties.
	go lb.distributeEvents()

	return lb, nil
}

// GetLatestSync returns the latest synced CID for the specified peer. If there
// is not handler for the peer, then nil is returned.  This does not mean that
// no data is synced with that peer, it means that the Subscriber does not know
// about it.  Calling Sync() first may be necessary.
func (lb *Subscriber) GetLatestSync(peerID peer.ID) ipld.Link {
	lb.handlersMutex.Lock()
	hnd, ok := lb.handlers[peerID]
	if !ok {
		lb.handlersMutex.Unlock()
		return nil
	}
	lb.handlersMutex.Unlock()

	return hnd.getLatestSync()
}

// SetLatestSync sets the latest synced CID for a specified peer.  If there is
// no handler for the peer, then one is created without consulting any
// AllowPeerFunc.
func (lb *Subscriber) SetLatestSync(peerID peer.ID, latestSync cid.Cid) error {
	if latestSync == cid.Undef {
		return errors.New("cannot set latest sync to undefined value")
	}
	hnd, err := lb.getOrCreateHandler(peerID, true)
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
func (lb *Subscriber) SetAllowPeer(allowPeer AllowPeerFunc) {
	lb.handlersMutex.Lock()
	defer lb.handlersMutex.Unlock()
	lb.allowPeer = allowPeer
}

// Close shuts down the Subscriber.
func (lb *Subscriber) Close() error {
	var err error
	lb.closeOnce.Do(func() {
		err = lb.doClose()
	})
	return err
}

func (lb *Subscriber) doClose() error {
	lb.psub.Cancel()

	var err, errs error
	if err = lb.dtSync.Close(); err != nil {
		errs = multierror.Append(errs, err)
	}

	// If Subscriber owns the pubsub topic, then close it.
	if lb.topic != nil {
		if err = lb.topic.Close(); err != nil {
			log.Errorf("Failed to close pubsub topic: %s", err)
			errs = multierror.Append(errs, err)
		}
	}

	// Dismiss any event readers.
	lb.outEventsMutex.Lock()
	for _, ch := range lb.outEventsChans {
		close(ch)
	}
	lb.outEventsChans = nil
	lb.outEventsMutex.Unlock()

	// Shutdown pubsub services.
	lb.cancelps()

	// Stop the distribution goroutine.
	close(lb.inEvents)
	return errs
}

// OnSyncFinished creates a channel that receives change notifications, and
// adds that channel to the list of notification channels.
//
// Calling the returned cancel function removes the notification channel from
// the list of channels to be notified on changes, and it closes the channel to
// allow any reading goroutines to stop waiting on the channel.
func (lb *Subscriber) OnSyncFinished() (<-chan SyncFinished, context.CancelFunc) {
	// Channel is buffered to prevent distribute() from blocking if a reader is
	// not reading the channel immediately.
	ch := make(chan SyncFinished, 1)
	lb.outEventsMutex.Lock()
	defer lb.outEventsMutex.Unlock()

	lb.outEventsChans = append(lb.outEventsChans, ch)
	cncl := func() {
		lb.outEventsMutex.Lock()
		defer lb.outEventsMutex.Unlock()
		for i, ca := range lb.outEventsChans {
			if ca == ch {
				lb.outEventsChans[i] = lb.outEventsChans[len(lb.outEventsChans)-1]
				lb.outEventsChans[len(lb.outEventsChans)-1] = nil
				lb.outEventsChans = lb.outEventsChans[:len(lb.outEventsChans)-1]
				close(ch)
				break
			}
		}
	}
	return ch, cncl
}

// Sync performs a one-off explicit sync with the given peer for a specific CID
// and updates the latest synced link to it.
//
// The Context passed in controls the lifetime of the background sync process,
// so make sure the context has an appropriate deadline, if any.
//
// A SyncFinished channel is returned to allow the caller to wait for this
// particular sync to complete.  Any OnSyncFinished readers will also get a
// SyncFinished when the sync succeeds, but only if syncing to the latest using
// the default selector and a `cid.Undef`.
//
// It is the responsibility of the caller to make sure the given CID appears
// after the latest sync in order to avid re-syncing of content that may have
// previously been synced.
//
// The selector sequence, selSec, can optionally be specified to customize the
// selection sequence during traversal.  If unspecified, the default selector
// sequence is used.
//
// Note that the selector sequence is wrapped with a selector logic that will
// stop traversal when the latest synced link is reached. Therefore, it must
// only specify the selection sequence itself.
//
// See: ExploreRecursiveWithStopNode.
func (lb *Subscriber) Sync(ctx context.Context, peerID peer.ID, c cid.Cid, sel ipld.Node, publisher multiaddr.Multiaddr) (<-chan SyncFinished, error) {
	if peerID == "" {
		return nil, errors.New("empty peer id")
	}

	log := log.With("peer", peerID)

	var err error
	var syncer Syncer

	log.Infow("Start sync", "cid", c)

	// If publisher specified, then get URL for http sync, or add multiaddr to peerstore.
	if publisher != nil {
		for _, p := range publisher.Protocols() {
			if p.Code == multiaddr.P_HTTP || p.Code == multiaddr.P_HTTPS {
				syncer, err = lb.httpSync.NewSyncer(publisher)
				if err != nil {
					return nil, err
				}
				break
			}
		}

		// Not http, so add multiaddr to peerstore.
		if syncer == nil {
			peerStore := lb.host.Peerstore()
			if peerStore != nil {
				peerStore.AddAddr(peerID, publisher, lb.addrTTL)
			}
		}
	}

	// No syncer yet, so create datatransfer syncer.
	if syncer == nil {
		syncer = lb.dtSync.NewSyncer(peerID, lb.topicName)
	}

	// Start goroutine to get latest root and handle
	out := make(chan SyncFinished, 1)
	go func() {
		defer close(out)

		var updateLatest bool
		if c == cid.Undef {
			// Query the peer for the latest CID
			c, err = syncer.GetHead(ctx)
			if err != nil {
				log.Errorw("Cannot get head for sync", "err", err)
				return
			}

			log.Debugw("Sync queried head CID", "cid", c)
			if sel == nil {
				// Update the latestSync only if no CID and no selector given.
				updateLatest = true
			}
		}

		if ctx.Err() != nil {
			log.Errorw("Sync canceled", "err", ctx.Err())
			return
		}

		var wrapSel bool
		if sel == nil {
			// Fall back onto the default selector sequence if one is not
			// given.  Note that if selector is specified it is used as is
			// without any wrapping.
			sel = lb.dss
			wrapSel = true
		}

		// Check for existing handler.  If none, create on if allowed.
		hnd, err := lb.getOrCreateHandler(peerID, true)
		if err != nil {
			log.Errorw("Cannot sync", "err", err)
			return
		}

		hnd.syncMutex.Lock()
		defer hnd.syncMutex.Unlock()

		err = hnd.handle(ctx, c, sel, wrapSel, updateLatest, syncer)
		if err != nil {
			log.Errorw("Cannot sync", "err", err)
			return
		}

		out <- SyncFinished{
			Cid:    c,
			PeerID: peerID,
		}
	}()

	return out, nil
}

// distributeEvents reads a SyncFinished, sent by a peer handler, and copies
// the even to all channels in outEventsChans.  This delivers the SyncFinished
// to all OnSyncFinished channel readers.
func (lb *Subscriber) distributeEvents() {
	for event := range lb.inEvents {
		if !event.Cid.Defined() {
			panic("SyncFinished event with undefined cid")
		}
		// Send update to all change notification channels.
		lb.outEventsMutex.Lock()
		for _, ch := range lb.outEventsChans {
			ch <- event
		}
		lb.outEventsMutex.Unlock()
	}
}

// getOrCreateHandler creates a handler for a specific peer
func (lb *Subscriber) getOrCreateHandler(peerID peer.ID, force bool) (*handler, error) {
	lb.handlersMutex.Lock()
	defer lb.handlersMutex.Unlock()

	// Check for existing handler, return if found.
	hnd, ok := lb.handlers[peerID]
	if ok {
		return hnd, nil
	}

	// If no handler, check callback to see if peer ID allowed.
	if lb.allowPeer != nil && !force {
		allow, err := lb.allowPeer(peerID)
		if err != nil {
			return nil, fmt.Errorf("error checking if peer allowed: %w", err)
		}
		if !allow {
			return nil, errSourceNotAllowed
		}
	}

	log.Infow("Creating new handler for publisher", "peer", peerID)
	hnd = &handler{
		subscriber: lb,
		msgChan:    make(chan cid.Cid, 1),
		peerID:     peerID,
		distEvents: lb.inEvents,
	}
	lb.handlers[peerID] = hnd

	return hnd, nil
}

// watch reads messages from a pubsub topic subscription and passes the message
// to the handler that is responsible for the peer that originally sent the
// message.  If the handler does not yet exist, then the allowPeer callback is
// consulted to determine if the peer's messages are allowed.  If allowed, a
// new handler is created.  Otherwise, the message is ignored.
func (lb *Subscriber) watch(ctx context.Context) {
	for {
		msg, err := lb.psub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil || err == pubsub.ErrSubscriptionCancelled {
				// This is a normal result of shutting down the Subscriber.
				log.Debug("Canceled watching pubsub subscription")
			} else {
				log.Errorf("Error reading from pubsub: %s", err)
				// TODO: restart subscription.
			}
			return
		}

		srcPeer, err := peer.IDFromBytes(msg.From)
		if err != nil {
			continue
		}

		hnd, err := lb.getOrCreateHandler(srcPeer, false)
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
			log.Errorf("Could not decode pubsub message: %s", err)
			continue
		}

		// Add the message originator's address to the peerstore.  This allows
		// a connection, back to that provider that sent the message, to
		// retrieve advertisements.
		if len(m.Addrs) != 0 {
			peerStore := lb.host.Peerstore()
			if peerStore != nil {
				peerStore.AddAddrs(srcPeer, m.Addrs, lb.addrTTL)
			}
		}

		log.Infow("Handling message", "peer", srcPeer)

		// Start new goroutine to handle this message instead of having
		// persistent goroutine for each peer.
		hnd.handleAsync(ctx, m.Cid, lb.dss)
	}
}

// handleAsync starts a goroutine to process the latest message received over
// pubsub.
func (h *handler) handleAsync(ctx context.Context, nextCid cid.Cid, ss ipld.Node) {
	// Remove any previous message and replace it with the most recent.
	select {
	case prevCid := <-h.msgChan:
		log.Infow("Penging update replaced by new", "previous_cid", prevCid, "new_cid", nextCid)
	default:
	}

	// Put new message on channel.  This is necessary so that if multiple
	// messages arrive while this handler is already syncing, the messages are
	// handled in order, regardless of goroutine scheduling.
	h.msgChan <- nextCid

	go func() {
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
	if wrapSel {
		sel = ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), sel, h.latestSync)
	}

	log.Debugw("Starting data channel for message source", "cid", nextCid, "latest_sync", h.latestSync, "source_peer", h.peerID)

	err := syncer.Sync(ctx, nextCid, sel)
	if err != nil {
		return err
	}

	if !updateLatest {
		return nil
	}

	// Update latest head seen.
	// NOTE: This is not persisted anywhere. Is the top-level user's
	// responsibility to persist if needed to initialize a
	// partiallySynced subscriber.
	h.latestSync = cidlink.Link{Cid: nextCid}
	log.Infow("Updating latest sync", "sync_cid", nextCid, "peer", h.peerID)

	// Tell the Subscriber to distribute SyncFinished to all notification
	// destinations.
	h.distEvents <- SyncFinished{Cid: nextCid, PeerID: h.peerID}

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
