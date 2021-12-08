package legs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// defaultAddrTTL is the default amount of time that addresses discovered from
// pubsub messages will remain in the peerstore.  This is twice the default
// provider poll interval.
const defaultAddrTTL = 48 * time.Hour

var ErrSourceNotAllowed = errors.New("message source not allowed by policy")

// PeerPolicyHandler is the function signature of a function given to LegBroker
// that the broker uses to determine whether to allow or reject messages
// originating from peer passed into the function.  Returning true indicates
// that messages from the peer are allowed, and false indicated they should be
// rejected.  Returning an error indicates that there was a problem evaluating
// the peer, and results in the messages being rejected.
type PeerPolicyHandler func(peerID peer.ID) (bool, error)

// LegBroker is a message broker for pubsub messages.  LegBroker creates a
// single pubsub subscriber that receives messages from a gossip pubsub topic,
// and creates a stateful message handler for each message source peer.  An
// optional external policy determines whether to allow or deny handling
// messages from specific peers.
//
// Messages to separate peers are handled concurrently, and multiple messages
// to the same peer are handled serially.  If a handler is busy handling a
// message, and more messages arrive for the same peer, then the last message
// replaces the previous unhandled message to avoid having to maintain queues
// of messages.  Handlers do not have persistent goroutines, but start a new
// goroutine to handle a single message.
type LegBroker struct {
	dtManager   dt.Manager
	gsExchange  graphsync.GraphExchange
	tmpDir      string
	unsubEvents dt.Unsubscribe

	host    host.Host
	psub    *pubsub.Subscription
	topic   *pubsub.Topic
	addrTTL time.Duration

	// dss captures the default selector sequence passed to ExploreRecursiveWithStopNode
	dss ipld.Node

	handlers      map[peer.ID]*handler
	handlersMutex sync.Mutex
	peerPolicy    PeerPolicyHandler

	// Map of CID of in-progress sync to sync done channel.
	syncDoneChans map[cid.Cid]chan<- struct{}
	syncDoneMutex sync.Mutex

	// inEvents is used to send a ChangeEvent from a peer handler to the
	// distributeEvents goroutine.
	inEvents chan ChangeEvent

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of a ChangeEvent to an OnChange reader.
	outEventsChans []chan ChangeEvent
	outEventsMutex sync.Mutex

	// cancelBroker cancels broker goroutines
	cancelBrkr context.CancelFunc
	// cancelAll cancels pubsub and datatransfer and broker goroutines
	cancelAll context.CancelFunc
}

// ChangeEvent notifies an OnChange reader that a specified peer completed a sync.
type ChangeEvent struct {
	Cid    cid.Cid
	PeerID peer.ID
}

// Defined tells whether the ChangeEvent holds any data.
func (ce ChangeEvent) Defined() bool {
	return ce.Cid.Defined()
}

// handler holds state that is specific to a peer
type handler struct {
	dtManager dt.Manager
	// distEvents is used to communicate a syncDone event back to the
	// LegBroker for distribution to OnChange readers.
	distEvents chan<- ChangeEvent
	latestSync ipld.Link
	msgChan    chan cid.Cid
	// peerID is the ID of the peer this handler is responsible for.
	peerID peer.ID
	// putSyncDone is a function supplied by the LegBroker for the handler
	// to give LegBroker the channel to send sync done notification to.
	putSyncDone func(cid.Cid, chan<- struct{})
	// syncMutex serializes the handling of individual syncs
	syncMutex sync.Mutex
}

// NewLegBroker creates a new LegBroker, and will process messages that are allowed by given peerPolicy.
func NewLegBroker(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, dss ipld.Node, addrTTL time.Duration, peerPolicy PeerPolicyHandler) (*LegBroker, error) {
	ctx, cancelAll := context.WithCancel(context.Background())

	pubsubTopic, err := makePubsub(ctx, host, topic)
	if err != nil {
		cancelAll()
		return nil, err
	}

	psub, err := pubsubTopic.Subscribe()
	if err != nil {
		cancelAll()
		return nil, err
	}

	dt, gs, tmpDir, err := makeDataTransfer(ctx, host, ds, lsys)
	if err != nil {
		cancelAll()
		return nil, err
	}

	if addrTTL == 0 {
		addrTTL = defaultAddrTTL
	}

	ctx, cancelBrkr := context.WithCancel(ctx)

	lb := &LegBroker{
		dtManager:  dt,
		host:       host,
		gsExchange: gs,
		tmpDir:     tmpDir,

		addrTTL:    addrTTL,
		psub:       psub,
		topic:      pubsubTopic,
		cancelBrkr: cancelBrkr,
		cancelAll:  cancelAll,

		peerPolicy: peerPolicy,
		handlers:   make(map[peer.ID]*handler),
		inEvents:   make(chan ChangeEvent, 1),
	}

	lb.unsubEvents = dt.SubscribeToEvents(lb.onEvent)

	// Start watcher to read pubsub messages.
	go lb.watch(ctx)
	// Start distributor to send ChangeEvent messages to interested parties.
	go lb.distributeEvents(ctx)

	return lb, nil
}

// GetLatestSync returns the latest synced CID for the specified peer. If there
// is not handler for the peer, then nil is returned.  This does not mean that
// no data is synced with that peer, it means that the broker does not know
// about it.  Calling Sync() first may be necessary.
func (lb *LegBroker) GetLatestSync(peerID peer.ID) ipld.Link {
	lb.handlersMutex.Lock()
	defer lb.handlersMutex.Unlock()

	hnd, ok := lb.handlers[peerID]
	if !ok {
		return nil
	}

	return hnd.latestSync
}

// SetLatestSync sets the latest synced CID for a specified peer.  If there is
// no handler for the peer one is created, regardless of policy.
func (lb *LegBroker) SetLatestSync(peerID peer.ID, latestSync cid.Cid) error {
	hnd, err := lb.getOrCreateHandler(peerID, true)
	if err != nil {
		return err
	}

	hnd.setLatestSync(latestSync)
	return nil
}

// SetPolicyHandler configures LegBroker with a function to evaluate whether to
// allow or reject messages from the specified peer.  This replaces any
// previously configured policy handler.
func (lb *LegBroker) SetPolicyHandler(policy PeerPolicyHandler) {
	lb.handlersMutex.Lock()
	defer lb.handlersMutex.Unlock()
	lb.peerPolicy = policy
}

// Close shuts down the broker.
func (lb *LegBroker) Close() error {
	lb.unsubEvents()
	lb.psub.Cancel()
	lb.cancelBrkr()

	// Dismiss any event readers.
	lb.outEventsMutex.Lock()
	for _, ch := range lb.outEventsChans {
		close(ch)
	}
	lb.outEventsChans = nil
	lb.outEventsMutex.Unlock()

	// Dismiss and handlers waiting completion of sync
	lb.syncDoneMutex.Lock()
	for c, ch := range lb.syncDoneChans {
		close(ch)
		delete(lb.syncDoneChans, c)
	}
	lb.syncDoneMutex.Unlock()

	// Shutdown datatransfer.
	lb.cancelAll()
	return nil
}

// OnChange creates a channel that receives change notifications, and adds that
// channel to the list of notification channels.
func (lb *LegBroker) OnChange() (<-chan ChangeEvent, context.CancelFunc) {
	// Channel is buffered to prevent distribute() from blocking if a reader is
	// not reading the channel immediately.
	ch := make(chan ChangeEvent, 1)
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

// Sync performs a one-off explicit sync with the given peer for a specific CID and updates the latest
// synced link to it.
//
// The Context passed in controls the lifetime of the background sync process
// and not the call to sync, so make sure the context has an appropriate
// deadline, if any.
//
// A change event channel is returned to allow the caller to wait for this particular sync
// to complete.  Any OnChange watchers will also get a ChangeEvent when the sync succeeds.
//
// It is the responsibility of the caller to make sure the given CID appears after the latest sync
// in order to avid re-syncing of content that may have previously been synced.
//
// The selector sequence, selSec, can optionally be specified to customize the
// selection sequence during traversal.  If unspecified, the subscriber's
// default selector sequence is used.
//
// Note that the selector sequence is wrapped with a selector logic that will stop traversal when
// the latest synced link is reached. Therefore, it must only specify the selection sequence itself.
//
// See: ExploreRecursiveWithStopNode.
func (lb *LegBroker) Sync(ctx context.Context, peerID peer.ID, c cid.Cid, selSeq ipld.Node) (<-chan ChangeEvent, error) {
	if peerID == "" {
		return nil, errors.New("empty peer id")
	}

	// Check for existing handler.  If none, create on if allowed.
	hnd, err := lb.getOrCreateHandler(peerID, true)
	if err != nil {
		return nil, fmt.Errorf("cannot sync to peer: %s", err)
	}

	// Start goroutine to get latest root and handle
	out := make(chan ChangeEvent, 1)
	go func() {
		defer close(out)

		var updateLatest bool
		if c == cid.Undef {
			// Query the peer for the latest CID
			var err error
			c, err = head.QueryRootCid(ctx, lb.host, lb.topic.String(), peerID)
			if err != nil {
				log.Errorw("Cannot get root for sync", "err", err, "peer", peerID)
				return
			}
			log.Debugw("Sync queried root CID", "peer", peerID, "cid", c)
			updateLatest = true
		}

		if ctx.Err() != nil {
			log.Errorw("Sync canceled", "err", ctx.Err(), "peer", peerID)
			return
		}

		hnd.syncMutex.Lock()
		defer hnd.syncMutex.Unlock()

		var wrapSel bool
		if selSeq == nil {
			// Fall back onto the default selector sequence if one is not given.
			// Note that if selector is specified it is used as is without any wrapping.
			selSeq = lb.dss
			wrapSel = true
		}

		err := hnd.handle(ctx, c, selSeq, wrapSel, updateLatest)
		if err != nil {
			log.Errorw("Cannot sync", "err", err, "peer", peerID)
			return
		}

		out <- ChangeEvent{
			Cid:    c,
			PeerID: peerID,
		}
	}()

	return out, nil
}

// distributeEvents reads a ChangeEvent, sent by a peer handler, and copies the
// even to all channels in outEventsChans.  This delivers the ChangeEvent to
// all OnChange channel readers.
func (lb *LegBroker) distributeEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-lb.inEvents:
			if !event.Defined() {
				continue
			}
			// Send update to all change notification channels.
			lb.outEventsMutex.Lock()
			for _, ch := range lb.outEventsChans {
				ch <- event
			}
			lb.outEventsMutex.Unlock()
		}
	}
}

// getOrCreateHandler creates a handler for a specific peer
func (lb *LegBroker) getOrCreateHandler(peerID peer.ID, force bool) (*handler, error) {
	lb.handlersMutex.Lock()
	defer lb.handlersMutex.Unlock()

	// Check for existing handler, return if found.
	hnd, ok := lb.handlers[peerID]
	if ok {
		return hnd, nil
	}

	// If no handler, check policy to see if peer ID allowed.
	if lb.peerPolicy != nil && !force {
		allow, err := lb.peerPolicy(peerID)
		if err != nil {
			return nil, fmt.Errorf("cannot evaluate policy: %w", err)
		}
		if !allow {
			return nil, ErrSourceNotAllowed
		}
	}

	log.Infow("Policy allows message sender, creating new handler", "peer", peerID)
	hnd = &handler{
		dtManager:   lb.dtManager,
		msgChan:     make(chan cid.Cid, 1),
		peerID:      peerID,
		putSyncDone: lb.putSyncDoneChan,
		distEvents:  lb.inEvents,
	}
	lb.handlers[peerID] = hnd

	return hnd, nil
}

// watch reads messages from a pubsub topic subscription and passes the
// message to the handler that is responsible for the peer that originally sent
// the message.  If the handler does not yet exist, then the policy callback is
// consulted to determine if the indexer is allowing the peer.  If allowed, a
// new handler is created.  Otherwise, the message is ignored.
func (lb *LegBroker) watch(ctx context.Context) {
	for {
		msg, err := lb.psub.Next(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// TODO: restart subscription.
			return
		}

		srcPeer, err := peer.IDFromBytes(msg.From)
		if err != nil {
			continue
		}

		hnd, err := lb.getOrCreateHandler(srcPeer, false)
		if err != nil {
			if err == ErrSourceNotAllowed {
				log.Infow("Ignored message", "reason", err, "peer", srcPeer)
			} else {
				log.Errorw("Cannot process message", "err", err)
			}
			continue
		}

		// Decode CID and originator addresses from message.
		m, err := decodeMessage(msg.Data)
		if err != nil {
			log.Warnf("Could not decode pubsub message: %s", err)
			continue
		}

		// Add the message originator's address to the peerstore.  This
		// allows a connection, back to that provider that sent the
		// message, to retrieve advertisements.
		if len(m.addrs) != 0 {
			peerStore := lb.host.Peerstore()
			if peerStore != nil {
				peerStore.AddAddrs(srcPeer, m.addrs, lb.addrTTL)
			}
		}

		log.Infow("Handling message", "peer", srcPeer)

		// Start new goroutine to handle this message instead of having
		// persistent goroutine for each peer.
		hnd.handleAsync(ctx, m.cid, lb.dss)
	}
}

// putSyncDoneChan lets a handler give LegBroker the channel to send sync
// done notification to.  This is how LegBroker waits on a pending sync.
func (lb *LegBroker) putSyncDoneChan(c cid.Cid, syncDone chan<- struct{}) {
	lb.syncDoneMutex.Lock()
	if lb.syncDoneChans == nil {
		lb.syncDoneChans = make(map[cid.Cid]chan<- struct{})
	}
	lb.syncDoneChans[c] = syncDone
	lb.syncDoneMutex.Unlock()
}

// popSyncDone removes the channel when the pending sync has completed.
func (lb *LegBroker) popSyncDoneChan(c cid.Cid) (chan<- struct{}, bool) {
	lb.syncDoneMutex.Lock()
	defer lb.syncDoneMutex.Unlock()

	syncDone, ok := lb.syncDoneChans[c]
	if !ok {
		return nil, false
	}
	if len(lb.syncDoneChans) == 1 {
		lb.syncDoneChans = nil
	} else {
		delete(lb.syncDoneChans, c)
	}
	return syncDone, true
}

// onEvent is called by the datatransfer manager to send events.
func (lb *LegBroker) onEvent(event dt.Event, channelState dt.ChannelState) {
	if event.Code != dt.FinishTransfer {
		return
	}
	// Find the channel to notify the handler that the transfer is finished.
	syncDone, ok := lb.popSyncDoneChan(channelState.BaseCID())
	if !ok {
		log.Errorw("could not find channel for completed transfer notice", "cid", channelState.BaseCID())
		return
	}

	// Send the FinishTransfer signal to the handler.  This will allow its
	// handle goroutine to distribute the update and exit.
	close(syncDone)
}

// handleAsync starts a goroutine to process the latest message received over pubsub.
func (h *handler) handleAsync(ctx context.Context, c cid.Cid, ss ipld.Node) {
	// Remove any previous message and replace it with the most recent.
	select {
	case oldmsg := <-h.msgChan:
		log.Info("Message %s replaced by %s", oldmsg, c)
	default:
	}

	// Put new message on channel.  This is necessary so that if multiple
	// messages arrive while this handler is already syncing, that messages are
	// handled in order, regardless of goroutine scheduling.
	h.msgChan <- c

	go func() {
		// Wait for any previous messages to be handled.
		h.syncMutex.Lock()
		defer h.syncMutex.Unlock()

		select {
		case <-ctx.Done():
		case c := <-h.msgChan:
			err := h.handle(ctx, c, ss, true, true)
			if err != nil {
				// Log error for now.
				log.Errorw("Cannot process message", "err", err, "peer", h.peerID)
			}
		default:
			// A previous goroutine, that had its message replaced, read the
			// message.  Or, the message was removed and will be replaced by
			// another message and goroutine.  Either way, nothing to do in this
			// routine.
		}
	}()
}

// handle processes a message from the peer that the handler is responsible.
// The caller is responsible for ensuring that this is called while h.syncMutex
// is locked.
func (h *handler) handle(ctx context.Context, c cid.Cid, selSeq ipld.Node, wrapSel, updateLatest bool) error {
	v := Voucher{&c}

	log.Debugw("Starting data channel for message source", "cid", c, "latest_sync", h.latestSync, "source_peer", h.peerID)

	syncDone := make(chan struct{})
	h.putSyncDone(c, syncDone)

	if wrapSel {
		selSeq = ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), selSeq, h.latestSync)
	}
	_, err := h.dtManager.OpenPullDataChannel(ctx, h.peerID, &v, c, selSeq)
	if err != nil {
		return fmt.Errorf("cannot open data channel: %s", err)
		// TODO: Should we retry if there's an error in the exchange?
	}

	// Wait for transfer finished signal.
	<-syncDone

	if !updateLatest {
		return nil
	}

	// Update latest head seen.
	// NOTE: This is not persisted anywhere. Is the top-level user's
	// responsibility to persist if needed to initialize a
	// partiallySynced subscriber.
	h.latestSync = cidlink.Link{Cid: c}
	log.Debugw("Exchange finished, updating latest to %s", c)

	// Tell the broker to distribute ChangeEvent to all notification
	// destinations.
	h.distEvents <- ChangeEvent{Cid: c, PeerID: h.peerID}

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
