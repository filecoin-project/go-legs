package legs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/hashicorp/go-multierror"
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

// errSourceNotAllowed is the error returned when a message source peer's
// messages is not allowed to be processed.  This is only used internally, and
// pre-allocated here as it may occur frequently.
var errSourceNotAllowed = errors.New("message source not allowed")

// AllowPeerFunc is the function signature of a function given to Broker that
// the broker uses to determine whether to allow or reject messages originating
// from peer passed into the function.  Returning true indicates that messages
// from the peer are allowed, and false indicated they should be rejected.
// Returning an error indicates that there was a problem evaluating the peer,
// and results in the messages being rejected.
type AllowPeerFunc func(peerID peer.ID) (bool, error)

// Broker is a message broker for pubsub messages.  Broker creates a single
// pubsub subscriber that receives messages from a gossip pubsub topic, and
// creates a stateful message handler for each message source peer.  An
// optional externally-defined AllowPeerFunc determines whether to allow or
// deny handling messages from specific peers.
//
// Messages from separate peers are handled concurrently, and multiple messages
// from the same peer are handled serially.  If a handler is busy handling a
// message, and more messages arrive from the same peer, then the last message
// replaces the previous unhandled message to avoid having to maintain queues
// of messages.  Handlers do not have persistent goroutines, but start a new
// goroutine to handle a single message.
type Broker struct {
	gsExchange graphsync.GraphExchange
	// dss captures the default selector sequence passed to
	// ExploreRecursiveWithStopNode.
	dss         ipld.Node
	dtManager   dt.Manager
	tmpDir      string
	unsubEvents dt.Unsubscribe

	addrTTL time.Duration
	host    host.Host
	psub    *pubsub.Subscription
	topic   *pubsub.Topic

	allowPeer     AllowPeerFunc
	handlers      map[peer.ID]*handler
	handlersMutex sync.Mutex

	// Map of CID of in-progress sync to sync done channel.
	syncDoneChans map[cid.Cid]chan<- struct{}
	syncDoneMutex sync.Mutex

	// inEvents is used to send a SyncFinished from a peer handler to the
	// distributeEvents goroutine.
	inEvents chan SyncFinished

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of a SyncFinished to an OnSyncFinished reader.
	outEventsChans []chan SyncFinished
	outEventsMutex sync.Mutex

	// closing signals that the Broker is closing.
	closing chan struct{}
	// cancel sub-services (pubsub, datatransfer).
	cancelAll context.CancelFunc
	// closeOnde ensures that the Close only happens once.
	closeOnce sync.Once
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
	dtManager dt.Manager
	// distEvents is used to communicate a syncDone event back to the Broker
	// for distribution to OnSyncFinished readers.
	distEvents chan<- SyncFinished
	latestSync ipld.Link
	msgChan    chan cid.Cid
	// peerID is the ID of the peer this handler is responsible for.
	peerID peer.ID
	// putSyncDone is a function supplied by the Broker for the handler to give
	// Broker the channel to send sync done notification to.
	putSyncDone func(cid.Cid, chan<- struct{})
	// syncMutex serializes the handling of individual syncs
	syncMutex sync.Mutex
}

// NewBroker creates a new Broker that process pubsub messages.
func NewBroker(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, dss ipld.Node, options ...Option) (*Broker, error) {
	cfg := config{
		addrTTL: defaultAddrTTL,
	}
	err := cfg.apply(options)
	if err != nil {
		panic(err.Error())
	}

	ctx, cancelAll := context.WithCancel(context.Background())

	pubsubTopic := cfg.topic
	if pubsubTopic == nil {
		pubsubTopic, err = makePubsub(ctx, host, topic)
		if err != nil {
			cancelAll()
			return nil, err
		}
	}
	psub, err := pubsubTopic.Subscribe()
	if err != nil {
		cancelAll()
		return nil, err
	}

	dt, gs, tmpDir, err := makeDataTransfer(ctx, host, ds, lsys, cfg.dtManager)
	if err != nil {
		cancelAll()
		return nil, err
	}

	lb := &Broker{
		dtManager:  dt,
		dss:        dss,
		host:       host,
		gsExchange: gs,
		tmpDir:     tmpDir,

		addrTTL:   cfg.addrTTL,
		psub:      psub,
		topic:     pubsubTopic,
		closing:   make(chan struct{}),
		cancelAll: cancelAll,

		allowPeer: cfg.allowPeer,
		handlers:  make(map[peer.ID]*handler),
		inEvents:  make(chan SyncFinished, 1),
	}

	lb.unsubEvents = dt.SubscribeToEvents(lb.onEvent)

	// Start watcher to read pubsub messages.
	go lb.watch(ctx)
	// Start distributor to send SyncFinished messages to interested parties.
	go lb.distributeEvents()

	return lb, nil
}

// GetLatestSync returns the latest synced CID for the specified peer. If there
// is not handler for the peer, then nil is returned.  This does not mean that
// no data is synced with that peer, it means that the broker does not know
// about it.  Calling Sync() first may be necessary.
func (lb *Broker) GetLatestSync(peerID peer.ID) ipld.Link {
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
func (lb *Broker) SetLatestSync(peerID peer.ID, latestSync cid.Cid) error {
	hnd, err := lb.getOrCreateHandler(peerID, true)
	if err != nil {
		return err
	}

	hnd.setLatestSync(latestSync)
	return nil
}

// SetAllowPeer configures Broker with a function to evaluate whether to allow
// or reject messages from the specified peer.  Setting nil removes any
// filtering and allows messages from all peers.  This replaces any previously
// configured AllowPeerFunc.
func (lb *Broker) SetAllowPeer(allowPeer AllowPeerFunc) {
	lb.handlersMutex.Lock()
	defer lb.handlersMutex.Unlock()
	lb.allowPeer = allowPeer
}

// Close shuts down the broker.
func (lb *Broker) Close() error {
	var err error
	lb.closeOnce.Do(func() {
		err = lb.doClose()
	})
	return err
}

func (lb *Broker) doClose() error {
	lb.unsubEvents()
	lb.psub.Cancel()

	var errs error
	err := lb.dtManager.Stop(context.Background())
	if err != nil {
		log.Errorf("Failed to stop datatransfer manager: %s", err)
		errs = multierror.Append(errs, err)
	}
	if lb.tmpDir != "" {
		if err = os.RemoveAll(lb.tmpDir); err != nil {
			log.Errorf("Failed to remove temp dir: %s", err)
			errs = multierror.Append(errs, err)
		}
	}
	if err = lb.topic.Close(); err != nil {
		log.Errorf("Failed to close pubsub topic: %s", err)
		errs = multierror.Append(errs, err)
	}

	// Dismiss any event readers.
	lb.outEventsMutex.Lock()
	for _, ch := range lb.outEventsChans {
		close(ch)
	}
	lb.outEventsChans = nil
	lb.outEventsMutex.Unlock()

	// Dismiss any handlers waiting completion of sync.
	lb.syncDoneMutex.Lock()
	if len(lb.syncDoneChans) != 0 {
		log.Warnf("Closing broker with %d syncs in progress", len(lb.syncDoneChans))
	}
	for _, ch := range lb.syncDoneChans {
		close(ch)
	}
	lb.syncDoneChans = nil
	lb.syncDoneMutex.Unlock()

	// Shutdown sub-services.
	lb.cancelAll()

	// Stop the distribution goroutine.
	close(lb.inEvents)
	return errs
}

// OnSyncFinished creates a channel that receives change notifications, and
// adds that channel to the list of notification channels.
//
// Calling the returned cancel function removed the notification channel from
// those the receive change notifications, and closes the channel to allow any
// waiting goroutines to stop waiting on the channel.  For this reason, channel
// readers should check for channel closure.
func (lb *Broker) OnSyncFinished() (<-chan SyncFinished, context.CancelFunc) {
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
// the default selector.
//
// It is the responsibility of the caller to make sure the given CID appears
// after the latest sync in order to avid re-syncing of content that may have
// previously been synced.
//
// The selector sequence, selSec, can optionally be specified to customize the
// selection sequence during traversal.  If unspecified, the subscriber's
// default selector sequence is used.
//
// Note that the selector sequence is wrapped with a selector logic that will
// stop traversal when the latest synced link is reached. Therefore, it must
// only specify the selection sequence itself.
//
// See: ExploreRecursiveWithStopNode.
func (lb *Broker) Sync(ctx context.Context, peerID peer.ID, c cid.Cid, selSeq ipld.Node) (<-chan SyncFinished, error) {
	if peerID == "" {
		return nil, errors.New("empty peer id")
	}

	// Check for existing handler.  If none, create on if allowed.
	hnd, err := lb.getOrCreateHandler(peerID, true)
	if err != nil {
		return nil, fmt.Errorf("cannot sync to peer: %s", err)
	}

	// Start goroutine to get latest root and handle
	out := make(chan SyncFinished, 1)
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
			if selSeq == nil {
				// Update the latestSync only if no CID and no selector given.
				updateLatest = true
			}
		}

		if ctx.Err() != nil {
			log.Errorw("Sync canceled", "err", ctx.Err(), "peer", peerID)
			return
		}

		var wrapSel bool
		if selSeq == nil {
			// Fall back onto the default selector sequence if one is not
			// given.  Note that if selector is specified it is used as is
			// without any wrapping.
			selSeq = lb.dss
			wrapSel = true
		}

		hnd.syncMutex.Lock()
		defer hnd.syncMutex.Unlock()

		err := hnd.handle(ctx, c, selSeq, wrapSel, updateLatest)
		if err != nil {
			log.Errorw("Cannot sync", "err", err, "peer", peerID)
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
func (lb *Broker) distributeEvents() {
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
func (lb *Broker) getOrCreateHandler(peerID peer.ID, force bool) (*handler, error) {
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

	log.Infow("Message sender allowed, creating new handler", "peer", peerID)
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

// watch reads messages from a pubsub topic subscription and passes the message
// to the handler that is responsible for the peer that originally sent the
// message.  If the handler does not yet exist, then the allowPeer callback is
// consulted to determine if the peer's messages are allowed.  If allowed, a
// new handler is created.  Otherwise, the message is ignored.
func (lb *Broker) watch(ctx context.Context) {
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
			if err == errSourceNotAllowed {
				log.Infow("Ignored message", "reason", err, "peer", srcPeer)
			} else {
				log.Errorw("Cannot process message", "err", err)
			}
			continue
		}

		// Decode CID and originator addresses from message.
		m, err := decodeMessage(msg.Data)
		if err != nil {
			log.Errorf("Could not decode pubsub message: %s", err)
			continue
		}

		// Add the message originator's address to the peerstore.  This allows
		// a connection, back to that provider that sent the message, to
		// retrieve advertisements.
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

// putSyncDoneChan lets a handler give Broker the channel to send sync done
// notification to.  This is how Broker waits on a pending sync.
func (lb *Broker) putSyncDoneChan(c cid.Cid, syncDone chan<- struct{}) {
	lb.syncDoneMutex.Lock()
	defer lb.syncDoneMutex.Unlock()

	if lb.syncDoneChans == nil {
		lb.syncDoneChans = make(map[cid.Cid]chan<- struct{})
	}
	lb.syncDoneChans[c] = syncDone
}

// popSyncDone removes the channel when the pending sync has completed.
func (lb *Broker) popSyncDoneChan(c cid.Cid) (chan<- struct{}, bool) {
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
func (lb *Broker) onEvent(event dt.Event, channelState dt.ChannelState) {
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
	//
	// It is not necessary to return the channelState CID, since we already
	// know it is the correct on since it was used to look up this syncDone
	// channel.
	close(syncDone)
}

// handleAsync starts a goroutine to process the latest message received over
// pubsub.
func (h *handler) handleAsync(ctx context.Context, c cid.Cid, ss ipld.Node) {
	// Remove any previous message and replace it with the most recent.
	select {
	case oldmsg := <-h.msgChan:
		log.Info("Message %s replaced by %s", oldmsg, c)
	default:
	}

	// Put new message on channel.  This is necessary so that if multiple
	// messages arrive while this handler is already syncing, the messages are
	// handled in order, regardless of goroutine scheduling.
	h.msgChan <- c

	go func() {
		// Wait for this handler to become available.
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
			// another message and goroutine.  Either way, nothing to do in
			// this routine.
		}
	}()
}

// handle processes a message from the peer that the handler is responsible.
// The caller is responsible for ensuring that this is called while h.syncMutex
// is locked.
func (h *handler) handle(ctx context.Context, c cid.Cid, selSeq ipld.Node, wrapSel, updateLatest bool) error {
	syncDone := make(chan struct{})
	h.putSyncDone(c, syncDone)

	if wrapSel {
		selSeq = ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), selSeq, h.latestSync)
	}
	log.Debugw("Starting data channel for message source", "cid", c, "latest_sync", h.latestSync, "source_peer", h.peerID)
	v := Voucher{&c}
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

	// Tell the broker to distribute SyncFinished to all notification
	// destinations.
	h.distEvents <- SyncFinished{Cid: c, PeerID: h.peerID}

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
