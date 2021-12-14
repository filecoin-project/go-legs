package dtsync

import (
	"context"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type legSubscriber struct {
	updates chan cid.Cid
	topic   *pubsub.Topic
	dt      dt.Manager
	onClose func() error
	host    host.Host

	submtx sync.Mutex
	subs   []chan cid.Cid
	cancel context.CancelFunc

	hndmtx sync.RWMutex
	policy legs.PolicyHandler
	dss    ipld.Node

	// syncmtx synchronizes read/write for latestSync and syncing
	syncmtx    sync.Mutex
	latestSync ipld.Link
	syncing    cid.Cid
}

// NewSubscriber creates a new leg subscriber listening to a specific pubsub topic
// with a specific policyHandle to determine when to perform exchanges
func NewSubscriber(ctx context.Context,
	host host.Host,
	ds datastore.Batching,
	lsys ipld.LinkSystem,
	topic string,
	selector ipld.Node) (legs.LegSubscriber, error) {
	ss, err := newSimpleSetup(ctx, host, ds, lsys, topic)
	if err != nil {
		return nil, err
	}
	return newSubscriber(ctx, ss.dt, ss.t, ss.onClose, host, nil, selector)
}

// NewSubscriberPartiallySynced creates a new leg subscriber with a specific latestSync.
// LegSubscribers don't persist their latestSync. With this, users are able to
// start a new subscriber with knowledge about what has already been synced.
func NewSubscriberPartiallySynced(
	ctx context.Context,
	host host.Host,
	ds datastore.Batching,
	lsys ipld.LinkSystem,
	topic string,
	latestSync cid.Cid,
	selector ipld.Node) (legs.LegSubscriber, error) {
	ss, err := newSimpleSetup(ctx, host, ds, lsys, topic)
	if err != nil {
		return nil, err
	}
	l, err := newSubscriber(ctx, ss.dt, ss.t, ss.onClose, host, nil, selector)
	if err != nil {
		return nil, err
	}
	l.syncmtx.Lock()
	defer l.syncmtx.Unlock()
	if latestSync != cid.Undef {
		l.latestSync = cidlink.Link{Cid: latestSync}
	}
	return l, nil
}

func newSubscriber(ctx context.Context, dt dt.Manager, topic *pubsub.Topic, onClose func() error, host host.Host, policy legs.PolicyHandler, dss ipld.Node) (*legSubscriber, error) {
	ls := &legSubscriber{
		dt:      dt,
		topic:   topic,
		onClose: onClose,
		host:    host,
		updates: make(chan cid.Cid, 1),
		subs:    make([]chan cid.Cid, 0),
		policy:  policy,
		dss:     dss,
	}

	// Start subscription
	err := ls.subscribe(ctx)
	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (ls *legSubscriber) subscribe(ctx context.Context) error {
	psub, err := ls.topic.Subscribe()
	if err != nil {
		log.Errorf("Faield to subscribe to topic %s: %s", ls.topic, err)
		return err
	}
	unsub := ls.dt.SubscribeToEvents(ls.onEvent)

	cctx, cancel := context.WithCancel(ctx)
	ls.cancel = func() {
		unsub()
		cancel()
		psub.Cancel()
	}

	go ls.watch(cctx, psub)
	go ls.distribute(cctx)
	return nil

}

// SetPolicyHandler sets a new policyHandler to leg subscription.
func (ls *legSubscriber) SetPolicyHandler(policy legs.PolicyHandler) error {
	ls.hndmtx.Lock()
	defer ls.hndmtx.Unlock()
	ls.policy = policy
	return nil
}

// SetLatestSync sets a new latestSync in case the subscriber has
func (ls *legSubscriber) SetLatestSync(c cid.Cid) error {
	ls.syncmtx.Lock()
	defer ls.syncmtx.Unlock()
	ls.latestSync = cidlink.Link{Cid: c}
	return nil
}

func (ls *legSubscriber) onEvent(event dt.Event, channelState dt.ChannelState) {
	baseCID := channelState.BaseCID()
	log.Debugf("DataTransfer event with base CID %s from %s to %s: %s %s",
		baseCID,
		channelState.Sender(),
		channelState.Recipient(),
		dt.Events[event.Code],
		event.Message)

	if event.Code == dt.FinishTransfer {
		// Now we share the data channel between different subscribers.
		// When we receive a FinishTransfer we need to check if it belongs
		// to us
		if ls.syncing == baseCID {
			ls.syncing = cid.Undef
			ls.updates <- baseCID
			// Update latest head seen.
			// NOTE: This is not persisted anywhere. Is the top-level user's
			// responsability to persist if needed to intialize a
			// partiallySynced subscriber.
			log.Debugw("Exchange finished, updating latest to  %s", baseCID)
			ls.latestSync = cidlink.Link{Cid: baseCID}
			// This Unlocks the syncMutex that was locked in watch(),
			// refer to that function for the lock functionality.
			ls.syncmtx.Unlock()
		}
	}
}

func (ls *legSubscriber) watch(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil || err == pubsub.ErrSubscriptionCancelled {
				log.Debug("Pubsub watch canceled")
			} else {
				log.Errorf("Error reading from pubsub: %s", err)
				// TODO: restart subscription.
			}
			return
		}

		// Run policy from pubsub message to see if exchange needs to be run.
		//
		// TODO: Change this to call callback to evaluate peer using policy
		// callback provided to go-legs.
		if ls.policy != nil {
			var allow bool
			ls.hndmtx.RLock()
			allow, err = ls.policy(msg)
			if err != nil {
				log.Errorf("Error running policy: %v", err)
			}
			ls.hndmtx.RUnlock()
			if !allow {
				log.Infof("Message from peer %s not allowed by legs policy", msg.GetFrom())
				continue
			}
		}

		log.Debugf("Pubsub message received. Policy says we are allowed to process")
		src := msg.GetFrom()

		// Decode cid and originator addresses from message.
		m, err := DecodeMessage(msg.GetData())
		if err != nil {
			log.Errorf("Failed to decode pubsub message: %s", err)
			continue
		}
		c := m.Cid
		v := Voucher{&c}

		// Add the message originator's address to the peerstore.  This
		// allows a connection, back to that provider that sent the
		// message, to retrieve advertisements.
		peerStore := ls.host.Peerstore()
		if peerStore != nil {
			peerStore.AddAddrs(src, m.Addrs, peerstore.ProviderAddrTTL)
			log.Debugf("Added multiaddr %v for peer ID %s", m.Addrs, src)
		}

		// Locking latestSync to avoid data races by several updates
		// This sync is unlocked when the transfer is finished or if
		// there is an error in the channel.
		ls.syncmtx.Lock()
		log.Debugf("Starting data channel to %s for cid %s with latest synced %s", src, c, ls.latestSync)
		ls.syncing = c
		_, err = ls.dt.OpenPullDataChannel(ctx, src, &v, c,
			legs.ExploreRecursiveWithStopNode(
				selector.RecursionLimitNone(),
				ls.dss,
				ls.latestSync))
		if err != nil {
			// Log error for now.
			log.Errorf("Failed to open data channel to %s for cid %s with latest synced %s: %s", src, c, ls.latestSync, err)
			// There has been an error, no FinishTransfer event will be triggered,
			// so we need to release the lock here.
			ls.syncmtx.Unlock()

			// TODO: Should we retry if there's an error in the exchange?
		}
	}
}

func (ls *legSubscriber) distribute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case nh := <-ls.updates:
			ls.submtx.Lock()
			for _, d := range ls.subs {
				d <- nh
			}
			ls.submtx.Unlock()
		}
	}
}

func (ls *legSubscriber) OnChange() (chan cid.Cid, context.CancelFunc) {
	ch := make(chan cid.Cid)
	ls.submtx.Lock()
	defer ls.submtx.Unlock()
	ls.subs = append(ls.subs, ch)
	cncl := func() {
		ls.submtx.Lock()
		defer ls.submtx.Unlock()
		for i, ca := range ls.subs {
			if ca == ch {
				ls.subs[i] = ls.subs[len(ls.subs)-1]
				ls.subs[len(ls.subs)-1] = nil
				ls.subs = ls.subs[:len(ls.subs)-1]
				close(ch)
				break
			}
		}
	}
	return ch, cncl
}

func (ls *legSubscriber) Close() error {
	ls.cancel()
	return ls.onClose()
}

func (ls *legSubscriber) unlockOnce(ulOnce *sync.Once) {
	ulOnce.Do(ls.syncmtx.Unlock)
}

// Sync performs a one-off explicit sync from the given peer for a specific cid.
//
// Both cid and selector are optional parameters.
//
// If no cid is specified, i.e. the given cid equals cid.Undef, then the latest head is fetched from
// the remote publisher and used instead.
//
// If no selector is specified, the default selector sequence is used, wrapped with a logic that
// stops the traversal upon encountering the current head. See: legs.ExploreRecursiveWithStopNode.
// Otherwise, the given selector is used directly, without any wrapping.
//
// Note that if both the CID and the selector are unspecified this function behaves exactly like the
// background sync process, performing an explicit sync cycle for the latest head, updating the
// current head upon successful resolution.
//
// Specifying either a CID or a selector will not update the current head. This allows the caller to
// sync parts of a DAG selectively without updating the internal reference to the current head.
func (ls *legSubscriber) Sync(ctx context.Context, p peer.ID, c cid.Cid, ss ipld.Node) (<-chan cid.Cid, context.CancelFunc, error) {
	var ulOnce *sync.Once
	var updateLatestSync bool
	if c == cid.Undef {
		log.Debug("No CID is specified; fetching latest head to use as CID")
		var err error
		c, err = head.QueryRootCid(ctx, ls.host, ls.topic.String(), p)
		if err != nil {
			log.Errorf("Failed to query latest head: %s", err)
			return nil, nil, err
		}
		if ss == nil {
			// Update the latestSync only if no CID and no selector given.
			updateLatestSync = true
			// Instantiate sync.Once to assure legSubscriber.syncmtx is unlocked exactly once.
			ulOnce = &sync.Once{}
			ls.syncmtx.Lock()
			ss = legs.ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), ls.dss, ls.latestSync)
			log.Debugf("Explicit sync will update latest sync %s on successuful resolution", ls.latestSync)
		}
	} else if ss == nil {
		// Fall back onto the default selector sequence if one is not given.
		// Note that if selector is specified it is used as is without any wrapping.
		// Construct a selector consistent with the way background selector is constructed in legSubscriber.watch
		latestSync := ls.LatestSync()
		ss = legs.ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), ls.dss, latestSync)
		log.Debugf("Using default selector in explicit sync stopping at latest sync %s", latestSync)
	}

	done := make(chan cid.Cid)
	unsub := ls.dt.SubscribeToEvents(ls.onSyncEvent(c, done, ulOnce))

	cancel := func() {
		unsub()
		close(done)
		if updateLatestSync {
			// Make sure legSubscriber.syncmtx is unlocked since it has been locked if we reach here.
			ls.unlockOnce(ulOnce)
		}
	}

	v := Voucher{&c}
	_, err := ls.dt.OpenPullDataChannel(ctx, p, &v, c, ss)
	if err != nil {
		log.Errorf("Failed to open pull data channel to %s for CID %s: %v", p, c, err)
		cancel()
		return nil, nil, err
	}
	return done, cancel, nil
}

func (ls *legSubscriber) onSyncEvent(c cid.Cid, out chan cid.Cid, ulOnce *sync.Once) func(dt.Event, dt.ChannelState) {
	return func(event dt.Event, channelState dt.ChannelState) {
		baseCID := channelState.BaseCID()
		log.Debugf("DataTransfer event with base CID %s from %s to %s: %s %s",
			baseCID,
			channelState.Sender(),
			channelState.Recipient(),
			dt.Events[event.Code],
			event.Message)
		if event.Code == dt.FinishTransfer {
			baseCID := baseCID
			if c == baseCID {
				out <- baseCID
				log.Debugw("Explicit sync finished transfer for cid: %s", baseCID)

				if ulOnce != nil {
					log.Debugw("Updating latestSync to: %s", baseCID)
					// Update the latest sync to the head we are proactively syncing to.
					// Beware! This means that if we sync to a CID, which is before
					// the latest one it may require resending content from the chain that
					// we already have. This should be handled by users.
					ls.latestSync = cidlink.Link{Cid: baseCID}
					ls.unlockOnce(ulOnce)
				}
			}
		}
	}
}

// LatestSync gets the latest synced link.
// This function is safe to call from multiple goroutines and exposed for testing purposes only.
func (ls *legSubscriber) LatestSync() ipld.Link {
	ls.syncmtx.Lock()
	defer ls.syncmtx.Unlock()
	return ls.latestSync
}
