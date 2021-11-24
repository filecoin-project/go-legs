package legs

import (
	"context"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

var log = logging.Logger("go-legs")

type legSubscriber struct {
	updates chan cid.Cid
	topic   *pubsub.Topic
	dt      dt.Manager
	onClose func() error

	submtx sync.Mutex
	subs   []chan cid.Cid
	cancel context.CancelFunc

	hndmtx sync.RWMutex
	policy PolicyHandler
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
	selector ipld.Node) (LegSubscriber, error) {
	ss, err := newSimpleSetup(ctx, host, ds, lsys, topic)
	if err != nil {
		return nil, err
	}
	return newSubscriber(ctx, ss.dt, ss.t, ss.onClose, nil, selector)
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
	selector ipld.Node) (LegSubscriber, error) {
	ss, err := newSimpleSetup(ctx, host, ds, lsys, topic)
	if err != nil {
		return nil, err
	}
	l, err := newSubscriber(ctx, ss.dt, ss.t, ss.onClose, nil, selector)
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

func newSubscriber(ctx context.Context, dt dt.Manager, topic *pubsub.Topic, onClose func() error, policy PolicyHandler, dss ipld.Node) (*legSubscriber, error) {
	ls := &legSubscriber{
		dt:      dt,
		topic:   topic,
		onClose: onClose,
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
		return err
	}
	cctx, cancel := context.WithCancel(ctx)

	unsub := ls.dt.SubscribeToEvents(ls.onEvent)
	ls.cancel = func() {
		unsub()
		psub.Cancel()
		cancel()
	}

	go ls.watch(cctx, psub)
	go ls.distribute(cctx)
	return nil

}

// SetPolicyHandler sets a new policyHandler to leg subscription.
func (ls *legSubscriber) SetPolicyHandler(policy PolicyHandler) error {
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
	if event.Code == dt.FinishTransfer {
		// Now we share the data channel between different subscribers.
		// When we receive a FinishTransfer we need to check if it belongs
		// to us
		if ls.syncing == channelState.BaseCID() {
			ls.syncing = cid.Undef
			ls.updates <- channelState.BaseCID()
			// Update latest head seen.
			// NOTE: This is not persisted anywhere. Is the top-level user's
			// responsability to persist if needed to intialize a
			// partiallySynced subscriber.
			log.Debugw("Exchange finished, updating latest to  %s", channelState.BaseCID())
			ls.latestSync = cidlink.Link{Cid: channelState.BaseCID()}
			// This Unlocks the syncMutex that was locked in watch(),
			// refer to that function for the lock functionality.
			ls.syncmtx.Unlock()
		}
	}
}

func (ls *legSubscriber) watch(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// TODO: restart subscription.
			return
		}

		// Run policy from pubsub message to see if exchange needs to be run
		allow := true
		if ls.policy != nil {
			ls.hndmtx.RLock()
			allow, err = ls.policy(msg)
			if err != nil {
				log.Errorf("Error running policy: %v", err)
			}
			ls.hndmtx.RUnlock()
		}

		if allow {
			log.Debugf("Pubsub message received. Policy says we are allowed to process")
			src, err := peer.IDFromBytes(msg.From)
			if err != nil {
				continue
			}

			c, err := cid.Cast(msg.Data)
			if err != nil {
				log.Warnf("Couldn't cast CID from pubsub message")
				continue
			}
			v := Voucher{&c}

			// Locking latestSync to avoid data races by several updates
			// This sync is unlocked when the transfer is finished or if
			// there is an error in the channel.
			ls.syncmtx.Lock()
			log.Debugf("Starting data channel (cid: %s, latestSync: %s)", c, ls.latestSync)
			ls.syncing = c
			_, err = ls.dt.OpenPullDataChannel(ctx, src, &v, c,
				ExploreRecursiveWithStopNode(
					selector.RecursionLimitNone(),
					ls.dss,
					ls.latestSync))
			if err != nil {
				// Log error for now.
				log.Errorf("Error in data channel: %v", err)
				// There has been an error, no FinishTransfer event will be triggered,
				// so we need to release the lock here.
				ls.syncmtx.Unlock()

				// TODO: Should we retry if there's an error in the exchange?
			}
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
	err := ls.onClose()
	ls.cancel()
	return err
}

func (ls *legSubscriber) unlockOnce(ulOnce *sync.Once) {
	ulOnce.Do(ls.syncmtx.Unlock)
}

// Sync performs a one-off explicit sync from the given peer for a specific cid and updates the latest
// synced link to it.
//
// It is the responsibility of the caller to make sure the given CID appears after the latest sync
// in order to avid re-syncing of content that may have previously been synced.
//
// The selector sequence, ss, can optionally be specified to customize the selection sequence during traversal.
// If unspecified, the subscriber's default selector sequence is used.
//
// Note that the selector sequence is wrapped with a selector logic that will stop traversal when
// the latest synced link is reached. Therefore, it must only specify the selection sequence itself.
//
// See: ExploreRecursiveWithStopNode.
func (ls *legSubscriber) Sync(ctx context.Context, p peer.ID, c cid.Cid, ss ipld.Node) (<-chan cid.Cid, context.CancelFunc, error) {
	out := make(chan cid.Cid)
	v := Voucher{&c}
	var ulOnce sync.Once

	ls.syncmtx.Lock()

	unsub := ls.dt.SubscribeToEvents(ls.onSyncEvent(c, out, &ulOnce))

	// Fall back onto the default selector sequence if one is not given.
	if ss == nil {
		ss = ls.dss
	}
	// Construct a selector consistent with the way background selector is constructed in legSubscriber.watch
	s := ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), ss, ls.latestSync)
	_, err := ls.dt.OpenPullDataChannel(ctx, p, &v, c, s)
	if err != nil {
		log.Errorf("Error in data channel for sync: %v", err)
		ls.syncmtx.Unlock()
		return nil, nil, err
	}
	cncl := func() {
		unsub()
		close(out)
		// if the mutex is lock, unlock it. This may happen
		// if CancelFunc is called after the exchange has finished
		// and the lock has unlocked.
		// We need this sanity-check to avoid the mutex from being
		// unlocked twice
		ls.unlockOnce(&ulOnce)
	}
	return out, cncl, nil
}

func (ls *legSubscriber) onSyncEvent(c cid.Cid, out chan cid.Cid, ulOnce *sync.Once) func(dt.Event, dt.ChannelState) {
	return func(event dt.Event, channelState dt.ChannelState) {
		if event.Code == dt.FinishTransfer {
			if c == channelState.BaseCID() {
				out <- channelState.BaseCID()
				log.Debugw("Exchange finished, updating latest to  %s", channelState.BaseCID())
				// Update latest sync to the head we are proactively syncing to.
				// Beware! This means that if we sync to a CID which is before
				// the latest one it may require to resend content from the chain that
				// we already have. This should be handled by users.
				ls.latestSync = cidlink.Link{Cid: channelState.BaseCID()}
				ls.unlockOnce(ulOnce)
			}
		}
	}
}

// getLatestSync gets the latest synced link.
// This function is safe to call from multiple goroutines and exposed for testing purposes only.
func (ls *legSubscriber) getLatestSync() ipld.Link {
	ls.syncmtx.Lock()
	defer ls.syncmtx.Unlock()
	return ls.latestSync
}
