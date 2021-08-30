package legs

import (
	"context"
	"sync"
	"sync/atomic"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

var log = logging.Logger("go-legs")

type legSubscriber struct {
	updates  chan cid.Cid
	transfer *LegTransport

	submtx sync.Mutex
	subs   []chan cid.Cid
	cancel context.CancelFunc

	hndmtx sync.RWMutex
	policy PolicyHandler

	syncmtx    sync.Mutex
	latestSync ipld.Link
	syncing    cid.Cid
}

// NewSubscriber creates a new leg subscriber listening to a specific pubsub topic
// with a specific policyHandle to determine when to perform exchanges
func NewSubscriber(ctx context.Context, dt *LegTransport,
	policy PolicyHandler) (LegSubscriber, error) {
	return newSubscriber(ctx, dt, policy)
}

// NewSubscriberPartiallySynced creates a new leg subscriber with a specific latestSync.
// LegSubscribers don't persist their latestSync. With this, users are able to
// start a new subscriber with knowledge about what has already been synced.
func NewSubscriberPartiallySynced(
	ctx context.Context, dt *LegTransport, policy PolicyHandler,
	latestSync cid.Cid) (LegSubscriber, error) {

	l, err := newSubscriber(ctx, dt, policy)
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

func newSubscriber(ctx context.Context, dt *LegTransport, policy PolicyHandler) (*legSubscriber, error) {

	ls := &legSubscriber{
		transfer: dt,
		updates:  make(chan cid.Cid, 5),
		subs:     make([]chan cid.Cid, 0),
		cancel:   nil,
		policy:   policy,
	}

	// Start subscription
	err := ls.subscribe(ctx)
	if err != nil {
		return nil, err
	}
	// Add refC to track how many subscribers are using the transport.
	dt.addRefc()

	return ls, nil
}

func (ls *legSubscriber) subscribe(ctx context.Context) error {
	psub, err := ls.transfer.topic.Subscribe()
	if err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)

	unsub := ls.transfer.t.SubscribeToEvents(ls.onEvent)
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
			src, err := peer.IDFromBytes(msg.From)
			if err != nil {
				continue
			}

			c, err := cid.Cast(msg.Data)
			if err != nil {
				continue
			}
			v := Voucher{&c}

			// Locking latestSync to avoid data races by several updates
			// This sync is unlocked when the transfer is finished or if
			// there is an error in the channel.
			ls.syncmtx.Lock()
			log.Debugf("Starting data channel (cid: %s, latestSync: %s)", c, ls.latestSync)
			ls.syncing = c
			_, err = ls.transfer.t.OpenPullDataChannel(ctx, src, &v, c, legSelector(ls.latestSync))
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

// LegSubscriber is an interface for watching a published dag.
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
				ls.subs = append(ls.subs[0:i], ls.subs[i+1:]...)
				close(ch)
				break
			}
		}
	}
	return ch, cncl
}

func (ls *legSubscriber) Close() error {
	atomic.AddInt32(ls.transfer.refc, -1)
	ls.cancel()
	return nil
}
