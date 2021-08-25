package legs

import (
	"context"
	"io/ioutil"
	"os"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

var log = logging.Logger("go-legs")

type legSubscriber struct {
	ds     datastore.Datastore
	tmpDir string
	*pubsub.Topic
	updates  chan cid.Cid
	transfer dt.Manager

	submtx sync.Mutex
	subs   []chan cid.Cid
	cancel context.CancelFunc

	hndmtx sync.RWMutex
	policy PolicyHandler

	syncmtx    sync.Mutex
	latestSync ipld.Link
}

// NewSubscriber creates a new leg subscriber listening to a specific pubsub topic
// with a specific policyHandle to determine when to perform exchanges
func NewSubscriber(ctx context.Context, ds datastore.Batching, host host.Host,
	topic string, lsys ipld.LinkSystem,
	policy PolicyHandler) (LegSubscriber, error) {
	return newSubscriber(ctx, ds, host, topic, lsys, policy)
}

// NewSubscriberPartiallySynced creates a new leg subscriber with a specific latestSync.
// LegSubscribers don't persist their latestSync. With this, users are able to
// start a new subscriber with knowledge about what has already been synced.
func NewSubscriberPartiallySynced(
	ctx context.Context, ds datastore.Batching, host host.Host,
	topic string, lsys ipld.LinkSystem,
	policy PolicyHandler, latestSync cid.Cid) (LegSubscriber, error) {
	l, err := newSubscriber(ctx, ds, host, topic, lsys, policy)
	if err != nil {
		return nil, err
	}
	if latestSync != cid.Undef {
		l.latestSync = cidlink.Link{Cid: latestSync}
	}
	return l, nil
}

// TODO: Add a parameter or config to set the directory that the subscriber's
// tmpDir is created in
func newSubscriber(ctx context.Context, ds datastore.Batching, host host.Host, topic string, lsys ipld.LinkSystem, policy PolicyHandler) (*legSubscriber, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(ctx, gsnet, lsys)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	tmpDir, err := ioutil.TempDir("", "golegs-sub")
	if err != nil {
		return nil, err
	}

	dt, err := datatransfer.NewDataTransfer(ds, tmpDir, dtNet, tp)
	if err != nil {
		return nil, err
	}

	v := &Voucher{}
	lvr := &VoucherResult{}
	val := &legsValidator{}
	if err := dt.RegisterVoucherType(v, val); err != nil {
		return nil, err
	}
	if err := dt.RegisterVoucherResultType(lvr); err != nil {
		return nil, err
	}
	if err := dt.Start(ctx); err != nil {
		return nil, err
	}

	ls := &legSubscriber{
		ds:       ds,
		tmpDir:   tmpDir,
		Topic:    t,
		transfer: dt,
		updates:  make(chan cid.Cid, 5),
		subs:     make([]chan cid.Cid, 0),
		cancel:   nil,
		policy:   policy,
	}

	// Start subscription
	err = ls.subscribe(ctx)
	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (ls *legSubscriber) subscribe(ctx context.Context) error {
	psub, err := ls.Topic.Subscribe()
	if err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)

	unsub := ls.transfer.SubscribeToEvents(ls.onEvent)
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

func (ls *legSubscriber) onEvent(event dt.Event, channelState dt.ChannelState) {
	if event.Code == dt.FinishTransfer {
		ls.updates <- channelState.BaseCID()
		// Update latest head seen.
		// NOTE: This is not persisted anywhere. Is the top-level user's
		// responsability to persisted if needed to intialize a
		// partiallySynced subscriber.
		ls.latestSync = cidlink.Link{Cid: channelState.BaseCID()}
		// This Unlocks the syncMutex that was locked in watch(),
		// refer to that function for the lock functionality.
		ls.syncmtx.Unlock()
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
			_, err = ls.transfer.OpenPullDataChannel(ctx, src, &v, c, legSelector(ls.latestSync))
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

func (ls *legSubscriber) Close(ctx context.Context) error {
	ls.cancel()
	err := ls.transfer.Stop(ctx)
	err2 := os.RemoveAll(ls.tmpDir)
	err3 := ls.Topic.Close()
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	return err3
}
