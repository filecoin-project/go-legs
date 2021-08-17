package legs

import (
	"context"
	"errors"
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
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type legSubscriber struct {
	ds     datastore.Datastore
	tmpDir string
	*pubsub.Topic
	isSubscribed bool
	updates      chan cid.Cid
	transfer     dt.Manager

	submtx sync.Mutex
	subs   []chan cid.Cid
	cancel context.CancelFunc

	hndmtx sync.RWMutex
	policy PolicyHandler
}

// NewSubscriber creates a new leg subscriber listening to a specific pubsub topic
func NewSubscriber(ctx context.Context, ds datastore.Batching, host host.Host, topic string, lsys ipld.LinkSystem) (LegSubscriber, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(ctx, gsnet, lsys)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	tmpDir := os.TempDir()

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

	return &legSubscriber{
		ds:       ds,
		tmpDir:   tmpDir,
		Topic:    t,
		transfer: dt,
		updates:  make(chan cid.Cid, 5),
		subs:     make([]chan cid.Cid, 0),
		cancel:   nil}, nil
}

// Subscribe will subscribe for update to a pubsub topic. Every update triggers
// the policyHandler and trigger handler with
// every DAG root update being synced. If we are already subscribed to the topic,
// subscribe updates the handler.
func (ls *legSubscriber) Subscribe(ctx context.Context, selector ipld.Node, policy PolicyHandler) error {
	if selector == nil {
		return errors.New("cannot subscribe without a selector")
	}
	// Set specified policy
	ls.SetPolicyHandler(policy)

	if !ls.isSubscribed {
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

		go ls.watch(cctx, psub, selector)
		go ls.distribute(cctx)
		ls.isSubscribed = true
	}

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
	}
}

func (ls *legSubscriber) watch(ctx context.Context, sub *pubsub.Subscription, sel ipld.Node) {
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
			_, err = ls.transfer.OpenPullDataChannel(ctx, src, &v, c, sel)
			if err != nil {
				// retry?
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
