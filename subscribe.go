package legs

import (
	"bytes"
	"context"
	"io"
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
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type legSubscriber struct {
	ds     datastore.Datastore
	tmpDir string
	*pubsub.Topic
	updates  chan cid.Cid
	transfer dt.Manager

	submtx sync.Mutex
	subs   []chan cid.Cid
	cancel context.CancelFunc
}

// Subscribe will sync an IPLD dag of data from a publisher
func Subscribe(ctx context.Context, dataStore datastore.Batching, host host.Host, topic string) (LegSubscriber, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	loader := func(lnk ipld.Link, _ ipld.LinkContext) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := dataStore.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	storer := func(_ ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return dataStore.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(ctx, gsnet, loader, storer)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	tmpDir := os.TempDir()

	dt, err := datatransfer.NewDataTransfer(dataStore, tmpDir, dtNet, tp)
	if err != nil {
		return nil, err
	}

	v := &LegsVoucher{}
	lvr := &LegsVoucherResult{}
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

	psub, err := t.Subscribe()
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithCancel(context.Background())

	sub := legSubscriber{
		ds:       dataStore,
		tmpDir:   tmpDir,
		Topic:    t,
		transfer: dt,
		updates:  make(chan cid.Cid, 5),
		subs:     make([]chan cid.Cid, 0),
		cancel:   nil}
	unsub := sub.transfer.SubscribeToEvents(sub.onEvent)
	sub.cancel = func() {
		unsub()
		psub.Cancel()
		cancel()
	}

	go sub.watch(cctx, psub)
	go sub.distribute(cctx)

	return &sub, nil
}

func (ls *legSubscriber) onEvent(event dt.Event, channelState dt.ChannelState) {
	if event.Code == dt.FinishTransfer {
		ls.updates <- channelState.BaseCID()
	}
}

func (ls *legSubscriber) watch(ctx context.Context, sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// todo: restart subscription.
			return
		}

		// TODO: validate msg.from
		src, err := peer.IDFromBytes(msg.From)
		if err != nil {
			continue
		}

		c, err := cid.Cast(msg.Data)
		if err != nil {
			continue
		}
		v := LegsVoucher{&c}
		np := basicnode.Prototype__Any{}
		ssb := selectorbuilder.NewSelectorSpecBuilder(np)
		sn := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()
		_, err = ls.transfer.OpenPullDataChannel(ctx, src, &v, c, sn)
		if err != nil {
			// retry?
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
