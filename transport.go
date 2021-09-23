package legs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"

	dt "github.com/filecoin-project/go-data-transfer"
	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/minio/blake2b-simd"
	errors "golang.org/x/xerrors"
)

const (
	// directConnectTicks makes pubsub check it's connected to direct peers every N seconds.
	directConnectTicks uint64 = 30
)

// LegTransport wraps all the assets to set-up the data transfer.
type LegTransport struct {
	tmpDir string
	ds     datastore.Batching
	t      dt.Manager
	Gs     graphsync.GraphExchange
	topic  *pubsub.Topic

	refc int32
}

// MakeLegTransport creates a new datatransfer transport to use with go-legs
func MakeLegTransport(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string) (*LegTransport, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(context.Background(), gsnet, lsys)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	// DataTransfer channels use this file to track cidlist of exchanges
	// NOTE: It needs to be initialized for the datatransfer not to fail, but
	// it has no other use outside the cidlist, so I don't think it should be
	// exposed publicly. It's only used for the life of a data transfer.
	// In the future, once an empty directory is accepted as input, it
	// this may be removed.
	tmpDir, err := ioutil.TempDir("", "go-legs")
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

	return &LegTransport{
		tmpDir: tmpDir,
		ds:     ds,
		t:      dt,
		Gs:     gs,
		topic:  t,
	}, nil
}

func (lt *LegTransport) addRefc() {
	atomic.AddInt32(&lt.refc, 1)
}

// Fetch starts a new transfer with a peer for certain CID using the selector passed as argument.
//
// sel determines the selector to use for the exchange
// Optionally, we can run an onFinish callback when the exchange has finished.
func (lt *LegTransport) Fetch(ctx context.Context, p peer.ID, c cid.Cid, sel ipld.Node, onFinish func()) (chan cid.Cid, context.CancelFunc, error) {
	v := Voucher{&c}
	out := make(chan cid.Cid)
	unsub := lt.t.SubscribeToEvents(lt.onFetchFinished(c, out, onFinish))
	_, err := lt.t.OpenPullDataChannel(ctx, p, &v, c, sel)
	if err != nil {
		log.Errorf("Error starting data channel to fetch cid %s: %v", c, err)
		return nil, nil, err
	}
	cncl := func() {
		unsub()
		close(out)
	}
	return out, cncl, nil
}

func (lt *LegTransport) onFetchFinished(c cid.Cid, out chan cid.Cid, onFinish func()) func(dt.Event, dt.ChannelState) {
	return func(event dt.Event, channelState dt.ChannelState) {
		if event.Code == dt.FinishTransfer {
			if c == channelState.BaseCID() {
				if onFinish != nil {
					onFinish()
				}
				out <- channelState.BaseCID()
			}
		}
	}
}

// Close closes the LegTransport. It returns an error if it still
// has an active publisher or subscriber attached to the transport.
func (lt *LegTransport) Close(ctx context.Context) error {
	refc := atomic.LoadInt32(&lt.refc)
	if refc == 0 {
		err := lt.t.Stop(ctx)
		err2 := os.RemoveAll(lt.tmpDir)
		err3 := lt.topic.Close()
		if err != nil {
			return err
		}
		if err2 != nil {
			return err2
		}
		return err3
	} else if refc > 0 {
		return errors.Errorf("can't close transport. %d pub/sub still active", refc)
	}
	return nil
}

func makePubsub(ctx context.Context, h host.Host, topic string) (*pubsub.Topic, error) {
	p, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerExchange(true),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			hash := blake2b.Sum256(pmsg.Data)
			return string(hash[:])
		}),
		pubsub.WithFloodPublish(true),
		pubsub.WithDirectConnectTicks(directConnectTicks),
	)
	if err != nil {
		return nil, fmt.Errorf("constructing pubsub: %d", err)
	}

	return p.Join(topic)
}
