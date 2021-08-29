package legs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/minio/blake2b-simd"
)

const (
	// directConnectTicks makes pubsub check it's connected to direct peers every N seconds.
	directConnectTicks uint64 = 30
)

// LegTransport wraps all the assets to set-up the data transfer.
type LegTransport struct {
	tmpDir string
	t      dt.Manager
	Gs     graphsync.GraphExchange
	topic  *pubsub.Topic

	lk   sync.Mutex
	refc int
}

// MakeLegTransport creates a new datatransfer transport to use with go-legs
func MakeLegTransport(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, tmpPath string) (*LegTransport, error) {

	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(context.Background(), gsnet, lsys)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	tmpDir, err := ioutil.TempDir("", tmpPath)
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
	return &LegTransport{tmpDir: tmpDir, t: dt, Gs: gs, topic: t}, nil
}

func (lt *LegTransport) addRefc() {
	lt.lk.Lock()
	defer lt.lk.Unlock()
	lt.refc++
}

func (lt *LegTransport) Close(ctx context.Context) error {
	lt.lk.Lock()
	defer lt.lk.Unlock()
	lt.refc--

	if lt.refc == 0 {
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
