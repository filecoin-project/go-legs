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
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
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

	refc *int32
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

	var r int32 = 0
	return &LegTransport{
		tmpDir: tmpDir,
		ds:     ds,
		t:      dt,
		Gs:     gs,
		topic:  t,
		refc:   &r}, nil
}

func (lt *LegTransport) addRefc() {
	atomic.AddInt32(lt.refc, 1)
}

// Close closes the LegTransport. It returns an error if it still
// has an active publisher or subscriber attached to the transport.
func (lt *LegTransport) Close(ctx context.Context) error {
	if n := atomic.AddInt32(lt.refc, -1); n == 0 {
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
	} else if n > 0 {
		return errors.Errorf("can't close transport. %d pub/sub still active", n)
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
