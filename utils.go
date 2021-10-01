package legs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	dt "github.com/filecoin-project/go-data-transfer"
	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/minio/blake2b-simd"

	"github.com/ipld/go-ipld-prime"
)

// configureDataTransferForLegs configures an existing data transfer instance to serve go-legs requests
// from given linksystem (publisher only)
func configureDataTransferForLegs(ctx context.Context, dt dt.Manager, lsys ipld.LinkSystem) error {
	v := &Voucher{}
	lvr := &VoucherResult{}
	val := &legsValidator{}
	lsc := legStorageConfigration{lsys}
	if err := dt.RegisterVoucherType(v, val); err != nil {
		return err
	}
	if err := dt.RegisterVoucherResultType(lvr); err != nil {
		return err
	}
	if err := dt.RegisterTransportConfigurer(v, lsc.configureTransport); err != nil {
		return err
	}
	return nil
}

type storeConfigurableTransport interface {
	UseStore(dt.ChannelID, ipld.LinkSystem) error
}

type legStorageConfigration struct {
	linkSystem ipld.LinkSystem
}

func (lsc legStorageConfigration) configureTransport(chid dt.ChannelID, voucher dt.Voucher, transport dt.Transport) {
	storeConfigurableTransport, ok := transport.(storeConfigurableTransport)
	if !ok {
		return
	}
	err := storeConfigurableTransport.UseStore(chid, lsc.linkSystem)
	if err != nil {
		log.Errorf("attempting to configure data store: %s", err)
	}
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

func makeDataTransfer(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem) (dt.Manager, graphsync.GraphExchange, string, error) {

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
		return nil, nil, "", err
	}
	dt, err := datatransfer.NewDataTransfer(ds, tmpDir, dtNet, tp)
	if err != nil {
		return nil, nil, "", err
	}

	v := &Voucher{}
	lvr := &VoucherResult{}
	val := &legsValidator{}
	if err := dt.RegisterVoucherType(v, val); err != nil {
		return nil, nil, "", err
	}
	if err := dt.RegisterVoucherResultType(lvr); err != nil {
		return nil, nil, "", err
	}
	if err := dt.Start(ctx); err != nil {
		return nil, nil, "", err
	}

	return dt, gs, tmpDir, nil
}

type simpleSetup struct {
	ctx    context.Context
	t      *pubsub.Topic
	dt     dt.Manager
	tmpDir string
}

func newSimpleSetup(ctx context.Context,
	host host.Host,
	ds datastore.Batching,
	lsys ipld.LinkSystem,
	topic string) (*simpleSetup, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}
	dt, _, tmpDir, err := makeDataTransfer(ctx, host, ds, lsys)
	if err != nil {
		return nil, err
	}

	return &simpleSetup{
		ctx, t, dt, tmpDir,
	}, nil
}

func (ss *simpleSetup) onClose() error {
	err := ss.dt.Stop(ss.ctx)
	err2 := os.RemoveAll(ss.tmpDir)
	err3 := ss.t.Close()
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	return err3
}
