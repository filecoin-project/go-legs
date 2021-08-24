package legs

import (
	"context"
	"io/ioutil"
	"os"

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
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type legPublisher struct {
	ds     datastore.Datastore
	tmpDir string
	*pubsub.Topic
	transfer dt.Manager
}

// NewPublisher creates a new legs publisher
//
// TODO: Add a parameter or config to set the directory that the publisher's
// tmpDir is created in
func NewPublisher(ctx context.Context, dataStore datastore.Batching, host host.Host, topic string, lsys ipld.LinkSystem) (LegPublisher, error) {

	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(ctx, gsnet, lsys)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	tmpDir, err := ioutil.TempDir("", "golegs-pub")
	if err != nil {
		return nil, err
	}

	dt, err := datatransfer.NewDataTransfer(dataStore, tmpDir, dtNet, tp)
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

	return &legPublisher{
		ds:       dataStore,
		tmpDir:   tmpDir,
		Topic:    t,
		transfer: dt}, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return lp.Topic.Publish(ctx, c.Bytes())
}

func (lp *legPublisher) Close(ctx context.Context) error {
	err := lp.transfer.Stop(ctx)
	err2 := os.RemoveAll(lp.tmpDir)
	err3 := lp.Topic.Close()
	if err != nil {
		return err
	}
	if err2 != nil {
		return err2
	}
	return err3
}
