package legs

import (
	"bytes"
	"context"
	"io"
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
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type legPublisher struct {
	ds     datastore.Datastore
	tmpDir string
	*pubsub.Topic
	transfer dt.Manager
}

// Publish will export an IPLD dag of data publicly for consumption.
func Publish(ctx context.Context, dataStore datastore.Batching, host host.Host, topic string) (LegPublisher, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := dataStore.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return dataStore.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(ctx, gsnet, lsys)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	tmpDir := os.TempDir()

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
