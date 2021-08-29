package legs

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type legPublisher struct {
	ds datastore.Datastore
	*pubsub.Topic
	transfer *LegTransport
}

// NewPublisher creates a new legs publisher
//
// TODO: Add a parameter or config to set the directory that the publisher's
// tmpDir is created in
func NewPublisher(ctx context.Context, dataStore datastore.Batching, host host.Host,
	dt *LegTransport, topic string,
	lsys ipld.LinkSystem) (LegPublisher, error) {

	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	// Track how many publishers are using this transport
	dt.addRefc()

	return &legPublisher{
		ds:       dataStore,
		Topic:    t,
		transfer: dt}, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return lp.Topic.Publish(ctx, c.Bytes())
}

func (lp *legPublisher) Close(ctx context.Context) error {
	err := lp.Topic.Close()
	err2 := lp.transfer.Close(ctx)
	if err != nil {
		return err
	}
	return err2
}
