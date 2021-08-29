package legs

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
)

type legPublisher struct {
	ds       datastore.Datastore
	transfer *LegTransport
}

// NewPublisher creates a new legs publisher
func NewPublisher(ctx context.Context, dataStore datastore.Batching, host host.Host,
	dt *LegTransport,
	lsys ipld.LinkSystem) (LegPublisher, error) {

	// Track how many publishers are using this transport
	dt.addRefc()

	return &legPublisher{
		ds:       dataStore,
		transfer: dt}, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	log.Debugf("Published CID in pubsub channel: %s", c)
	return lp.transfer.topic.Publish(ctx, c.Bytes())
}

func (lp *legPublisher) Close(ctx context.Context) error {
	return lp.transfer.Close(ctx)
}
