package legs

import (
	"context"
	"sync/atomic"

	"github.com/ipfs/go-cid"
)

type legPublisher struct {
	transfer *LegTransport
}

// NewPublisher creates a new legs publisher
func NewPublisher(ctx context.Context, dt *LegTransport) (LegPublisher, error) {

	// Track how many publishers are using this transport
	dt.addRefc()

	return &legPublisher{
		transfer: dt}, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	log.Debugf("Published CID in pubsub channel: %s", c)
	return lp.transfer.topic.Publish(ctx, c.Bytes())
}

func (lp *legPublisher) Close() error {
	atomic.AddInt32(lp.transfer.refc, -1)
	return nil
}
