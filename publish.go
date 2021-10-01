package legs

import (
	"context"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type legPublisher struct {
	r     Reference
	topic *pubsub.Topic
}

// NewPublisher creates a new legs publisher
func NewPublisher(ctx context.Context, dt *LegTransport) (LegPublisher, error) {
	return NewPublisherFromDeps(ctx, dt, dt.topic)
}

// NewPublisherFromDeps instantiates go-legs from direct dependencies
func NewPublisherFromDeps(ctx context.Context, r Reference, topic *pubsub.Topic) (LegPublisher, error) {
	r.AddReference()
	return &legPublisher{
		r:     r,
		topic: topic,
	}, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	log.Debugf("Published CID in pubsub channel: %s", c)
	return lp.topic.Publish(ctx, c.Bytes())
}

func (lp *legPublisher) Close() error {
	lp.r.RemoveReference()
	return nil
}
