package legs

import (
	"context"

	"github.com/ipfs/go-cid"
)

// Publish will export an IPLD dag of data publicly for consumption.
//func Publish(ctx context.Context, dataStore datastore.Datastore, host host.Host, topic string) LegPublisher

// LegPublisher is an interface for updating the published dag.
type LegPublisher interface {
	UpdateRoot(context.Context, cid.Cid) error
	Close(context.Context) error
}

// Subscribe will sync an IPLD dag of data from a publisher
//func Subscribe(ctx context.Context, dataStore datastore.Datastore, host host.Host, topic string) LegSubscriber

// LegSubscriber is an interface for watching a published dag.
type LegSubscriber interface {
	OnChange() (chan cid.Cid, context.CancelFunc)
	Close(context.Context) error
}
