package legs

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Publish will export an IPLD dag of data publicly for consumption.
//func Publish(ctx context.Context, dataStore datastore.Datastore, host host.Host, topic string) LegPublisher

// LegPublisher is an interface for updating the published dag.
type LegPublisher interface {
	// Publishes and update for the DAG in the pubsub channel.
	UpdateRoot(context.Context, cid.Cid) error
	// Close publisher
	Close() error
}

// Subscribe will sync an IPLD dag of data from a publisher
//func Subscribe(ctx context.Context, dataStore datastore.Datastore, host host.Host, topic string) LegSubscriber

// LegSubscriber is an interface for watching a published dag.
type LegSubscriber interface {
	// OnChange returns a listener and cancel func for subscriber.  When cancel
	// is called the listener channel is closed.
	OnChange() (chan cid.Cid, context.CancelFunc)
	// SetPolicyHandler triggered to know if an exchange needs to be made.
	SetPolicyHandler(PolicyHandler) error
	// SetLatestSync updates the latest sync of a subcriber in case it has
	// already update some data off-band.
	SetLatestSync(c cid.Cid) error
	// Sync to a specific Cid of a peer's DAG without having to wait for a
	// publication.
	Sync(ctx context.Context, p peer.ID, c cid.Cid) (chan cid.Cid, context.CancelFunc, error)
	// Close subscriber
	Close() error
}

// PolicyHandler make some preliminary checks before running the exchange
type PolicyHandler func(*pubsub.Message) (bool, error)

// FilterPeerPolicy is a sample policy that only triggers exchanges
// if the update is generated from a specific peer.
func FilterPeerPolicy(p peer.ID) PolicyHandler {
	return func(msg *pubsub.Message) (bool, error) {
		src, err := peer.IDFromBytes(msg.From)
		if err != nil {
			return false, err
		}
		return src == p, nil
	}
}
