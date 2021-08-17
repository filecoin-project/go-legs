package legs

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
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
	Subscribe(ctx context.Context, selector ipld.Node, policy PolicyHandler) error
	OnChange() (chan cid.Cid, context.CancelFunc)
	SetPolicyHandler(PolicyHandler) error
	Close(context.Context) error
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
