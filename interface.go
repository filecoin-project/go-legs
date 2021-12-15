package legs

import (
	"context"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("go-legs")

// Publisher is an interface for updating the published dag.
type Publisher interface {
	// Publishes and update for the DAG in the pubsub channel.
	UpdateRoot(context.Context, cid.Cid) error
	// Publishes and update for the DAG in the pubsub channel using custom multiaddrs.
	UpdateRootWithAddrs(context.Context, cid.Cid, []ma.Multiaddr) error
	// Close publisher
	Close() error
}

// LegSubscriber is an interface for watching a published dag.
type LegSubscriber interface {
	// OnChange returns a listener and cancel func for subscriber.  When cancel
	// is called the listener channel is closed.
	OnChange() (chan cid.Cid, context.CancelFunc)
	// SetPolicyHandler triggered to know if an exchange needs to be made.
	SetPolicyHandler(PolicyHandler) error
	// SetLatestSync updates the latest sync of a subcriber to account for
	// out of band updates.
	SetLatestSync(c cid.Cid) error
	// Sync to a specific Cid of a peer's DAG without having to wait for a
	// publication.
	// Returns a channel that will resolve with the same cid, c that was passed as input when
	// the dag from 'c' is available, a function to cancel, or a synchronous error if
	// the subscriber is not in a state where it can sync from the specified peer.
	Sync(ctx context.Context, p peer.ID, c cid.Cid, selector ipld.Node) (<-chan cid.Cid, context.CancelFunc, error)
	// Close subscriber
	Close() error
	// LatestSync gets the latest synced link.
	LatestSync() ipld.Link
}

// PolicyHandler make some preliminary checks before running the exchange
type PolicyHandler func(*pubsub.Message) (bool, error)

// FilterPeerPolicy is a sample policy that only triggers exchanges
// if the update is generated from a specific peer.
func FilterPeerPolicy(p peer.ID) PolicyHandler {
	return func(msg *pubsub.Message) (bool, error) {
		from := msg.GetFrom()
		allow := from == p
		if !allow {
			log.Debugf("Filtered pubsub message from %s", from)
		}
		return allow, nil
	}
}

// Syncer is the interface used to sync with a data source.
type Syncer interface {
	GetHead(context.Context) (cid.Cid, error)
	Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error
}
