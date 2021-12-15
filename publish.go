package legs

import (
	"context"
	"net/http"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	ma "github.com/multiformats/go-multiaddr"
)

type legPublisher struct {
	topic         *pubsub.Topic
	onClose       func() error
	host          host.Host
	headPublisher *head.Publisher
}

// NewPublisher creates a new legs publisher
func NewPublisher(ctx context.Context,
	host host.Host,
	ds datastore.Batching,
	lsys ipld.LinkSystem,
	topic string) (LegPublisher, error) {
	ss, err := newSimpleSetup(ctx, host, ds, lsys, topic)
	if err != nil {
		log.Errorf("Failed to instantiate simple setup")
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)
	return &legPublisher{ss.t, ss.onClose, host, headPublisher}, nil
}

func startHeadPublisher(host host.Host, topic string, headPublisher *head.Publisher) {
	go func() {
		log.Infof("Starting head publisher on peer ID %s for topic %s", host.ID(), topic)
		err := headPublisher.Serve(host, topic)
		if err != http.ErrServerClosed {
			log.Errorf("Error head publisher stopped serving on peer ID %s for topic %s: %s", host.ID(), topic, err)
		}
		log.Infof("Stopped head publisher on peer ID %s for topic %s", host.ID(), topic)
	}()
}

// NewPublisherFromExisting instantiates go-legs publishing on an existing
// data transfer instance
func NewPublisherFromExisting(ctx context.Context,
	dt dt.Manager,
	host host.Host,
	topic string,
	lsys ipld.LinkSystem) (LegPublisher, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}
	err = configureDataTransferForLegs(ctx, dt, lsys)
	if err != nil {
		return nil, err
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	return &legPublisher{t, t.Close, host, headPublisher}, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return lp.UpdateRootWithAddrs(ctx, c, lp.host.Addrs())
}

func (lp *legPublisher) UpdateRootWithAddrs(ctx context.Context, c cid.Cid, addrs []ma.Multiaddr) error {
	log.Debugf("Published CID and addresses in pubsub channel: %s", c)
	msg := message{
		cid:   c,
		addrs: addrs,
	}
	err1 := lp.topic.Publish(ctx, encodeMessage(msg))
	err2 := lp.headPublisher.UpdateRoot(ctx, c)
	if err1 != nil {
		return err1
	}
	return err2
}

func (lp *legPublisher) Close() error {
	err1 := lp.headPublisher.Close()
	err2 := lp.onClose()

	if err1 != nil {
		return err1
	}
	return err2
}
