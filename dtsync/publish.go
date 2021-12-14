package dtsync

import (
	"context"
	"errors"
	"net/http"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/gpubsub"
	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/hashicorp/go-multierror"
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
	closeOnce     sync.Once
}

// NewPublisher creates a new legs publisher
func NewPublisher(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string) (legs.LegPublisher, error) {
	ss, err := newSimpleSetup(ctx, host, ds, lsys, topic)
	if err != nil {
		log.Errorf("Failed to instantiate simple setup")
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)
	return &legPublisher{
		topic:         ss.t,
		onClose:       ss.onClose,
		host:          host,
		headPublisher: headPublisher,
	}, nil
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
	lsys ipld.LinkSystem) (legs.LegPublisher, error) {
	t, err := gpubsub.MakePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}
	err = configureDataTransferForLegs(ctx, dt, lsys)
	if err != nil {
		return nil, err
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	return &legPublisher{
		topic:         t,
		onClose:       t.Close,
		host:          host,
		headPublisher: headPublisher,
	}, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return lp.UpdateRootWithAddrs(ctx, c, lp.host.Addrs())
}

func (lp *legPublisher) UpdateRootWithAddrs(ctx context.Context, c cid.Cid, addrs []ma.Multiaddr) error {
	if c == cid.Undef {
		return errors.New("cannot update to an undefined cid")
	}

	log.Debugf("Publishing CID and addresses in pubsub channel: %s", c)
	var errs error
	err := lp.headPublisher.UpdateRoot(ctx, c)
	if err != nil {
		errs = multierror.Append(errs, err)
	}
	msg := message{
		cid:   c,
		addrs: addrs,
	}
	err = lp.topic.Publish(ctx, EncodeMessage(msg))
	if err != nil {
		errs = multierror.Append(errs, err)
	}
	return errs
}

func (lp *legPublisher) Close() error {
	var errs error
	lp.closeOnce.Do(func() {
		err := lp.headPublisher.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}
		err = lp.onClose()
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	})
	return errs
}
