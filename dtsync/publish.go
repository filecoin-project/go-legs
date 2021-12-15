package dtsync

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync"
	"time"

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

type publisher struct {
	cancelPubSub  context.CancelFunc
	closeOnce     sync.Once
	dtManager     dt.Manager
	headPublisher *head.Publisher
	host          host.Host
	tmpDir        string
	topic         *pubsub.Topic
}

const shutdownTime = 5 * time.Second

// NewPublisher creates a new legs publisher
func NewPublisher(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string) (legs.Publisher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	t, err := gpubsub.MakePubsub(ctx, host, topic)
	if err != nil {
		cancel()
		return nil, err
	}
	dtManager, _, tmpDir, err := makeDataTransfer(context.Background(), host, ds, lsys, nil)
	if err != nil {
		cancel()
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	return &publisher{
		cancelPubSub:  cancel,
		dtManager:     dtManager,
		headPublisher: headPublisher,
		host:          host,
		tmpDir:        tmpDir,
		topic:         t,
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
func NewPublisherFromExisting(dtManager dt.Manager, host host.Host, topic string, lsys ipld.LinkSystem) (legs.Publisher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	t, err := gpubsub.MakePubsub(ctx, host, topic)
	if err != nil {
		cancel()
		return nil, err
	}
	err = configureDataTransferForLegs(context.Background(), dtManager, lsys)
	if err != nil {
		cancel()
		return nil, err
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	return &publisher{
		cancelPubSub:  cancel,
		headPublisher: headPublisher,
		host:          host,
		topic:         t,
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

func (lp *publisher) Close() error {
	var errs error
	lp.closeOnce.Do(func() {
		err := lp.headPublisher.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}

		// If tmpDir is non-empty, that means the publisher started the dtManager and
		// it is ok to stop is and clean up the tmpDir.
		if lp.tmpDir != "" {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTime)
			defer cancel()

			err = lp.dtManager.Stop(ctx)
			if err != nil {
				log.Errorf("Failed to stop datatransfer manager: %s", err)
				errs = multierror.Append(errs, err)
			}
			if err = os.RemoveAll(lp.tmpDir); err != nil {
				log.Errorf("Failed to remove temp dir: %s", err)
				errs = multierror.Append(errs, err)
			}
		}

		t := time.AfterFunc(shutdownTime, lp.cancelPubSub)
		if err = lp.topic.Close(); err != nil {
			log.Errorf("Failed to close pubsub topic: %s", err)
			errs = multierror.Append(errs, err)
		}

		t.Stop()
		lp.cancelPubSub()
	})
	return errs
}
