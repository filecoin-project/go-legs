package dtsync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
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
	dtClose       dtCloseFunc
	headPublisher *head.Publisher
	host          host.Host
	extraData     []byte
	topic         *pubsub.Topic
}

const shutdownTime = 5 * time.Second

// NewPublisher creates a new legs publisher
func NewPublisher(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, options ...Option) (*publisher, error) {
	cfg := config{}
	err := cfg.apply(options)
	if err != nil {
		return nil, err
	}

	var cancel context.CancelFunc
	t := cfg.topic
	if t == nil {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		t, err = gpubsub.MakePubsub(ctx, host, topic)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	dtManager, _, dtClose, err := makeDataTransfer(host, ds, lsys)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	p := &publisher{
		cancelPubSub:  cancel,
		dtManager:     dtManager,
		dtClose:       dtClose,
		headPublisher: headPublisher,
		host:          host,
		topic:         t,
	}

	if len(cfg.extraData) != 0 {
		p.extraData = cfg.extraData
	}
	return p, nil
}

func startHeadPublisher(host host.Host, topic string, headPublisher *head.Publisher) {
	go func() {
		log.Infow("Starting head publisher for topic", "topic", topic, "host", host.ID())
		err := headPublisher.Serve(host, topic)
		if err != http.ErrServerClosed {
			log.Errorw("Head publisher stopped serving on topic on host", "topic", topic, "host", host.ID(), "err", err)
		}
		log.Infow("Stopped head publisher", "host", host.ID(), "topic", topic)
	}()
}

// NewPublisherFromExisting instantiates go-legs publishing on an existing
// data transfer instance
func NewPublisherFromExisting(dtManager dt.Manager, host host.Host, topic string, lsys ipld.LinkSystem, options ...Option) (*publisher, error) {
	cfg := config{}
	err := cfg.apply(options)
	if err != nil {
		return nil, err
	}

	var cancel context.CancelFunc
	t := cfg.topic
	if t == nil {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		t, err = gpubsub.MakePubsub(ctx, host, topic)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	err = configureDataTransferForLegs(context.Background(), dtManager, lsys)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, fmt.Errorf("cannot configure datatransfer: %w", err)
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	p := &publisher{
		cancelPubSub:  cancel,
		headPublisher: headPublisher,
		host:          host,
		topic:         t,
	}

	if len(cfg.extraData) != 0 {
		p.extraData = cfg.extraData
	}
	return p, nil
}

func (p *publisher) SetRoot(ctx context.Context, c cid.Cid) error {
	if c == cid.Undef {
		return errors.New("cannot update to an undefined cid")
	}
	log.Debugf("Setting root CID: %s", c)
	return p.headPublisher.UpdateRoot(ctx, c)
}

func (p *publisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return p.UpdateRootWithAddrs(ctx, c, p.host.Addrs())
}

func (p *publisher) UpdateRootWithAddrs(ctx context.Context, c cid.Cid, addrs []ma.Multiaddr) error {
	err := p.SetRoot(ctx, c)
	if err != nil {
		return err
	}
	log.Debugf("Publishing CID and addresses in pubsub channel: %s", c)
	msg := Message{
		Cid:       c,
		Addrs:     addrs,
		ExtraData: p.extraData,
	}
	return p.topic.Publish(ctx, EncodeMessage(msg))
}

func (p *publisher) Close() error {
	var errs error
	p.closeOnce.Do(func() {
		err := p.headPublisher.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}

		if p.dtClose != nil {
			err = p.dtClose()
			if err != nil {
				errs = multierror.Append(errs, err)
			}
		}

		t := time.AfterFunc(shutdownTime, p.cancelPubSub)
		if err = p.topic.Close(); err != nil {
			log.Errorw("Failed to close pubsub topic", "err", err)
			errs = multierror.Append(errs, err)
		}

		t.Stop()
		if p.cancelPubSub != nil {
			p.cancelPubSub()
		}
	})
	return errs
}
