package legs

import (
	"context"
	"os"
	"sync/atomic"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	errors "golang.org/x/xerrors"
)

const (
	// directConnectTicks makes pubsub check it's connected to direct peers every N seconds.
	directConnectTicks uint64 = 30
)

// LegMultiSubscriber allows you to setup multiple subscriptions over single graphsync/datatransfer instance
// with difference policy filters
type LegMultiSubscriber interface {
	NewSubscriber(PolicyHandler) (LegSubscriber, error)
	NewSubscriberPartiallySynced(PolicyHandler, cid.Cid) (LegSubscriber, error)
	GraphSync() graphsync.GraphExchange
	DataTransfer() dt.Manager
	Close(ctx context.Context) error
}

type legMultiSubscriber struct {
	ctx             context.Context
	tmpDir          string
	t               dt.Manager
	gs              graphsync.GraphExchange
	topic           *pubsub.Topic
	refc            int32
	defaultSelector ipld.Node
}

// NewMultiSubscriber sets up a new instance of a multi subscriber
func NewMultiSubscriber(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, selector ipld.Node) (LegMultiSubscriber, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	dt, gs, tmpDir, err := makeDataTransfer(ctx, host, ds, lsys)
	if err != nil {
		return nil, err
	}

	return &legMultiSubscriber{
		ctx:             ctx,
		tmpDir:          tmpDir,
		t:               dt,
		gs:              gs,
		topic:           t,
		defaultSelector: selector,
	}, nil
}

func (lt *legMultiSubscriber) NewSubscriber(policy PolicyHandler) (LegSubscriber, error) {

	l, err := newSubscriber(lt.ctx, lt.t, lt.topic, lt.onCloseSubscriber, policy, lt.defaultSelector)
	if err != nil {
		return nil, err
	}
	lt.addRefc()
	return l, nil
}

func (lt *legMultiSubscriber) NewSubscriberPartiallySynced(policy PolicyHandler, latestSync cid.Cid) (LegSubscriber, error) {
	l, err := newSubscriber(lt.ctx, lt.t, lt.topic, lt.onCloseSubscriber, policy, lt.defaultSelector)
	if err != nil {
		return nil, err
	}
	l.syncmtx.Lock()
	defer l.syncmtx.Unlock()
	if latestSync != cid.Undef {
		l.latestSync = cidlink.Link{Cid: latestSync}
	}
	lt.addRefc()
	return l, nil
}

func (lt *legMultiSubscriber) GraphSync() graphsync.GraphExchange {
	return lt.gs
}

func (lt *legMultiSubscriber) DataTransfer() dt.Manager {
	return lt.t
}

func (lt *legMultiSubscriber) addRefc() {
	atomic.AddInt32(&lt.refc, 1)
}

func (lt *legMultiSubscriber) onCloseSubscriber() error {
	atomic.AddInt32(&lt.refc, -1)
	return nil
}

// Close closes the legMultiSubscriber. It returns an error if it still
// has an active publisher or subscriber attached to the transport.
func (lt *legMultiSubscriber) Close(ctx context.Context) error {
	refc := atomic.LoadInt32(&lt.refc)
	if refc == 0 {
		err := lt.t.Stop(ctx)
		err2 := os.RemoveAll(lt.tmpDir)
		err3 := lt.topic.Close()
		if err != nil {
			return err
		}
		if err2 != nil {
			return err2
		}
		return err3
	} else if refc > 0 {
		return errors.Errorf("can't close transport. %d pub/sub still active", refc)
	}
	return nil
}
