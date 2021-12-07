package legs

import (
	"context"
	"fmt"
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
	ctx    context.Context
	tmpDir string
	t      dt.Manager
	gs     graphsync.GraphExchange
	topic  *pubsub.Topic
	refc   int32
	host   host.Host

	// dss captures the default selector sequence passed to ExploreRecursiveWithStopNode
	dss ipld.Node
}

// NewMultiSubscriber sets up a new instance of a multi subscriber.
//
// A default selector sequence, dss, may optionally be specified. The selector sequence is used
// during sync traversal to define the extent by which a node is explored. If unspecified, all edges
// of nodes are recursively explored.
//
// Note that the default selector sequence is wrapped with a selector logic that will stop the
// traversal when the latest synced link is reached. Therefore, it must only specify the selection
// sequence itself.
//
// See: ExploreRecursiveWithStopNode.
func NewMultiSubscriber(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, dss ipld.Node) (LegMultiSubscriber, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}

	dt, gs, tmpDir, err := makeDataTransfer(ctx, host, ds, lsys)
	if err != nil {
		return nil, err
	}

	return &legMultiSubscriber{
		ctx:    ctx,
		tmpDir: tmpDir,
		t:      dt,
		gs:     gs,
		topic:  t,
		host:   host,
		dss:    dss,
	}, nil
}

func (lt *legMultiSubscriber) NewSubscriber(policy PolicyHandler) (LegSubscriber, error) {
	l, err := newSubscriber(lt.ctx, lt.t, lt.topic, lt.onCloseSubscriber, lt.host, policy, lt.dss)
	if err != nil {
		return nil, err
	}
	lt.addRefc()
	return l, nil
}

func (lt *legMultiSubscriber) NewSubscriberPartiallySynced(policy PolicyHandler, latestSync cid.Cid) (LegSubscriber, error) {
	l, err := newSubscriber(lt.ctx, lt.t, lt.topic, lt.onCloseSubscriber, lt.host, policy, lt.dss)
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
	log.Info("Closing multi subscriber")
	refc := atomic.LoadInt32(&lt.refc)
	if refc == 0 {
		err := lt.t.Stop(ctx)
		err2 := os.RemoveAll(lt.tmpDir)
		err3 := lt.topic.Close()
		if err != nil {
			log.Errorf("Failed to stop datatransfer manager: %s", err)
			return err
		}
		if err2 != nil {
			log.Errorf("Failed to remove temp dir: %s", err)
			return err2
		}
		if err3 != nil {
			log.Errorf("Failed to close pubsub topic: %s", err)
		}
		return err3
	} else if refc > 0 {
		err := fmt.Errorf("can't close transport. %d pub/sub still active", refc)
		log.Errorf("Failed to close multi subscriber: %s", err)
		return err
	}
	return nil
}
