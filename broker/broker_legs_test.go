package broker_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	dt "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/broker"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

func initPubSub(srcStore, dstStore datastore.Batching) (host.Host, host.Host, legs.LegPublisher, *broker.Broker, error) {
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)
	lp, err := dtsync.NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	lb, err := broker.NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		return nil, nil, nil, nil, err
	}

	return srcHost, dstHost, lp, lb, nil
}

func TestBrokerRoundTripExistingDataTransfer(t *testing.T) {
	// Init legs publisher and subscriber
	srcHost := test.MkTestHost()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	fakeLsys := cidlink.DefaultLinkSystem()
	srcLnkS := test.MkLinkSystem(srcStore)

	gsnet := gsnet.NewFromLibp2pHost(srcHost)
	dtNet := dtnetwork.NewFromLibp2pHost(srcHost)
	gs := gsimpl.New(context.Background(), gsnet, fakeLsys)
	tp := gstransport.NewTransport(srcHost.ID(), gs, dtNet)

	// DataTransfer channels use this file to track cidlist of exchanges
	// NOTE: It needs to be initialized for the datatransfer not to fail, but
	// it has no other use outside the cidlist, so I don't think it should be
	// exposed publicly. It's only used for the life of a data transfer.
	// In the future, once an empty directory is accepted as input, it
	// this may be removed.
	tmpDir, err := ioutil.TempDir("", "go-legs")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dt, err := dt.NewDataTransfer(srcStore, tmpDir, dtNet, tp)
	if err != nil {
		t.Fatal(err)
	}
	err = dt.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lp, err := dtsync.NewPublisherFromExisting(context.Background(), dt, srcHost, testTopic, srcLnkS)
	if err != nil {
		t.Fatal(err)
	}
	defer lp.Close()

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkS := test.MkLinkSystem(dstStore)

	lb, err := broker.NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lb.Close()
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	watcher, cncl := lb.OnSyncFinished()
	defer cncl()

	// Update root with item
	itm := basicnode.NewString("hello world")
	lnk, err := test.Store(srcStore, itm)
	if err != nil {
		t.Fatal(err)
	}

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(5 * time.Second)

	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Cid.Equals(lnk.(cidlink.Link).Cid) {
			t.Fatalf("sync'd cid unexpected %s vs %s", downstream.Cid, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
}

func TestBrokerAllowPeerReject(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	_, dstHost, lp, lb, err := initPubSub(srcStore, dstStore)
	if err != nil {
		t.Fatal(err)
	}
	defer lp.Close()
	defer lb.Close()

	// Set function to reject anything except dstHost, which is not the one
	// generating the update.
	lb.SetAllowPeer(func(peerID peer.ID) (bool, error) {
		return peerID == dstHost.ID(), nil
	})

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(2 * time.Second)

	watcher, cncl := lb.OnSyncFinished()
	defer cncl()

	c := mkLnk(t, srcStore)

	// Update root with item
	err = lp.UpdateRoot(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 3):
	case _, open := <-watcher:
		if open {
			t.Fatal("something was exchanged, and that is wrong")
		}
	}
}

func TestBrokerAllowPeerAllows(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	_, _, lp, lb, err := initPubSub(srcStore, dstStore)
	if err != nil {
		t.Fatal(err)
	}
	defer lp.CLose()
	defer lb.Close()

	// Set function to allow any peer.
	lb.SetAllowPeer(func(peerID peer.ID) (bool, error) {
		return true, nil
	})

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(2 * time.Second)

	watcher, cncl := lb.OnSyncFinished()
	defer cncl()

	c := mkLnk(t, srcStore)

	// Update root with item
	err = lp.UpdateRoot(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 7):
		t.Fatal("timed out waiting for SyncFinished")
	case <-watcher:
	}
}

func mkLnk(t *testing.T, srcStore datastore.Batching) cid.Cid {
	// Update root with item
	np := basicnode.Prototype__Any{}
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	err := ma.AssembleKey().AssignString("hey")
	if err != nil {
		t.Fatal(err)
	}
	if err = ma.AssembleValue().AssignString("it works!"); err != nil {
		t.Fatal(err)
	}
	if err = ma.AssembleKey().AssignString("yes"); err != nil {
		t.Fatal(err)
	}
	if err = ma.AssembleValue().AssignBool(true); err != nil {
		t.Fatal(err)
	}
	if err = ma.Finish(); err != nil {
		t.Fatal(err)
	}
	n := nb.Build()
	lnk, err := test.Store(srcStore, n)
	if err != nil {
		t.Fatal(err)
	}

	return lnk.(cidlink.Link).Cid
}
