package legs

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

const testTopic = "legs/testtopic"

func TestBrokerRoundTripSimple(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	_, _, lp, lb := brokerInitPubSub(t, srcStore, dstStore)

	watcher, cncl := lb.OnSyncFinished()

	// Update root with item
	itm := basicnode.NewString("hello world")
	lnk, err := test.Store(srcStore, itm)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		cncl()
		lp.Close()
		lb.Close()
	})

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(2 * time.Second)

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

func TestBrokerRoundTrip(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost1 := mkTestHost()
	srcLnkS1 := test.MkLinkSystem(srcStore1)

	lp1, err := NewPublisher(context.Background(), srcHost1, srcStore1, srcLnkS1, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost2 := mkTestHost()
	srcLnkS2 := test.MkLinkSystem(srcStore2)
	lp2, err := NewPublisher(context.Background(), srcHost2, srcStore2, srcLnkS2, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := mkTestHost()

	srcHost1.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost1.ID(), srcHost1.Addrs(), time.Hour)
	srcHost2.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost2.ID(), srcHost2.Addrs(), time.Hour)

	dstLnkS := test.MkLinkSystem(dstStore)
	ld, err := NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Connections must be made after Broker is created, because the
	// gossip pubsub must be created before connections are made.  Otherwise,
	// the connecting hosts will not see the destination host has pubsub and
	// messages will not get published.
	dstPeerInfo := dstHost.Peerstore().PeerInfo(dstHost.ID())
	if err = srcHost1.Connect(context.Background(), dstPeerInfo); err != nil {
		t.Fatal(err)
	}
	if err = srcHost2.Connect(context.Background(), dstPeerInfo); err != nil {
		t.Fatal(err)
	}

	watcher1, cncl1 := ld.OnSyncFinished()
	watcher2, cncl2 := ld.OnSyncFinished()

	// Update root on publisher one with item
	itm1 := basicnode.NewString("hello world")
	lnk1, err := test.Store(srcStore1, itm1)
	if err != nil {
		t.Fatal(err)
	}
	// Update root on publisher one with item
	itm2 := basicnode.NewString("hello world 2")
	lnk2, err := test.Store(srcStore2, itm2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cncl1()
		cncl2()
		ld.Close()
	})

	if err := lp1.UpdateRoot(context.Background(), lnk1.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}
	t.Log("Publish 1:", lnk1.(cidlink.Link).Cid)

	if err := lp2.UpdateRoot(context.Background(), lnk2.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}
	t.Log("Publish 2:", lnk2.(cidlink.Link).Cid)

	// Check that watcher 1 gets both events.
	for i := 0; i < 4; i++ {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timed out waiting for sync to propogate")
		case downstream := <-watcher1:
			if !downstream.Cid.Equals(lnk1.(cidlink.Link).Cid) && !downstream.Cid.Equals(lnk2.(cidlink.Link).Cid) {
				t.Fatalf("sync'd cid unexpected %s vs %s", downstream, lnk1)
			}
			if _, err := dstStore.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
				t.Fatalf("data not in receiver store: %v", err)
			}
			t.Log("Watcher 1 got sync:", downstream.Cid)
		case downstream := <-watcher2:
			if !downstream.Cid.Equals(lnk1.(cidlink.Link).Cid) && !downstream.Cid.Equals(lnk2.(cidlink.Link).Cid) {
				t.Fatalf("sync'd cid unexpected %s vs %s", downstream, lnk1)
			}
			if _, err := dstStore.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
				t.Fatalf("data not in receiver store: %v", err)
			}
			t.Log("Watcher 2 got sync:", downstream.Cid)
		}
	}
}

func TestCloseBroker(t *testing.T) {
	st := dssync.MutexWrap(datastore.NewMapDatastore())
	sh := mkTestHost()
	lsys := test.MkLinkSystem(st)

	ld, err := NewBroker(sh, st, lsys, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}

	watcher, cncl := ld.OnSyncFinished()

	err = ld.Close()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case _, open := <-watcher:
		if open {
			t.Fatal("Watcher channel should have been closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watcher to close")
	}

	done := make(chan struct{})
	go func() {
		cncl()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("OnSyncFinished cancel func did not return after Close")
	}
}
