package legs_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

func TestMultiSubscribeRoundTrip(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost1 := test.MkTestHost()
	srcLnkS1 := test.MkLinkSystem(srcStore1)

	lp1, err := dtsync.NewPublisher(context.Background(), srcHost1, srcStore1, srcLnkS1, testTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer lp1.Close()

	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost2 := test.MkTestHost()
	srcLnkS2 := test.MkLinkSystem(srcStore2)
	lp2, err := dtsync.NewPublisher(context.Background(), srcHost2, srcStore2, srcLnkS2, testTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer lp2.Close()

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost()
	srcHost1.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost1.ID(), srcHost1.Addrs(), time.Hour)
	srcHost2.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost2.ID(), srcHost2.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)
	ms, err := dtsync.NewMultiSubscriber(context.Background(), dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close(context.Background())
	if err := srcHost1.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
	if err := srcHost2.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
	ls1, err := ms.NewSubscriber(legs.FilterPeerPolicy(srcHost1.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer ls1.Close()

	watcher1, cncl1 := ls1.OnChange()
	defer cncl1()

	ls2, err := ms.NewSubscriber(legs.FilterPeerPolicy(srcHost2.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer ls2.Close()

	watcher2, cncl2 := ls2.OnChange()
	defer cncl2()

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

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	if err := lp1.UpdateRoot(context.Background(), lnk1.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}
	if err := lp2.UpdateRoot(context.Background(), lnk2.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher1:
		if !downstream.Equals(lnk1.(cidlink.Link).Cid) {
			t.Fatalf("sync'd sid unexpected %s vs %s", downstream, lnk1)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	case downstream := <-watcher2:
		if !downstream.Equals(lnk2.(cidlink.Link).Cid) {
			t.Fatalf("sync'd sid unexpected %s vs %s", downstream, lnk2)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
}

func TestCloseTransport(t *testing.T) {
	st := dssync.MutexWrap(datastore.NewMapDatastore())
	sh := test.MkTestHost()
	lsys := test.MkLinkSystem(st)
	ms, err := dtsync.NewMultiSubscriber(context.Background(), sh, st, lsys, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close(context.Background())
	ls1, err := ms.NewSubscriber(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ls1.Close()
	ls2, err := ms.NewSubscriber(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ls2.Close()

	ls2.Close()
	err = ms.Close(context.Background())
	if err == nil {
		t.Fatal("There are still active subscribers, it should have thrown an error")
	}
	ls1.Close()
	err = ms.Close(context.Background())
	if err != nil {
		t.Fatal("If no subscribers, then transport should've been closed successfully")
	}
}
