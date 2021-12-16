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

const (
	testTopic     = "/legs/testtopic"
	updateTimeout = 1 * time.Second
)

func TestBrokerRoundTripSimple(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	_, _, lp, bkr, err := brokerInitPubSub(t, srcStore, dstStore)
	if err != nil {
		t.Fatal(err)
	}
	defer lp.Close()
	defer bkr.Close()

	watcher, cncl := bkr.OnSyncFinished()
	defer cncl()

	// Update root with item
	itm := basicnode.NewString("hello world")
	lnk, err := test.Store(srcStore, itm)
	if err != nil {
		t.Fatal(err)
	}

	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(updateTimeout):
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
	srcHost1 := test.MkTestHost()
	srcLnkS1 := test.MkLinkSystem(srcStore1)

	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost2 := test.MkTestHost()
	srcLnkS2 := test.MkLinkSystem(srcStore2)

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost()

	dstLnkS := test.MkLinkSystem(dstStore)

	topics := test.WaitForMeshWithMessage(t, "testTopic", srcHost1, srcHost2, dstHost)

	pub1, err := dtsync.NewPublisher(srcHost1, srcStore1, srcLnkS1, "", dtsync.Topic(topics[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer pub1.Close()

	pub2, err := dtsync.NewPublisher(srcHost2, srcStore2, srcLnkS2, "", dtsync.Topic(topics[1]))
	if err != nil {
		t.Fatal(err)
	}
	defer pub2.Close()

	bkr, err := legs.NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil, legs.Topic(topics[2]))
	if err != nil {
		t.Fatal(err)
	}
	defer bkr.Close()

	watcher1, cncl1 := bkr.OnSyncFinished()
	defer cncl1()
	watcher2, cncl2 := bkr.OnSyncFinished()
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

	if err = pub1.UpdateRoot(context.Background(), lnk1.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}
	t.Log("Publish 1:", lnk1.(cidlink.Link).Cid)
	waitForSync(t, "Watcher 1", dstStore, lnk1.(cidlink.Link), watcher1)
	waitForSync(t, "Watcher 2", dstStore, lnk1.(cidlink.Link), watcher2)

	if err = pub2.UpdateRoot(context.Background(), lnk2.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}
	t.Log("Publish 2:", lnk2.(cidlink.Link).Cid)
	waitForSync(t, "Watcher 1", dstStore, lnk2.(cidlink.Link), watcher1)
	waitForSync(t, "Watcher 2", dstStore, lnk2.(cidlink.Link), watcher2)
}

func waitForSync(t *testing.T, logPrefix string, store *dssync.MutexDatastore, expectedCid cidlink.Link, watcher <-chan legs.SyncFinished) {
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Cid.Equals(expectedCid.Cid) {
			t.Fatalf("sync'd cid unexpected %s vs %s", downstream, expectedCid.Cid)
		}
		if _, err := store.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
		t.Log(logPrefix+" got sync:", downstream.Cid)
	}

}

func TestCloseBroker(t *testing.T) {
	st := dssync.MutexWrap(datastore.NewMapDatastore())
	sh := test.MkTestHost()
	lsys := test.MkLinkSystem(st)

	bkr, err := legs.NewBroker(sh, st, lsys, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}

	watcher, cncl := bkr.OnSyncFinished()
	defer cncl()

	err = bkr.Close()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case _, open := <-watcher:
		if open {
			t.Fatal("Watcher channel should have been closed")
		}
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for watcher to close")
	}

	err = bkr.Close()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		cncl()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(updateTimeout):
		t.Fatal("OnSyncFinished cancel func did not return after Close")
	}
}
