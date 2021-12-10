package legs

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestBrokerLatestSyncSuccess(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)

	lb, err := NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	watcher, cncl := lb.OnSyncFinished()

	// Store the whole chain in source node
	chainLnks := mkChain(srcLnkS, true)

	t.Cleanup(func() {
		cncl()
		lp.Close()
		lb.Close()
	})

	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[2], false, chainLnks[2].(cidlink.Link).Cid)
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[1], false, chainLnks[1].(cidlink.Link).Cid)
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)
}

func TestBrokerSyncFn(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)

	lb, err := NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	t.Cleanup(func() {
		lp.Close()
		lb.Close()
	})

	// Store the whole chain in source node
	chainLnks := mkChain(srcLnkS, true)

	// Try to sync with a non-existing cid, and cancel right away.
	// This is to check that we unlock syncmtx if the exchange is cancelled.
	cids, _ := RandomCids(1)

	ctx, syncncl := context.WithCancel(context.Background())
	defer syncncl()
	_, err = lb.Sync(ctx, srcHost.ID(), cids[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	// Cancel without any exchange being done.
	syncncl()

	lnk := chainLnks[1]
	// Proactively sync with publisher without him publishing to gossipsub channel.
	ctx, syncncl = context.WithCancel(context.Background())
	defer syncncl()
	out, err := lb.Sync(ctx, srcHost.ID(), lnk.(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(time.Second * 2):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-out:
		if !downstream.Cid.Equals(lnk.(cidlink.Link).Cid) {
			t.Fatalf("sync'd cid unexpected %s vs %s", downstream.Cid, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
	// Stop listening to sync events.
	syncncl()

	// Assert the latestSync is not updated by explicit sync when cid is set
	if lb.GetLatestSync(srcHost.ID()) != nil {
		t.Fatal("Sync should not update latestSync")
	}

	// Assert the latestSync is updated by explicit sync when cid and selector are unset
	newHead := chainLnks[0].(cidlink.Link).Cid
	if err := lp.UpdateRoot(context.Background(), newHead); err != nil {
		t.Fatal(err)
	}

	ctx, syncncl = context.WithCancel(context.Background())
	defer syncncl()
	out, err = lb.Sync(ctx, srcHost.ID(), cid.Undef, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(time.Second * 2):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-out:
		if !downstream.Cid.Equals(newHead) {
			t.Fatalf("sync'd cid unexpected %s vs %s", downstream.Cid, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
	syncncl()
	assertBrokerLatestSyncEquals(t, lb, srcHost.ID(), newHead)
}

func TestBrokerPartialSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	testStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	testLnkS := mkLinkSystem(testStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	chainLnks := mkChain(testLnkS, true)

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)

	lb, err := NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = lb.SetLatestSync(srcHost.ID(), chainLnks[3].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}

	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	mkChain(srcLnkS, true)

	time.Sleep(2 * time.Second)

	watcher, cncl := lb.OnSyncFinished()

	t.Cleanup(func() {
		lp.Close()
		lb.Close()
		cncl()
	})

	// Fetching first few nodes.
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[2], false, chainLnks[2].(cidlink.Link).Cid)

	// Check that first nodes hadn't been synced
	if _, err := dstStore.Get(datastore.NewKey(chainLnks[3].(cidlink.Link).Cid.String())); err != datastore.ErrNotFound {
		t.Fatalf("data should not be in receiver store: %v", err)
	}

	// Set latest sync so we pass through one of the links
	err = lb.SetLatestSync(srcHost.ID(), chainLnks[1].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}
	assertBrokerLatestSyncEquals(t, lb, srcHost.ID(), chainLnks[1].(cidlink.Link).Cid)
	// Update all the chain from scratch again.
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)

	// Check if the node we pass through was retrieved
	if _, err := dstStore.Get(datastore.NewKey(chainLnks[1].(cidlink.Link).Cid.String())); err != datastore.ErrNotFound {
		t.Fatalf("data should not be in receiver store: %v", err)
	}
}
func TestBrokerStepByStepSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)

	lb, err := NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	watcher, cncl := lb.OnSyncFinished()

	// Store the whole chain in source node
	chainLnks := mkChain(srcLnkS, true)

	// Store half of the chain already in destination
	// to simulate the partial sync.
	mkChain(dstLnkS, true)

	t.Cleanup(func() {
		lp.Close()
		lb.Close()
		cncl()
	})

	// Sync the rest of the chain
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[1], false, chainLnks[1].(cidlink.Link).Cid)
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)
}

func TestBrokerLatestSyncFailure(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	chainLnks := mkChain(srcLnkS, true)

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)

	t.Log("source host:", srcHost.ID())
	t.Log("targer host:", dstHost.ID())

	lb, err := NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	err = lb.SetLatestSync(srcHost.ID(), chainLnks[3].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}
	watcher, cncl := lb.OnSyncFinished()

	t.Log("Testing sync fail when the other end does not have the data")
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), cidlink.Link{Cid: cid.Undef}, true, chainLnks[3].(cidlink.Link).Cid)
	cncl()
	lb.Close()

	dstStore = dssync.MutexWrap(datastore.NewMapDatastore())
	lb, err = NewBroker(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = lb.SetLatestSync(srcHost.ID(), chainLnks[3].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}
	watcher, cncl = lb.OnSyncFinished()

	t.Cleanup(func() {
		lp.Close()
		lb.Close()
		cncl()
	})
	t.Log("Testing sync fail when not able to run the full exchange")
	newBrokerUpdateTest(t, lp, lb, dstStore, watcher, srcHost.ID(), chainLnks[2], true, chainLnks[3].(cidlink.Link).Cid)
}

func newBrokerUpdateTest(t *testing.T, lp LegPublisher, lb *Broker, dstStore datastore.Batching, watcher <-chan SyncFinished, peerID peer.ID, lnk ipld.Link, withFailure bool, expectedSync cid.Cid) {
	var err error

	c := lnk.(cidlink.Link).Cid
	if c != cid.Undef {
		err := lp.UpdateRoot(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
	}

	// If failure. then latestSync should not be updated.
	if withFailure {
		select {
		case <-time.After(time.Second * 5):
			assertBrokerLatestSyncEquals(t, lb, peerID, expectedSync)
		case changeEvent, open := <-watcher:
			if !open {
				return
			}
			t.Fatalf("no exchange should have been performed, but got change from peer %s for cid %s", changeEvent.PeerID, changeEvent.Cid)
		}
	} else {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timed out waiting for sync to propagate")
		case downstream, open := <-watcher:
			if !open {
				return
			}
			if !downstream.Cid.Equals(c) {
				t.Fatalf("sync'd cid unexpected %s vs %s", downstream.Cid, c)
			}
			if _, err = dstStore.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
				t.Fatalf("data not in receiver store: %v", err)
			}
		}
		assertBrokerLatestSyncEquals(t, lb, peerID, expectedSync)
	}
}

func assertBrokerLatestSyncEquals(t *testing.T, lb *Broker, peerID peer.ID, want cid.Cid) {
	latest := lb.GetLatestSync(peerID)
	if latest == nil {
		t.Fatal("latest sync is nil")
	}
	got := latest.(cidlink.Link)
	if got.Cid != want {
		t.Fatalf("latestSync not updated correctly, got %s want %s", got, want)
	}
}
