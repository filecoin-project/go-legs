package legs_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

const testTopic = "/legs/testtopic"

func TestLatestSyncSuccess(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)
	lp, err := dtsync.NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)
	ls, err := dtsync.NewSubscriber(context.Background(), dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	watcher, cncl := ls.OnChange()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[2], false, chainLnks[2].(cidlink.Link).Cid)
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[1], false, chainLnks[1].(cidlink.Link).Cid)
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)

	cncl()
	lp.Close()
	ls.Close()
}

func TestSyncFn(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)
	lp, err := dtsync.NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	ls, err := dtsync.NewSubscriber(context.Background(), dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	t.Cleanup(func() {
		lp.Close()
		ls.Close()
	})

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	// Try to sync with a non-existing cid, and cancel right away.
	// This is to check that we unlock syncmtx if the exchange is cancelled.
	cids, _ := test.RandomCids(1)
	_, syncncl, err := ls.Sync(context.Background(), srcHost.ID(), cids[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	// Cancel without any exchange being done.
	syncncl()

	lnk := chainLnks[1]
	// Proactively sync with publisher without him publishing to gossipsub channel.
	out, syncncl, err := ls.Sync(context.Background(), srcHost.ID(), lnk.(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(time.Second * 2):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-out:
		if !downstream.Equals(lnk.(cidlink.Link).Cid) {
			t.Fatalf("sync'd cid unexpected %s vs %s", downstream, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
	// Stop listening to sync events.
	syncncl()

	// Assert the latestSync is not updated by explicit sync when cid is set
	if ls.LatestSync() != nil {
		t.Fatal("Sync should not update latestSync")
	}

	// Assert the latestSync is updated by explicit sync when cid and selector are unset
	newHead := chainLnks[0].(cidlink.Link).Cid
	if err := lp.UpdateRoot(context.Background(), newHead); err != nil {
		t.Fatal(err)
	}

	out, syncncl, err = ls.Sync(context.Background(), srcHost.ID(), cid.Undef, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(time.Second * 2):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-out:
		if !downstream.Equals(newHead) {
			t.Fatalf("sync'd cid unexpected %s vs %s", downstream, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
	syncncl()
	assertLatestSyncEquals(t, ls, newHead)
}

func TestPartialSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	testStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)
	testLnkS := test.MkLinkSystem(testStore)
	lp, err := dtsync.NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	chainLnks := test.MkChain(testLnkS, true)

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)
	ls, err := dtsync.NewSubscriberPartiallySynced(context.Background(), dstHost, dstStore, dstLnkS, testTopic, chainLnks[3].(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	test.MkChain(srcLnkS, true)

	time.Sleep(2 * time.Second)

	watcher, cncl := ls.OnChange()

	t.Cleanup(clean(lp, ls, cncl))

	// Fetching first few nodes.
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[2], false, chainLnks[2].(cidlink.Link).Cid)

	// Check that first nodes hadn't been synced
	if _, err := dstStore.Get(datastore.NewKey(chainLnks[3].(cidlink.Link).Cid.String())); err != datastore.ErrNotFound {
		t.Fatalf("data should not be in receiver store: %v", err)
	}

	// Set latest sync so we pass through one of the links
	err = ls.SetLatestSync(chainLnks[1].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}
	assertLatestSyncEquals(t, ls, chainLnks[1].(cidlink.Link).Cid)
	// Update all the chain from scratch again.
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)

	// Check if the node we pass through was retrieved
	if _, err := dstStore.Get(datastore.NewKey(chainLnks[1].(cidlink.Link).Cid.String())); err != datastore.ErrNotFound {
		t.Fatalf("data should not be in receiver store: %v", err)
	}
}
func TestStepByStepSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)
	lp, err := dtsync.NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)
	ls, err := dtsync.NewSubscriber(context.Background(), dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	watcher, cncl := ls.OnChange()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	// Store half of the chain already in destination
	// to simulate the partial sync.
	test.MkChain(dstLnkS, true)

	t.Cleanup(clean(lp, ls, cncl))

	// Sync the rest of the chain
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[1], false, chainLnks[1].(cidlink.Link).Cid)
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)

}

func TestLatestSyncFailure(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)
	lp, err := dtsync.NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}

	chainLnks := test.MkChain(srcLnkS, true)

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
	dstLnkS := test.MkLinkSystem(dstStore)
	ls, err := dtsync.NewSubscriberPartiallySynced(context.Background(), dstHost, dstStore, dstLnkS, testTopic, chainLnks[3].(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}

	watcher, cncl := ls.OnChange()
	// The other end doesn't have the data
	newUpdateTest(t, lp, ls, dstStore, watcher, cidlink.Link{Cid: cid.Undef}, true, chainLnks[3].(cidlink.Link).Cid)
	ls.Close()
	cncl()

	dstStore = dssync.MutexWrap(datastore.NewMapDatastore())
	ls, err = dtsync.NewSubscriberPartiallySynced(context.Background(), dstHost, dstStore, dstLnkS, testTopic, chainLnks[3].(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}
	watcher, cncl = ls.OnChange()

	t.Cleanup(clean(lp, ls, cncl))
	// We are not able to run the full exchange
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[2], true, chainLnks[3].(cidlink.Link).Cid)
}

func newUpdateTest(t *testing.T, lp legs.LegPublisher, ls legs.LegSubscriber, dstStore datastore.Batching, watcher chan cid.Cid, lnk ipld.Link, withFailure bool, expectedSync cid.Cid) {
	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	// If failure latestSync shouldn't be updated
	if withFailure {
		select {
		case <-time.After(time.Second * 5):
			assertLatestSyncEquals(t, ls, expectedSync)
		case <-watcher:
			t.Fatal("no exchange should have been performed")
		}
	} else {
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timed out waiting for sync to propagate")
		case downstream := <-watcher:
			if !downstream.Equals(lnk.(cidlink.Link).Cid) {
				t.Fatalf("sync'd cid unexpected %s vs %s", downstream, lnk)
			}
			if _, err := dstStore.Get(datastore.NewKey(downstream.String())); err != nil {
				t.Fatalf("data not in receiver store: %v", err)
			}
		}
		assertLatestSyncEquals(t, ls, expectedSync)
	}
}

func assertLatestSyncEquals(t *testing.T, sub legs.LegSubscriber, want cid.Cid) {
	got := sub.LatestSync().(cidlink.Link)
	if got.Cid != want {
		t.Fatal("latestSync not updated correctly", got)
	}
}

func clean(lp legs.LegPublisher, ls legs.LegSubscriber, cncl context.CancelFunc) func() {
	return func() {
		cncl()
		lp.Close()
		ls.Close()
	}
}
