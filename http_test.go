package legs_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestManualSync(t *testing.T) {
	srcHost := test.MkTestHost()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcSys := test.MkLinkSystem(srcStore)
	pub, err := httpsync.NewPublisher("127.0.0.1:0", srcSys, srcHost.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLinkSys := test.MkLinkSystem(dstStore)
	dstHost := test.MkTestHost()

	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(p peer.ID, c cid.Cid) {
		blocksSeenByHook[c] = struct{}{}
		t.Log("http block hook got", c, "from", p)
	}

	sub, err := legs.NewSubscriber(dstHost, dstStore, dstLinkSys, testTopic, nil, legs.BlockHook(blockHook))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	rootLnk, err := test.Store(srcStore, basicnode.NewString("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	syncCid, err := sub.Sync(ctx, srcHost.ID(), cid.Undef, nil, pub.Address())
	if err != nil {
		t.Fatal(err)
	}

	if !syncCid.Equals(rootLnk.(cidlink.Link).Cid) {
		t.Fatalf("didn't get expected cid. expected %s, got %s", rootLnk, syncCid)
	}

	_, ok := blocksSeenByHook[syncCid]
	if !ok {
		t.Fatal("hook did not get", syncCid)
	}
}

func TestSyncFnHttp(t *testing.T) {
	srcHost := test.MkTestHost()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLnkS := test.MkLinkSystem(srcStore)

	dstHost := test.MkTestHost()
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkS := test.MkLinkSystem(dstStore)

	pub, err := httpsync.NewPublisher("127.0.0.1:0", srcLnkS, srcHost.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	var blockHookCalls int
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(_ peer.ID, c cid.Cid) {
		blockHookCalls++
		blocksSeenByHook[c] = struct{}{}
	}

	sub, err := legs.NewSubscriber(dstHost, dstStore, dstLnkS, testTopic, nil, legs.BlockHook(blockHook))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	watcher, cancelWatcher := sub.OnSyncFinished()
	defer cancelWatcher()

	// Try to sync with a non-existing cid to chack that sync returns with err, and SyncFinished watcher does not get event.
	cids, _ := test.RandomCids(1)
	ctx, syncncl := context.WithTimeout(context.Background(), time.Second)
	defer syncncl()
	syncCid, err := sub.Sync(ctx, srcHost.ID(), cids[0], nil, srcHost.Addrs()[0])
	if err == nil {
		t.Fatal("expected error when no content to sync")
	}
	syncncl()

	select {
	case <-time.After(updateTimeout):
	case <-watcher:
		t.Fatal("watcher should not receive event if sync error")
	}

	t.Log("updating root")

	// Assert the latestSync is updated by explicit sync when cid and selector are unset.
	newHead := chainLnks[0].(cidlink.Link).Cid
	if err := pub.UpdateRoot(context.Background(), newHead); err != nil {
		t.Fatal(err)
	}

	lnk := chainLnks[1]

	// Sync with publisher via HTTP.
	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	syncCid, err = sub.Sync(ctx, srcHost.ID(), lnk.(cidlink.Link).Cid, nil, pub.Address())
	if err != nil {
		t.Fatal(err)
	}

	if !syncCid.Equals(lnk.(cidlink.Link).Cid) {
		t.Fatalf("sync'd cid unexpected %s vs %s", syncCid, lnk)
	}
	if _, err := dstStore.Get(datastore.NewKey(syncCid.String())); err != nil {
		t.Fatalf("data not in receiver store: %v", err)
	}
	syncncl()

	_, ok := blocksSeenByHook[lnk.(cidlink.Link).Cid]
	if !ok {
		t.Fatal("block hook did not see link cid")
	}
	if blockHookCalls != 7 {
		t.Fatalf("expected 7 block hook calls, got %d", blockHookCalls)
	}

	// Assert the latestSync is not updated by explicit sync when cid is set
	if sub.GetLatestSync(srcHost.ID()) != nil {
		t.Fatal("Sync should not update latestSync")
	}

	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	syncCid, err = sub.Sync(ctx, srcHost.ID(), cid.Undef, nil, pub.Address())
	if err != nil {
		t.Fatal(err)
	}
	if !syncCid.Equals(newHead) {
		t.Fatalf("sync'd cid unexpected %s vs %s", syncCid, lnk)
	}
	if _, err := dstStore.Get(datastore.NewKey(syncCid.String())); err != nil {
		t.Fatalf("data not in receiver store: %v", err)
	}
	syncncl()

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync from published update")
	case syncFin, open := <-watcher:
		if !open {
			t.Fatal("sync finished channel closed with no event")
		}
		if syncFin.Cid != newHead {
			t.Fatalf("Should have been updated to %s, got %s", newHead, syncFin.Cid)
		}
	}
	cancelWatcher()

	err = assertLatestSyncEquals(sub, srcHost.ID(), newHead)
	if err != nil {
		t.Fatal(err)
	}
}
