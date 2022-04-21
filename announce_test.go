package legs

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

const (
	testTopic     = "/legs/testtopic"
	updateTimeout = 1 * time.Second
)

func TestAnnounceReplace(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)

	dstHost := test.MkTestHost()

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	pub, err := dtsync.NewPublisher(srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	sub, err := NewSubscriber(dstHost, dstStore, dstLnkS, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	hnd, err := sub.getOrCreateHandler(srcHost.ID(), true)
	if err != nil {
		t.Fatal(err)
	}
	// Lock mutex inside sync handler to simulate publisher blocked in graphsync.
	hnd.syncMutex.Lock()

	firstCid := chainLnks[2].(cidlink.Link).Cid
	err = pub.SetRoot(context.Background(), firstCid)
	if err != nil {
		t.Fatal(err)
	}

	// Have the subscriber receive an announce.  This is the same as if it was
	// published by the publisher without having to wait for it to arrive.
	err = sub.Announce(context.Background(), firstCid, srcHost.ID(), srcHost.Addrs())
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Sent announce for first CID", firstCid)
	// This first announce should start the handler goroutine and clear the
	// pending cid.
	//
	// Once the latestSyncMu cannot be acquired, that means that the handler is
	// running (and blocked).
	var i int
	for {
		if !hnd.latestSyncMu.TryLock() {
			break
		}
		hnd.latestSyncMu.Unlock()
		time.Sleep(time.Millisecond)
		i++
		if i > 100 {
			t.Fatal("handler did not start")
		}
	}

	hnd.qlock.Lock()
	pendingCid := hnd.pendingCid
	hnd.qlock.Unlock()
	if pendingCid != cid.Undef {
		t.Fatal("wrong pending cid, expected cid.Undef")
	}

	// Announce two more times.
	c := chainLnks[1].(cidlink.Link).Cid
	err = pub.SetRoot(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	err = sub.Announce(context.Background(), c, srcHost.ID(), srcHost.Addrs())
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Sent announce for second CID", c)
	lastCid := chainLnks[0].(cidlink.Link).Cid
	err = pub.SetRoot(context.Background(), lastCid)
	if err != nil {
		t.Fatal(err)
	}
	err = sub.Announce(context.Background(), lastCid, srcHost.ID(), srcHost.Addrs())
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Sent announce for last CID", lastCid)
	// Check that the pending CID is set to the last one announced.
	hnd.qlock.Lock()
	pendingCid = hnd.pendingCid
	hnd.qlock.Unlock()
	if pendingCid != lastCid {
		t.Fatal("wrong pending cid")
	}

	// Unblock the first handler goroutine
	hnd.syncMutex.Unlock()

	// Validate that sink for first CID happend.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher:
		if !open {
			t.Fatal("event channle closed without receiving event")
		}
		if !downstream.Cid.Equals(firstCid) {
			t.Fatalf("sync returned unexpected first cid %s, expected %s", downstream.Cid, firstCid)
		}
		if _, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String())); err != nil {
			t.Fatalf("data not in receiver store: %s", err)
		}
		t.Log("Received sync notification for first CID:", firstCid)
	}

	// Validate that sink for last CID happend.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher:
		if !open {
			t.Fatal("event channle closed without receiving event")
		}
		if !downstream.Cid.Equals(lastCid) {
			t.Fatalf("sync returned unexpected last cid %s, expected %s", downstream.Cid, lastCid)
		}
		if _, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String())); err != nil {
			t.Fatalf("data not in receiver store: %s", err)
		}
		t.Log("Received sync notification for last CID:", lastCid)
	}

	// Validate that no additional updates happen.
	select {
	case <-time.After(3 * time.Second):
	case changeEvent, open := <-watcher:
		if open {
			t.Fatalf("no exchange should have been performed, but got change from peer %s for cid %s",
				changeEvent.PeerID, changeEvent.Cid)
		}
	}
}
