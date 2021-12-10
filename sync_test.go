package legs

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multicodec"
)

var prefix = cid.Prefix{
	Version:  1,
	Codec:    uint64(multicodec.DagJson),
	MhType:   uint64(multicodec.Sha2_256),
	MhLength: 16,
}

func mkTestHost() host.Host {
	h, _ := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	return h
}

func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func RandomCids(n int) ([]cid.Cid, error) {
	var prng = rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		res[i] = c
	}
	return res, nil
}

// Return the chain with all nodes or just half of it for testing
func mkChain(lsys ipld.LinkSystem, full bool) []ipld.Link {
	out := make([]ipld.Link, 4)
	_, leafAlphaLnk := encode(lsys, basicnode.NewString("alpha"))
	_, leafBetaLnk := encode(lsys, basicnode.NewString("beta"))
	_, middleMapNodeLnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("alink").AssignLink(leafAlphaLnk)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))
	_, middleListNodeLnk := encode(lsys, fluent.MustBuildList(basicnode.Prototype__List{}, 4, func(na fluent.ListAssembler) {
		na.AssembleValue().AssignLink(leafAlphaLnk)
		na.AssembleValue().AssignLink(leafAlphaLnk)
		na.AssembleValue().AssignLink(leafBetaLnk)
		na.AssembleValue().AssignLink(leafAlphaLnk)
	}))

	_, ch1Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("linkedList").AssignLink(middleListNodeLnk)
	}))
	out[3] = ch1Lnk
	_, ch2Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("linkedMap").AssignLink(middleMapNodeLnk)
		na.AssembleEntry("ch1").AssignLink(ch1Lnk)
	}))
	out[2] = ch2Lnk
	if full {
		_, ch3Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
			na.AssembleEntry("linkedString").AssignLink(leafAlphaLnk)
			na.AssembleEntry("ch2").AssignLink(ch2Lnk)
		}))
		out[1] = ch3Lnk
		_, headLnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
			na.AssembleEntry("plain").AssignString("olde string")
			na.AssembleEntry("ch3").AssignLink(ch3Lnk)
		}))
		out[0] = headLnk
	}
	return out
}

// encode hardcodes some encoding choices for ease of use in fixture generation;
// just gimme a link and stuff the bytes in a map.
// (also return the node again for convenient assignment.)
func encode(lsys ipld.LinkSystem, n ipld.Node) (ipld.Node, ipld.Link) {
	lp := cidlink.LinkPrototype{
		Prefix: prefix,
	}

	lnk, err := lsys.Store(ipld.LinkContext{}, lp, n)
	if err != nil {
		panic(err)
	}
	return n, lnk
}

func TestLatestSyncSuccess(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)
	ls, err := NewSubscriber(context.Background(), dstHost, dstStore, dstLnkS, "legs/testtopic", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	watcher, cncl := ls.OnChange()

	// Store the whole chain in source node
	chainLnks := mkChain(srcLnkS, true)

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
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)

	ls, err := NewSubscriber(context.Background(), dstHost, dstStore, dstLnkS, "legs/testtopic", nil)
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
	chainLnks := mkChain(srcLnkS, true)

	// Try to sync with a non-existing cid, and cancel right away.
	// This is to check that we unlock syncmtx if the exchange is cancelled.
	cids, _ := RandomCids(1)
	_, syncncl, err := ls.Sync(context.Background(), srcHost.ID(), cids[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	// Cancel without any exchange being done.
	syncncl()

	lnk := chainLnks[1]
	lsT := ls.(*legSubscriber)
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
	if lsT.getLatestSync() != nil {
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
	assertLatestSyncEquals(t, lsT, newHead)
}

func TestPartialSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	testStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	testLnkS := mkLinkSystem(testStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	chainLnks := mkChain(testLnkS, true)

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)
	ls, err := NewSubscriberPartiallySynced(context.Background(), dstHost, dstStore, dstLnkS, "legs/testtopic", chainLnks[3].(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	mkChain(srcLnkS, true)

	time.Sleep(2 * time.Second)

	watcher, cncl := ls.OnChange()

	t.Cleanup(clean(lp, ls, cncl))

	// Fetching first few nodes.
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[2], false, chainLnks[2].(cidlink.Link).Cid)

	// Check that first nodes hadn't been synced
	lsT := ls.(*legSubscriber)
	if _, err := dstStore.Get(datastore.NewKey(chainLnks[3].(cidlink.Link).Cid.String())); err != datastore.ErrNotFound {
		t.Fatalf("data should not be in receiver store: %v", err)
	}

	// Set latest sync so we pass through one of the links
	err = ls.SetLatestSync(chainLnks[1].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}
	assertLatestSyncEquals(t, lsT, chainLnks[1].(cidlink.Link).Cid)
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
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := mkLinkSystem(dstStore)
	ls, err := NewSubscriber(context.Background(), dstHost, dstStore, dstLnkS, "legs/testtopic", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	watcher, cncl := ls.OnChange()

	// Store the whole chain in source node
	chainLnks := mkChain(srcLnkS, true)

	// Store half of the chain already in destination
	// to simulate the partial sync.
	mkChain(dstLnkS, true)

	t.Cleanup(clean(lp, ls, cncl))

	// Sync the rest of the chain
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[1], false, chainLnks[1].(cidlink.Link).Cid)
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)

}

func TestLatestSyncFailure(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcHost, srcStore, srcLnkS, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	chainLnks := mkChain(srcLnkS, true)

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
	dstLnkS := mkLinkSystem(dstStore)
	ls, err := NewSubscriberPartiallySynced(context.Background(), dstHost, dstStore, dstLnkS, "legs/testtopic", chainLnks[3].(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}

	watcher, cncl := ls.OnChange()
	// The other end doesn't have the data
	newUpdateTest(t, lp, ls, dstStore, watcher, cidlink.Link{Cid: cid.Undef}, true, chainLnks[3].(cidlink.Link).Cid)
	ls.Close()
	cncl()

	dstStore = dssync.MutexWrap(datastore.NewMapDatastore())
	ls, err = NewSubscriberPartiallySynced(context.Background(), dstHost, dstStore, dstLnkS, "legs/testtopic", chainLnks[3].(cidlink.Link).Cid, nil)
	if err != nil {
		t.Fatal(err)
	}
	watcher, cncl = ls.OnChange()

	t.Cleanup(clean(lp, ls, cncl))
	// We are not able to run the full exchange
	newUpdateTest(t, lp, ls, dstStore, watcher, chainLnks[2], true, chainLnks[3].(cidlink.Link).Cid)
}

func newUpdateTest(t *testing.T, lp LegPublisher, ls LegSubscriber, dstStore datastore.Batching, watcher chan cid.Cid, lnk ipld.Link, withFailure bool, expectedSync cid.Cid) {
	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	lsT := ls.(*legSubscriber)

	// If failure latestSync shouldn't be updated
	if withFailure {
		select {
		case <-time.After(time.Second * 5):
			assertLatestSyncEquals(t, lsT, expectedSync)
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
		assertLatestSyncEquals(t, lsT, expectedSync)
	}
}

func assertLatestSyncEquals(t *testing.T, sub *legSubscriber, want cid.Cid) {
	got := sub.getLatestSync().(cidlink.Link)
	if got.Cid != want {
		t.Fatal("latestSync not updated correctly", got)
	}
}

func clean(lp LegPublisher, ls LegSubscriber, cncl context.CancelFunc) func() {
	return func() {
		cncl()
		lp.Close()
		ls.Close()
	}
}
