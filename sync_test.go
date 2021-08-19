package legs

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multicodec"
	testds "github.com/willscott/go-legs/test/datastore"
)

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
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}

	lnk, err := lsys.Store(ipld.LinkContext{}, lp, n)
	if err != nil {
		panic(err)
	}
	return n, lnk
}

func TestLatestSyncSuccess(t *testing.T) {
	srcStore := testds.NewMapDatastore()
	dstStore := testds.NewMapDatastore()
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcStore, srcHost, "legs/testtopic", srcLnkS)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
	dstLnkS := mkLinkSystem(dstStore)
	ls, err := NewSubscriber(context.Background(), dstStore, dstHost, "legs/testtopic", dstLnkS, nil)
	if err != nil {
		t.Fatal(err)
	}

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	watcher, cncl := ls.OnChange()

	// Store the whole chain in source node
	chainLnks := mkChain(srcLnkS, true)

	defer func() {
		cncl()
		lp.Close(context.Background())
		ls.Close(context.Background())
	}()

	newUpdateTest(t, lp, ls, watcher, chainLnks[2], false, chainLnks[2].(cidlink.Link).Cid)
	newUpdateTest(t, lp, ls, watcher, chainLnks[1], false, chainLnks[1].(cidlink.Link).Cid)
	newUpdateTest(t, lp, ls, watcher, chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)
}

func TestPartialSync(t *testing.T) {
	srcStore := testds.NewMapDatastore()
	dstStore := testds.NewMapDatastore()
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcStore, srcHost, "legs/testtopic", srcLnkS)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
	dstLnkS := mkLinkSystem(dstStore)
	ls, err := NewSubscriber(context.Background(), dstStore, dstHost, "legs/testtopic", dstLnkS, nil)
	if err != nil {
		t.Fatal(err)
	}

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	watcher, cncl := ls.OnChange()

	// Store the whole chain in source node
	chainLnks := mkChain(srcLnkS, true)
	// Store half of the chain already in destination
	// to simulate the partial sync.
	mkChain(dstLnkS, true)

	defer func() {
		cncl()
		lp.Close(context.Background())
		ls.Close(context.Background())
	}()

	newUpdateTest(t, lp, ls, watcher, chainLnks[1], false, chainLnks[1].(cidlink.Link).Cid)
	newUpdateTest(t, lp, ls, watcher, chainLnks[0], false, chainLnks[0].(cidlink.Link).Cid)
}

func TestLatestSyncFailure(t *testing.T) {
	srcStore := testds.NewMapDatastore()
	dstStore := testds.NewMapDatastore()
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := NewPublisher(context.Background(), srcStore, srcHost, "legs/testtopic", srcLnkS)
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
	chainLnks := mkChain(srcLnkS, true)
	dstLnkS := mkLinkSystem(dstStore)
	ls, err := NewSubscriberPartiallySynced(context.Background(), dstStore, dstHost, "legs/testtopic", dstLnkS, nil, chainLnks[3].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	watcher, cncl := ls.OnChange()

	defer func() {
		cncl()
		lp.Close(context.Background())
		ls.Close(context.Background())
	}()

	// The other end doesn't have the data
	newUpdateTest(t, lp, ls, watcher, cidlink.Link{Cid: cid.Undef}, true, chainLnks[3].(cidlink.Link).Cid)

	dstStore = testds.NewMapDatastore()
	ls, err = NewSubscriberPartiallySynced(context.Background(), dstStore, dstHost, "legs/testtopic", dstLnkS, nil, chainLnks[3].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}
	// We are not able to run the full exchange
	newUpdateTest(t, lp, ls, watcher, chainLnks[2], true, chainLnks[3].(cidlink.Link).Cid)
}

func newUpdateTest(t *testing.T, lp LegPublisher, ls LegSubscriber, watcher chan cid.Cid, lnk ipld.Link, withFailure bool, expectedSync cid.Cid) {
	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	lsT := ls.(*legSubscriber)

	// If failure latestSync shouldn't be updated
	if withFailure {
		select {
		case <-time.After(time.Second * 2):
			if lsT.latestSync.(cidlink.Link).Cid != expectedSync {
				t.Fatal("latestSync shouldn't have been updated", lsT.latestSync)
			}
		case <-watcher:
			t.Fatal("no exchange should have been performed")
		}
	} else {
		select {
		case <-time.After(time.Second * 2):
			t.Fatal("timed out waiting for sync to propogate")
		case downstream := <-watcher:
			if !downstream.Equals(lnk.(cidlink.Link).Cid) {
				t.Fatalf("sync'd sid unexpected %s vs %s", downstream, lnk)
			}
			if _, err := lsT.ds.Get(datastore.NewKey(downstream.String())); err != nil {
				t.Fatalf("data not in receiver store: %v", err)
			}
		}
		if lsT.latestSync.(cidlink.Link).Cid != expectedSync {
			t.Fatal("latestSync not updated correctly", lsT.latestSync)
		}
	}
}
