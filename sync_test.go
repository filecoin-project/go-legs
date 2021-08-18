package legs

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multicodec"
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

func mkChain(lsys ipld.LinkSystem) (ipld.Node, []ipld.Link) {
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
	_, ch2Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("linkedMap").AssignLink(middleMapNodeLnk)
		na.AssembleEntry("ch1").AssignLink(ch1Lnk)
	}))
	_, ch3Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("linkedString").AssignLink(leafAlphaLnk)
		na.AssembleEntry("ch2").AssignLink(ch2Lnk)
	}))

	headNode, headLnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("plain").AssignString("olde string")
		na.AssembleEntry("ch3").AssignLink(ch3Lnk)
	}))
	return headNode, []ipld.Link{headLnk, ch3Lnk, ch2Lnk, ch1Lnk}
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

func mkLevelDs(t *testing.T) datastore.Batching {
	tmpDir, err := ioutil.TempDir("", "go-legs")
	if err != nil {
		t.Fatal(err)
	}
	dstore, err := leveldb.NewDatastore(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dstore
}
func TestLatestSync(t *testing.T) {
	// NOTE: In order to aviod parallel read/write (which makes the test flaky), we need
	// to use a concurrency-friendly datastore like levelDB
	srcStore := mkLevelDs(t)
	dstStore := mkLevelDs(t)
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

	_, chainLnks := mkChain(srcLnkS)

	defer func() {
		cncl()
		lp.Close(context.Background())
		ls.Close(context.Background())
	}()

	newUpdateTest(t, lp, ls, watcher, chainLnks[3])
	newUpdateTest(t, lp, ls, watcher, chainLnks[2])
	newUpdateTest(t, lp, ls, watcher, chainLnks[1])
	newUpdateTest(t, lp, ls, watcher, chainLnks[0])
}

func newUpdateTest(t *testing.T, lp LegPublisher, ls LegSubscriber, watcher chan cid.Cid, lnk ipld.Link) {
	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	lsT := ls.(*legSubscriber)
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
	if lsT.latestSync.(cidlink.Link).Cid != lnk.(cidlink.Link).Cid {
		t.Fatal("latestSync not updated correctly", lsT.latestSync)
	}
}

// NOTE: This test may print datatransfer migration related errors. As long as this test
// pass you don't need to worry about it. However, if it is bothering you, comment or remove
// this test. In the end this a straightforward test which checks if we are initializing
// a partially synced subscriber correctly (which is quite straightforward).
func TestPartiallySynced(t *testing.T) {
	dstStore := datastore.NewMapDatastore()
	dstHost := mkTestHost()
	dstLnkS := mkLinkSystem(dstStore)
	_, chainLnks := mkChain(dstLnkS)
	ls, err := NewSubscriberPartiallySynced(context.Background(), dstStore, dstHost, "legs/testtopic", dstLnkS, nil, chainLnks[0].(cidlink.Link).Cid)
	if err != nil {
		t.Fatal(err)
	}
	if ls.(*legSubscriber).latestSync.(cidlink.Link).Cid != chainLnks[0].(cidlink.Link).Cid {
		t.Fatal("partial synced not set correctly")
	}

	ls, err = NewSubscriberPartiallySynced(context.Background(), dstStore, dstHost, "legs/testtopic", dstLnkS, nil, cid.Undef)
	if err != nil {
		t.Fatal(err)
	}
	if ls.(*legSubscriber).latestSync != nil {
		t.Fatal("cidUndef should set latestSync to nil to avoid errors")
	}

}
