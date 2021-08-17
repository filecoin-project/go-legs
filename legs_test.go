package legs_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"

	// dagjson codec registered for encoding
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multicodec"
	"github.com/willscott/go-legs"
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

func initPubSub(t *testing.T, srcStore, dstStore datastore.Batching) (host.Host, host.Host, legs.LegPublisher, legs.LegSubscriber) {
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	lp, err := legs.NewPublisher(context.Background(), srcStore, srcHost, "legs/testtopic", srcLnkS)
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
	ls, err := legs.NewSubscriber(context.Background(), dstStore, dstHost, "legs/testtopic", dstLnkS)
	if err != nil {
		t.Fatal(err)
	}
	return srcHost, dstHost, lp, ls
}

func mkRoot(srcStore datastore.Batching, n ipld.Node) (ipld.Link, error) {
	linkproto := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return srcStore.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}

	return lsys.Store(ipld.LinkContext{}, linkproto, n)
}

func TestRoundTrip(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := datastore.NewMapDatastore()
	dstStore := datastore.NewMapDatastore()
	_, _, lp, ls := initPubSub(t, srcStore, dstStore)

	// Fetch-all recursively selector
	np := basicnode.Prototype__Any{}
	ssb := selectorbuilder.NewSelectorSpecBuilder(np)
	sn := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	err := ls.Subscribe(context.Background(), sn, nil)
	if err != nil {
		t.Fatal(err)
	}

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	watcher, cncl := ls.OnChange()

	// Update root with item
	itm := basicnode.NewString("hello world")
	lnk, err := mkRoot(srcStore, itm)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cncl()
		lp.Close(context.Background())
		ls.Close(context.Background())
	}()

	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(lnk.(cidlink.Link).Cid) {
			t.Fatalf("sync'd sid unexpected %s vs %s", downstream, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
}

func TestSubscribeTwiceAndWithPolicy(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := datastore.NewMapDatastore()
	dstStore := datastore.NewMapDatastore()
	srcHost, _, lp, ls := initPubSub(t, srcStore, dstStore)

	// Fetch-all recursively selector
	np := basicnode.Prototype__Any{}
	ssb := selectorbuilder.NewSelectorSpecBuilder(np)
	sn := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	// Test subscribing with no selector. Should return error
	err := ls.Subscribe(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("subscribing without selector should return error")
	}

	// Test subscribing twice, and adding a policy in the second subscribe.
	err = ls.Subscribe(context.Background(), sn, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = ls.Subscribe(context.Background(), sn, legs.FilterPeerPolicy(srcHost.ID()))
	if err != nil {
		t.Fatal(err)
	}

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	watcher, cncl := ls.OnChange()

	// Update root with item
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("hey")
	ma.AssembleValue().AssignString("it works!")
	ma.AssembleKey().AssignString("yes")
	ma.AssembleValue().AssignBool(true)
	ma.Finish()
	n := nb.Build()
	lnk, err := mkRoot(srcStore, n)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cncl()
		lp.Close(context.Background())
		ls.Close(context.Background())
	}()

	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(lnk.(cidlink.Link).Cid) {
			t.Fatalf("sync'd sid unexpected %s vs %s", downstream, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
}

func TestSetAndFilterPeerPolicy(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := datastore.NewMapDatastore()
	dstStore := datastore.NewMapDatastore()
	srcHost, dstHost, lp, ls := initPubSub(t, srcStore, dstStore)

	// Fetch-all recursively selector
	np := basicnode.Prototype__Any{}
	ssb := selectorbuilder.NewSelectorSpecBuilder(np)
	sn := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	err := ls.Subscribe(context.Background(), sn, legs.FilterPeerPolicy(srcHost.ID()))
	if err != nil {
		t.Fatal(err)
	}

	// Set policy to filter dstHost, which is not the one generating the update.
	ls.SetPolicyHandler(legs.FilterPeerPolicy(dstHost.ID()))

	time.Sleep(time.Second)

	watcher, cncl := ls.OnChange()

	// Update root with item
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("hey")
	ma.AssembleValue().AssignString("it works!")
	ma.AssembleKey().AssignString("yes")
	ma.AssembleValue().AssignBool(true)
	ma.Finish()
	n := nb.Build()
	lnk, err := mkRoot(srcStore, n)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cncl()
		lp.Close(context.Background())
		ls.Close(context.Background())
	}()

	if err := lp.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 3):
	case <-watcher:
		t.Fatal("something was exchanged, and that is wrong")
	}
}
