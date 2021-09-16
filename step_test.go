package legs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

func TestSyncSteps(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := mkTestHost()
	srcLnkS := mkLinkSystem(srcStore)
	srcdt, err := MakeLegTransport(context.Background(), srcHost, srcStore, srcLnkS, "legs/testtopic")
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
	dstdt, err := MakeLegTransport(context.Background(), dstHost, dstStore, dstLnkS, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	// Store the whole chain in source node
	_, gitLnks := mkGitLike(srcLnkS)

	// Wait for migrations to be run before performing exchange.
	time.Sleep(500 * time.Millisecond)
	onFinish := func() {
		t.Log("Transfer finished")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	cch, cncl, err := dstdt.Fetch(ctx, srcHost.ID(), gitLnks[0].(cidlink.Link).Cid, LegSelector(nil, "nested"), onFinish)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case c := <-cch:
		if !c.Equals(gitLnks[0].(cidlink.Link).Cid) {
			t.Fatal("the wrong DAG was wxchanged")
		}
		s, _ := dstStore.Get(datastore.NewKey(gitLnks[0].(cidlink.Link).Cid.String()))
		fmt.Println(string(s))
		s, _ = dstStore.Get(datastore.NewKey(gitLnks[1].(cidlink.Link).Cid.String()))
		fmt.Println(string(s))
		s, _ = dstStore.Get(datastore.NewKey(gitLnks[1].(cidlink.Link).Cid.String()))
		fmt.Println(string(s))
	case <-ctx.Done():
		t.Fatal("the exchange timed out")
	}

	t.Cleanup(func() {
		cncl()
		cancel()
		srcdt.Close(context.Background())
		dstdt.Close(context.Background())
	})

}
func stepSelector(field string) ipld.Node {
	np := basicnode.Prototype__Any{}
	ssb := selectorbuilder.NewSelectorSpecBuilder(np)
	efields := ssb.ExploreFields(func(efsb selectorbuilder.ExploreFieldsSpecBuilder) { efsb.Insert(field, ssb.Matcher()) })
	return ssb.ExploreRecursive(selector.RecursionLimitNone(), efields).Node()

}
