package legs

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

func TestFetch(t *testing.T) {
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
	gitLnks := mkChain(srcLnkS, true)
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

// mkGitlike creates a DAG that represent a chain of subDAGs.
// All of the nodes of the chain are the same, and the bypass
// condition should prevent from recursing in specific fields.
// We will traverse the full chain but prevent recursion for
// certain fields
func mkGitLike(lsys ipld.LinkSystem) (datamodel.Node, []datamodel.Link) {
	_, leafAlphaLnk := encode(lsys, basicnode.NewString("alpha"))
	_, mapLnk1 := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("alink").AssignLink(leafAlphaLnk)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))
	_, mapLnk2 := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("alink").AssignLink(leafAlphaLnk)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))
	_, mapLnk3 := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("alink").AssignLink(leafAlphaLnk)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))

	_, ch1Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("genesis").AssignLink(mapLnk1)
	}))
	_, ch2Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("ch2").AssignLink(mapLnk2)
		na.AssembleEntry("ch1").AssignLink(ch1Lnk)
	}))
	headNode, headLnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("ch3").AssignLink(mapLnk3)
		na.AssembleEntry("ch2").AssignLink(ch2Lnk)
	}))

	return headNode, []datamodel.Link{headLnk, ch2Lnk, ch1Lnk}
}
