package legs_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"

	// dagjson codec registered for encoding
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multicodec"
	"github.com/willscott/go-legs"
)

func mkTestHost() host.Host {
	h, _ := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	return h
}

func TestRoundTrip(t *testing.T) {
	srcHost := mkTestHost()
	srcStore := ds.NewMapDatastore()
	lp, err := legs.Publish(context.Background(), srcStore, srcHost, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	dstHost := mkTestHost()
	dstStore := ds.NewMapDatastore()
	ls, err := legs.Subscribe(context.Background(), dstStore, dstHost, "legs/testtopic")
	if err != nil {
		t.Fatal(err)
	}

	watcher, cncl := ls.OnChange()

	itm := basicnode.NewBytes([]byte("hello world"))
	linkproto := cidlink.LinkBuilder{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}
	storer := func(_ ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return srcStore.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	lnk, err := linkproto.Build(context.Background(), ipld.LinkContext{}, itm, storer)
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
		if _, err := dstStore.Get(ds.RawKey(downstream.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
}
