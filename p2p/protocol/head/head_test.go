package head_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p"
)

func TestFetchLatestHead(t *testing.T) {
	publisher, _ := libp2p.New()
	client, _ := libp2p.New()

	// Provide multiaddrs to connect to
	client.Peerstore().AddAddrs(publisher.ID(), publisher.Addrs(), time.Hour)

	publisherStore := dssync.MutexWrap(datastore.NewMapDatastore())
	rootLnk, err := test.Store(publisherStore, basicnode.NewString("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	p := head.NewPublisher()
	go p.Serve(publisher, "test")
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := head.QueryRootCid(ctx, client, "test", publisher.ID())
	if err != nil && c != cid.Undef {
		t.Fatal("Expected to get a nil error and a cid undef because there is no root")
	}

	if err := p.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	c, err = head.QueryRootCid(ctx, client, "test", publisher.ID())
	if err != nil {
		t.Fatal(err)
	}

	if !c.Equals(rootLnk.(cidlink.Link).Cid) {
		t.Fatalf("didn't get expected cid. expected %s, got %s", rootLnk, c)
	}
}
