package legs_test

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/httpsync"
	maurl "github.com/filecoin-project/go-legs/httpsync/multiaddr"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

func TestManualSync(t *testing.T) {
	srcHost := test.MkTestHost()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcSys := test.MkLinkSystem(srcStore)
	pub := httpsync.NewPublisher(context.Background(), srcStore, srcSys, srcHost.ID(), nil)
	defer pub.Close()

	nl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = http.Serve(nl, pub)
	}()

	nlm, err := manet.FromNetAddr(nl.Addr())
	if err != nil {
		t.Fatal(err)
	}
	proto, _ := multiaddr.NewMultiaddr("/http")
	nlm = multiaddr.Join(nlm, proto)

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLinkSys := test.MkLinkSystem(dstStore)
	dstHost := test.MkTestHost()

	bkr, err := legs.NewBroker(dstHost, dstStore, dstLinkSys, testTopic, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bkr.Close()

	rootLnk, err := test.Store(srcStore, basicnode.NewString("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	pubAddr := nl.Addr()
	u, err := url.Parse("http://" + pubAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Log("HTTP publisher listening on", u.String())
	mu, err := maurl.ToMA(u)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cchan, err := bkr.Sync(ctx, srcHost.ID(), cid.Undef, nil, *mu)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case rc := <-cchan:
		if !rc.Cid.Equals(rootLnk.(cidlink.Link).Cid) {
			t.Fatalf("didn't get expected cid. expected %s, got %s", rootLnk, rc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}
