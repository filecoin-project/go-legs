package http_test

import (
	"context"
	"net"
	nhttp "net/http"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs/http"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

func TestManualSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcSys := test.MkLinkSystem(srcStore)
	p, err := http.NewPublisher(context.Background(), srcStore, srcSys)
	if err != nil {
		t.Fatal(err)
	}
	nl, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = nhttp.Serve(nl, p.(nhttp.Handler))
	}()
	nlm, err := manet.FromNetAddr(nl.Addr())
	if err != nil {
		t.Fatal(err)
	}
	proto, _ := multiaddr.NewMultiaddr("/http")
	nlm = multiaddr.Join(nlm, proto)

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstSys := test.MkLinkSystem(dstStore)
	s, err := http.NewHTTPSubscriber(context.Background(), nhttp.DefaultClient, nlm, &dstSys, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	rootLnk, err := test.Store(srcStore, basicnode.NewString("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	cchan, cncl, err := s.Sync(context.Background(), peer.NewPeerRecord().PeerID, cid.Undef, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case rc := <-cchan:
		if !rc.Equals(rootLnk.(cidlink.Link).Cid) {
			t.Fatalf("didn't get expected cid. expected %s, got %s", rootLnk, rc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	cncl()
}
