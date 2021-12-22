package legs_test

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	dt "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestMain(m *testing.M) {
	if runtime.GOARCH == "386" {
		log.Println("Skipping tests, cannot use GOARCH=386")
		return
	}

	// Run tests.
	os.Exit(m.Run())
}

func initPubSub(t *testing.T, srcStore, dstStore datastore.Batching) (host.Host, host.Host, legs.Publisher, *legs.Subscriber, error) {
	srcHost := test.MkTestHost()
	dstHost := test.MkTestHost()

	topics := test.WaitForMeshWithMessage(t, testTopic, srcHost, dstHost)

	srcLnkS := test.MkLinkSystem(srcStore)

	pub, err := dtsync.NewPublisher(srcHost, srcStore, srcLnkS, testTopic, dtsync.Topic(topics[0]))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	sub, err := legs.NewSubscriber(dstHost, dstStore, dstLnkS, testTopic, nil, legs.Topic(topics[1]))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		return nil, nil, nil, nil, err
	}

	return srcHost, dstHost, pub, sub, nil
}

func TestRoundTripExistingDataTransfer(t *testing.T) {
	// Init legs publisher and subscriber
	srcHost := test.MkTestHost()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	fakeLsys := cidlink.DefaultLinkSystem()
	srcLnkS := test.MkLinkSystem(srcStore)

	gsNet := gsnet.NewFromLibp2pHost(srcHost)
	dtNet := dtnetwork.NewFromLibp2pHost(srcHost)
	gs := gsimpl.New(context.Background(), gsNet, fakeLsys)
	tp := gstransport.NewTransport(srcHost.ID(), gs, dtNet)

	// DataTransfer channels use this file to track cidlist of exchanges
	// NOTE: It needs to be initialized for the datatransfer not to fail, but
	// it has no other use outside the cidlist, so I don't think it should be
	// exposed publicly. It's only used for the life of a data transfer.
	// In the future, once an empty directory is accepted as input, it
	// this may be removed.
	tmpDir, err := ioutil.TempDir("", "go-legs")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dtManager, err := dt.NewDataTransfer(srcStore, tmpDir, dtNet, tp)
	if err != nil {
		t.Fatal(err)
	}
	err = dtManager.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dtManager.Stop(context.Background())

	dstHost := test.MkTestHost()
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkS := test.MkLinkSystem(dstStore)

	topics := test.WaitForMeshWithMessage(t, testTopic, srcHost, dstHost)

	pub, err := dtsync.NewPublisherFromExisting(dtManager, srcHost, testTopic, srcLnkS, dtsync.Topic(topics[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	gsNetDst := gsnet.NewFromLibp2pHost(dstHost)
	dtNetDst := dtnetwork.NewFromLibp2pHost(dstHost)
	gsDst := gsimpl.New(context.Background(), gsNetDst, dstLnkS)
	tpDst := gstransport.NewTransport(dstHost.ID(), gsDst, dtNetDst)

	// DataTransfer channels use this file to track cidlist of exchanges
	// NOTE: It needs to be initialized for the datatransfer not to fail, but
	// it has no other use outside the cidlist, so I don't think it should be
	// exposed publicly. It's only used for the life of a data transfer.
	// In the future, once an empty directory is accepted as input, it
	// this may be removed.
	tmpDirDst, err := ioutil.TempDir("", "go-legs")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDirDst)
	dtManagerDst, err := dt.NewDataTransfer(dstStore, tmpDirDst, dtNetDst, tpDst)
	if err != nil {
		t.Fatal(err)
	}
	err = dtManagerDst.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dtManagerDst.Stop(context.Background())

	allowAll := func(_ peer.ID) (bool, error) {
		return true, nil
	}

	sub, err := legs.NewSubscriber(dstHost, dstStore, dstLnkS, testTopic, nil, legs.Topic(topics[1]), legs.DtManager(dtManagerDst), legs.AllowPeer(allowAll), legs.HttpClient(http.DefaultClient), legs.AddrTTL(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Update root with item
	itm := basicnode.NewString("hello world")
	lnk, err := test.Store(srcStore, itm)
	if err != nil {
		t.Fatal(err)
	}

	if err := pub.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Cid.Equals(lnk.(cidlink.Link).Cid) {
			t.Fatalf("sync'd cid unexpected %s vs %s", downstream.Cid, lnk)
		}
		if _, err := dstStore.Get(datastore.NewKey(downstream.Cid.String())); err != nil {
			t.Fatalf("data not in receiver store: %v", err)
		}
	}
}

func TestAllowPeerReject(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	_, dstHost, pub, sub, err := initPubSub(t, srcStore, dstStore)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	defer sub.Close()

	// Set function to reject anything except dstHost, which is not the one
	// generating the update.
	sub.SetAllowPeer(func(peerID peer.ID) (bool, error) {
		return peerID == dstHost.ID(), nil
	})

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	c := mkLnk(t, srcStore)

	// Update root with item
	err = pub.UpdateRoot(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(time.Second * 3):
	case _, open := <-watcher:
		if open {
			t.Fatal("something was exchanged, and that is wrong")
		}
	}
}

func TestAllowPeerAllows(t *testing.T) {
	// Init legs publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	_, _, pub, sub, err := initPubSub(t, srcStore, dstStore)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	defer sub.Close()

	// Set function to allow any peer.
	sub.SetAllowPeer(func(peerID peer.ID) (bool, error) {
		return true, nil
	})

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	c := mkLnk(t, srcStore)

	// Update root with item
	err = pub.UpdateRoot(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for SyncFinished")
	case <-watcher:
	}
}

func mkLnk(t *testing.T, srcStore datastore.Batching) cid.Cid {
	// Update root with item
	np := basicnode.Prototype__Any{}
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	err := ma.AssembleKey().AssignString("hey")
	if err != nil {
		t.Fatal(err)
	}
	if err = ma.AssembleValue().AssignString("it works!"); err != nil {
		t.Fatal(err)
	}
	if err = ma.AssembleKey().AssignString("yes"); err != nil {
		t.Fatal(err)
	}
	if err = ma.AssembleValue().AssignBool(true); err != nil {
		t.Fatal(err)
	}
	if err = ma.Finish(); err != nil {
		t.Fatal(err)
	}
	n := nb.Build()
	lnk, err := test.Store(srcStore, n)
	if err != nil {
		t.Fatal(err)
	}

	return lnk.(cidlink.Link).Cid
}
