package legs

import (
	"context"
	"io/ioutil"
	"os"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
)

// LegTransport wraps all the assets to set-up the data transfer.
type LegTransport struct {
	tmpDir string
	t      dt.Manager
	Gs     graphsync.GraphExchange

	lk   sync.Mutex
	refc int
}

// MakeLegTransport creates a new datatransfer transport to use with go-legs
func MakeLegTransport(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, tmpPath string) (*LegTransport, error) {
	gsnet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(context.Background(), gsnet, lsys)
	tp := gstransport.NewTransport(host.ID(), gs)
	dtNet := dtnetwork.NewFromLibp2pHost(host)

	tmpDir, err := ioutil.TempDir("", tmpPath)
	if err != nil {
		return nil, err
	}

	dt, err := datatransfer.NewDataTransfer(ds, tmpDir, dtNet, tp)
	if err != nil {
		return nil, err
	}

	v := &Voucher{}
	lvr := &VoucherResult{}
	val := &legsValidator{}
	if err := dt.RegisterVoucherType(v, val); err != nil {
		return nil, err
	}
	if err := dt.RegisterVoucherResultType(lvr); err != nil {
		return nil, err
	}
	if err := dt.Start(ctx); err != nil {
		return nil, err
	}
	return &LegTransport{tmpDir: tmpDir, t: dt, Gs: gs}, nil
}

func (lt *LegTransport) addRefc() {
	lt.lk.Lock()
	defer lt.lk.Unlock()
	lt.refc++
}

func (lt *LegTransport) Close(ctx context.Context) error {
	lt.lk.Lock()
	defer lt.lk.Unlock()
	lt.refc--

	if lt.refc == 0 {
		err := lt.t.Stop(ctx)
		err2 := os.RemoveAll(lt.tmpDir)
		if err != nil {
			return err
		}
		return err2
	}
	return nil
}
