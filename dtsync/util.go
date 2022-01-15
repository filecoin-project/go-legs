package dtsync

import (
	"context"

	dt "github.com/filecoin-project/go-data-transfer"
	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
)

type dtCloseFunc func() error

// configureDataTransferForLegs configures an existing data transfer instance to serve go-legs requests
// from given linksystem (publisher only)
func configureDataTransferForLegs(ctx context.Context, dtManager dt.Manager, lsys ipld.LinkSystem) error {
	v := &Voucher{}
	lvr := &VoucherResult{}
	val := &legsValidator{}
	lsc := legStorageConfigration{lsys}
	if err := dtManager.RegisterVoucherType(v, val); err != nil {
		log.Errorf("Failed to register legs voucher validator type: %s", err)
		return err
	}
	if err := dtManager.RegisterVoucherResultType(lvr); err != nil {
		log.Errorf("Failed to register legs voucher result type: %s", err)
		return err
	}
	if err := dtManager.RegisterTransportConfigurer(v, lsc.configureTransport); err != nil {
		log.Errorf("Failed to register datatransfer TransportConfigurer: %s", err)
		return err
	}
	return nil
}

type storeConfigurableTransport interface {
	UseStore(dt.ChannelID, ipld.LinkSystem) error
}

type legStorageConfigration struct {
	linkSystem ipld.LinkSystem
}

func (lsc legStorageConfigration) configureTransport(chid dt.ChannelID, voucher dt.Voucher, transport dt.Transport) {
	storeConfigurableTransport, ok := transport.(storeConfigurableTransport)
	if !ok {
		return
	}
	err := storeConfigurableTransport.UseStore(chid, lsc.linkSystem)
	if err != nil {
		log.Errorf("attempting to configure data store: %s", err)
	}
}

func registerVoucher(dtManager dt.Manager) error {
	v := &Voucher{}
	lvr := &VoucherResult{}
	val := &legsValidator{}
	err := dtManager.RegisterVoucherType(v, val)
	if err != nil {
		log.Errorf("Failed to register legs validator voucher type: %s", err)
		return err
	}
	if err = dtManager.RegisterVoucherResultType(lvr); err != nil {
		log.Errorf("Failed to register legs voucher result: %s", err)
		return err
	}

	return nil
}

func makeDataTransfer(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem) (dt.Manager, graphsync.GraphExchange, dtCloseFunc, error) {
	gsNet := gsnet.NewFromLibp2pHost(host)
	gs := gsimpl.New(context.Background(), gsNet, lsys)

	dtNet := dtnetwork.NewFromLibp2pHost(host)
	tp := gstransport.NewTransport(host.ID(), gs)

	dtManager, err := datatransfer.NewDataTransfer(ds, dtNet, tp)
	if err != nil {
		log.Errorf("Failed to instantiate datatransfer: %s", err)
		return nil, nil, nil, err
	}

	registerVoucher(dtManager)

	// Tell datatransfer to notify when ready.
	dtReady := make(chan error)
	dtManager.OnReady(func(e error) {
		dtReady <- e
	})

	// Start datatransfer.  The context passed in allows Start to be canceled
	// if fsm migration takes too long.  Timeout for dtManager.Start() is not
	// handled here, so pass context.Background().
	if err = dtManager.Start(context.Background()); err != nil {
		log.Errorf("Failed to start datatransfer: %s", err)
		return nil, nil, nil, err
	}

	// Wait for datatransfer to be ready.
	err = <-dtReady
	if err != nil {
		return nil, nil, nil, err
	}

	closeFunc := func() error {
		var err, errs error
		err = dtManager.Stop(context.Background())
		if err != nil {
			log.Errorf("Failed to stop datatransfer manager: %s", err)
			errs = multierror.Append(errs, err)
		}
		return errs
	}

	return dtManager, gs, closeFunc, nil
}
