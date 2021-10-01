package legs

import (
	"context"
	"sync/atomic"

	dt "github.com/filecoin-project/go-data-transfer"

	"github.com/ipld/go-ipld-prime"
)

// Reference wraps an atomic ref counter
type Reference interface {
	AddReference()
	RemoveReference()
	RefCount() int32
}

// NewReference instantiates an atomic ref counter
func NewReference() Reference {
	return &reference{}
}

type reference struct {
	refc int32
}

func (r *reference) AddReference() {
	atomic.AddInt32(&r.refc, 1)
}

func (r *reference) RemoveReference() {
	atomic.AddInt32(&r.refc, -1)
}

func (r *reference) RefCount() int32 {
	return atomic.LoadInt32(&r.refc)
}

// ConfigureDataTransferForLegs configures an existing data transfer instance to serve go-legs requests
// from given linksystem (publisher only)
func ConfigureDataTransferForLegs(ctx context.Context, dt dt.Manager, lsys ipld.LinkSystem) error {
	v := &Voucher{}
	lvr := &VoucherResult{}
	val := &legsValidator{}
	lsc := legStorageConfigration{lsys}
	if err := dt.RegisterVoucherType(v, val); err != nil {
		return err
	}
	if err := dt.RegisterVoucherResultType(lvr); err != nil {
		return err
	}
	if err := dt.RegisterTransportConfigurer(v, lsc.configureTransport); err != nil {
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
