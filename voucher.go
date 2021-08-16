package legs

import (
	"context"
	"errors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

//go:generate go run -tags cbg ./tools

var _ dt.RequestValidator = &legsValidator{}

// A Voucher is used to communicate a new DAG head
type Voucher struct {
	Head *cid.Cid
}

// Type provides an identifier for the voucher to go-data-transfer
func (v *Voucher) Type() datatransfer.TypeIdentifier {
	return "LegsVoucher"
}

// A VoucherResult responds to a voucher
type VoucherResult struct {
	Code uint64
}

// Type provides an identifier for the voucher result to go-data-transfer
func (v *VoucherResult) Type() datatransfer.TypeIdentifier {
	return "LegsVoucherResult"
}

type legsValidator struct {
	ctx context.Context
	//ValidationsReceived chan receivedValidation
}

func (vl *legsValidator) ValidatePush(
	isRestart bool,
	sender peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) (datatransfer.VoucherResult, error) {

	// This is a pull-only DT voucher.
	return nil, errors.New("invalid")
}

func (vl *legsValidator) ValidatePull(
	isRestart bool,
	receiver peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) (datatransfer.VoucherResult, error) {

	v := voucher.(*Voucher)

	if v.Head == nil {
		return nil, errors.New("invalid")
	}

	return &VoucherResult{0}, nil
}
