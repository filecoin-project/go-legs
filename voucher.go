package legs

import (
	"context"
	"errors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-peer"
)

//go:generate cbor-gen-for --map-encoding LegsVoucher LegsVoucherResult

type LegsVoucher struct {
	Head *cid.Cid
}

func (v *LegsVoucher) Type() datatransfer.TypeIdentifier {
	return "LegsVoucher"
}

type LegsVoucherResult struct {
	Code uint64
}

func (v *LegsVoucherResult) Type() datatransfer.TypeIdentifier {
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

	v := voucher.(*LegsVoucher)

	if v.Head == nil {
		return nil, errors.New("invalid")
	}

	return &LegsVoucherResult{0}, nil
}
