package legs

import (
	"context"
	"errors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	peer "github.com/libp2p/go-libp2p-peer"
)

type legsVoucher struct {
	head cid.Cid
}

func (v *legsVoucher) ToBytes() ([]byte, error) {
	return v.head.Bytes(), nil
}

func (v *legsVoucher) FromBytes(data []byte) (err error) {
	v.head, err = cid.Cast(data)
	return
}

func (v *legsVoucher) Type() datatransfer.TypeIdentifier {
	return "LegsVoucher"
}

type legsVoucherResult struct {
	code uint8
}

func (v *legsVoucherResult) ToBytes() ([]byte, error) {
	return []byte{v.code}, nil
}

func (v *legsVoucherResult) FromBytes(data []byte) error {
	v.code = data[0]
	return nil
}

func (v *legsVoucherResult) Type() datatransfer.TypeIdentifier {
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

	v := voucher.(*legsVoucher)

	if v.head == cid.Undef {
		return nil, errors.New("invalid")
	}

	return &legsVoucherResult{0}, nil
}
