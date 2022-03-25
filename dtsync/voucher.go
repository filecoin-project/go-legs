package dtsync

import (
	"errors"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	cborgen "github.com/whyrusleeping/cbor-gen"
)

//go:generate go run -tags cbg ../tools

var (
	_ datatransfer.RequestValidator = (*legsValidator)(nil)

	_ datatransfer.Registerable = (*Voucher)(nil)
	_ cborgen.CBORMarshaler     = (*Voucher)(nil)
	_ cborgen.CBORUnmarshaler   = (*Voucher)(nil)

	_ datatransfer.Registerable = (*VoucherResult)(nil)
	_ cborgen.CBORMarshaler     = (*VoucherResult)(nil)
	_ cborgen.CBORUnmarshaler   = (*VoucherResult)(nil)
)

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
	//ctx context.Context
	//ValidationsReceived chan receivedValidation
}

func (vl *legsValidator) ValidatePush(
	_ bool,
	_ datatransfer.ChannelID,
	_ peer.ID,
	_ datatransfer.Voucher,
	_ cid.Cid,
	_ ipld.Node) (datatransfer.VoucherResult, error) {

	// This is a pull-only DT voucher.
	return nil, errors.New("invalid")
}

func (vl *legsValidator) ValidatePull(
	_ bool,
	_ datatransfer.ChannelID,
	_ peer.ID,
	voucher datatransfer.Voucher,
	_ cid.Cid,
	_ ipld.Node) (datatransfer.VoucherResult, error) {

	v := voucher.(*Voucher)
	if v.Head == nil {
		return nil, errors.New("invalid")
	}

	return &VoucherResult{}, nil
}
