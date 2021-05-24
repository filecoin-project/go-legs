// +build cbg

package main

import (
	"os"
	"path"

	cborgen "github.com/whyrusleeping/cbor-gen"
	legs "github.com/willscott/go-legs"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	voucher_file := path.Clean(path.Join(wd, "..", "voucher_cbor_gen.go"))
	err = cborgen.WriteMapEncodersToFile(
		voucher_file,
		"legs",
		legs.Voucher{},
		legs.VoucherResult{},
	)
	if err != nil {
		panic(err)
	}
}
