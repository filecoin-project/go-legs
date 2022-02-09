package main

import (
	dtsync "github.com/filecoin-project/go-legs/dtsync"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "dtsync",
		dtsync.Message{},
	); err != nil {
		panic(err)
	}
}
