package main

import (
	"github.com/filecoin-project/go-legs/announce/gossiptopic"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	if err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "gossiptopic",
		gossiptopic.Message{},
	); err != nil {
		panic(err)
	}
}
