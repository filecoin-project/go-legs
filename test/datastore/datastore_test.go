package datastore_test

import (
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
	dstore "github.com/willscott/go-legs/test/datastore"
)

func TestMapDatastore(t *testing.T) {
	ds := dstore.NewMapDatastore()
	dstest.SubtestAll(t, ds)
}
