module github.com/willscott/go-legs

go 1.16

require (
	github.com/filecoin-project/go-data-transfer v1.6.1-0.20210608092034-e4f40bc3a685
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.6
	github.com/ipfs/go-graphsync v0.8.1-rc1
	github.com/ipfs/go-log/v2 v2.1.3
	github.com/ipld/go-ipld-prime v0.12.0
	github.com/libp2p/go-libp2p v0.14.4
	github.com/libp2p/go-libp2p-core v0.8.6
	github.com/libp2p/go-libp2p-pubsub v0.5.4
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1
	github.com/multiformats/go-multicodec v0.3.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20210713220151-be142a5ae1a8
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

// TODO: Replace when https://github.com/ipld/go-ipld-prime/pull/240 is merged
replace github.com/ipld/go-ipld-prime => github.com/adlrocha/go-ipld-prime v0.11.1-0.20210903061231-033de3964876
