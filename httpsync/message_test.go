package httpsync_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/filecoin-project/go-legs/httpsync"
	ic "github.com/libp2p/go-libp2p-core/crypto"
)

func TestRoundTripHeadMsg(t *testing.T) {
	privKey, pubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal("Err generarting private key", err)
	}

	headMsg := httpsync.HeadMsg{
		CidStr: "bafybeicyhbhhklw3kdwgrxmf67mhkgjbsjauphsvrzywav63kn7bkpmqfa",
	}

	signed, err := headMsg.SignedEnvelope(privKey)
	if err != nil {
		t.Fatal("Err creating signed envelope", err)
	}

	headMsgRT, err := httpsync.OpenHeadMsgEnvelope(pubKey, bytes.NewReader(signed))
	if err != nil {
		t.Fatal("Err Opening msg envelope", err)
	}

	if headMsgRT.CidStr != headMsg.CidStr {
		t.Fatal("CidStr mismatch. Failed round trip")
	}
}

func TestRoundTripHeadMsgWithIncludedPubKey(t *testing.T) {
	privKey, pubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal("Err generarting private key", err)
	}

	headMsg := httpsync.HeadMsg{
		CidStr: "bafybeicyhbhhklw3kdwgrxmf67mhkgjbsjauphsvrzywav63kn7bkpmqfa",
	}

	signed, err := headMsg.SignedEnvelope(privKey)
	if err != nil {
		t.Fatal("Err creating signed envelope", err)
	}

	includedPubKey, headMsgRT, err := httpsync.OpenHeadMsgEnvelopeWithIncludedPubKey(bytes.NewReader(signed))
	if err != nil {
		t.Fatal("Err Opening msg envelope", err)
	}

	if headMsgRT.CidStr != headMsg.CidStr {
		t.Fatal("CidStr mismatch. Failed round trip")
	}

	if !includedPubKey.Equals(pubKey) {
		t.Fatal("CidStr mismatch. Failed round trip")
	}
}
