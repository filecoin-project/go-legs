package legs

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
)

func TestEncodeDecodeMessage(t *testing.T) {
	cidStr := "QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB"
	c, err := cid.Decode(cidStr)
	if err != nil {
		panic(err)
	}

	maddrA, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	if err != nil {
		panic(err)
	}
	maddrB, err := multiaddr.NewMultiaddr("/ip4/192.168.0.1/tcp/2701")
	if err != nil {
		panic(err)
	}
	maddrs := []multiaddr.Multiaddr{maddrA, maddrB}

	data := encodeMessage(c, maddrs)

	c2, maddrs2, err := decodeMessage(data)
	if err != nil {
		t.Fatalf("Failed to decode message: %s", err)
	}

	if !c2.Equals(c) {
		t.Fatal("Decoded cid is not equal to original")
	}

	if len(maddrs2) != len(maddrs) {
		t.Fatalf("Wrong number of addresses, expected 2 got %d", len(maddrs2))
	}
	for i := range maddrs2 {
		if !maddrs2[i].Equal(maddrs[i]) {
			t.Fatalf("Decoded multiaddr %d %q is not equal to original %q", i, maddrs2[i], maddrs[i])
		}
	}
}
