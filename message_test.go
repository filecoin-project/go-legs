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

	msg1 := message{
		cid:   c,
		addrs: []multiaddr.Multiaddr{maddrA, maddrB},
	}

	data := encodeMessage(msg1)

	msg2, err := decodeMessage(data)
	if err != nil {
		t.Fatalf("Failed to decode message: %s", err)
	}

	if !msg2.cid.Equals(msg1.cid) {
		t.Fatal("Decoded cid is not equal to original")
	}

	if len(msg2.addrs) != len(msg1.addrs) {
		t.Fatalf("Wrong number of addresses, expected 2 got %d", len(msg2.addrs))
	}
	for i := range msg2.addrs {
		if !msg2.addrs[i].Equal(msg1.addrs[i]) {
			t.Fatalf("Decoded multiaddr %d %q is not equal to original %q", i, msg2.addrs[i], msg1.addrs[i])
		}
	}
}
