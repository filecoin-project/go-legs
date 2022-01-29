package dtsync

import (
	"bytes"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-varint"
)

type Message struct {
	Cid   cid.Cid
	Addrs []multiaddr.Multiaddr
}

func EncodeMessage(m Message) []byte {
	// Get size
	var size, maxVarintLen int
	for _, addr := range m.Addrs {
		addrBytes := addr.Bytes()
		varintLen := varint.UvarintSize(uint64(len(addrBytes)))
		if varintLen > maxVarintLen {
			maxVarintLen = varintLen
		}
		size += len(addrBytes) + varintLen
	}

	vibuf := make([]byte, maxVarintLen)
	var b bytes.Buffer
	b.Grow(m.Cid.ByteLen() + size)
	// CID already contains encoded length, so no need to add length to data.
	_, err := m.Cid.WriteBytes(&b)
	if err != nil {
		panic(err)
	}

	for _, addr := range m.Addrs {
		addrBytes := addr.Bytes()
		n := varint.PutUvarint(vibuf, uint64(len(addrBytes)))
		b.Write(vibuf[:n])
		b.Write(addrBytes)
	}

	return b.Bytes()
}

func DecodeMessage(data []byte) (Message, error) {
	n, c, err := cid.CidFromBytes(data)
	if err != nil {
		return Message{}, err
	}
	data = data[n:]

	// Read multiaddrs if there is any more data in message data.  This allows
	// backward-compatability with publishers that do not supply address data.
	var addrs []multiaddr.Multiaddr
	for len(data) != 0 {
		val, n, err := varint.FromUvarint(data)
		if err != nil {
			return Message{}, fmt.Errorf("cannot read number of multiadds: %s", err)
		}
		data = data[n:]

		addr, err := multiaddr.NewMultiaddrBytes(data[:val])
		if err != nil {
			return Message{}, fmt.Errorf("cannot decode multiadds: %s", err)
		}
		data = data[val:]
		addrs = append(addrs, addr)
	}

	return Message{
		Cid:   c,
		Addrs: addrs,
	}, nil
}
