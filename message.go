package legs

import (
	"bytes"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-varint"
)

func encodeMessage(c cid.Cid, addrs []multiaddr.Multiaddr) []byte {
	// Get size
	var size, maxVarintLen int
	for i := range addrs {
		addrBytes := addrs[i].Bytes()
		varintLen := varint.UvarintSize(uint64(len(addrBytes)))
		if varintLen > maxVarintLen {
			maxVarintLen = varintLen
		}
		size += len(addrBytes) + varintLen
	}

	vibuf := make([]byte, maxVarintLen)
	var b bytes.Buffer
	b.Grow(c.ByteLen() + size)
	// CID already contains encoded length, so no need to add length to data.
	c.WriteBytes(&b)

	for i := range addrs {
		addrBytes := addrs[i].Bytes()
		n := varint.PutUvarint(vibuf, uint64(len(addrBytes)))
		b.Write(vibuf[:n])
		b.Write(addrBytes)
	}

	return b.Bytes()
}

func decodeMessage(data []byte) (cid.Cid, []multiaddr.Multiaddr, error) {
	n, c, err := cid.CidFromBytes(data)
	if err != nil {
		return cid.Undef, nil, err
	}
	data = data[n:]

	var addrs []multiaddr.Multiaddr
	for len(data) != 0 {
		val, n, err := varint.FromUvarint(data)
		if err != nil {
			return cid.Undef, nil, err
		}
		data = data[n:]

		addr, err := multiaddr.NewMultiaddrBytes(data[:val])
		if err != nil {
			return cid.Undef, nil, err
		}
		data = data[val:]
		addrs = append(addrs, addr)
	}

	return c, addrs, nil
}
