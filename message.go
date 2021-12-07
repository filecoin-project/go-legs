package legs

import (
	"bytes"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-varint"
)

type message struct {
	cid   cid.Cid
	addrs []multiaddr.Multiaddr
}

func encodeMessage(m message) []byte {
	// Get size
	var size, maxVarintLen int
	for _, addr := range m.addrs {
		addrBytes := addr.Bytes()
		varintLen := varint.UvarintSize(uint64(len(addrBytes)))
		if varintLen > maxVarintLen {
			maxVarintLen = varintLen
		}
		size += len(addrBytes) + varintLen
	}

	vibuf := make([]byte, maxVarintLen)
	var b bytes.Buffer
	b.Grow(m.cid.ByteLen() + size)
	// CID already contains encoded length, so no need to add length to data.
	m.cid.WriteBytes(&b)

	for _, addr := range m.addrs {
		addrBytes := addr.Bytes()
		n := varint.PutUvarint(vibuf, uint64(len(addrBytes)))
		b.Write(vibuf[:n])
		b.Write(addrBytes)
	}

	return b.Bytes()
}

func decodeMessage(data []byte) (message, error) {
	n, c, err := cid.CidFromBytes(data)
	if err != nil {
		return message{}, err
	}
	data = data[n:]

	// Read multiaddrs if there is any more data in message data.  This allows
	// backward-compatability with publishers that do not supply address data.
	var addrs []multiaddr.Multiaddr
	for len(data) != 0 {
		val, n, err := varint.FromUvarint(data)
		if err != nil {
			log.Errorf("Failed to decode message: cannot read number of multiadds: %s", err)
			return message{}, err
		}
		data = data[n:]

		addr, err := multiaddr.NewMultiaddrBytes(data[:val])
		if err != nil {
			log.Errorf("Failed to decode message: cannot decode multiadds: %s", err)
			return message{}, err
		}
		data = data[val:]
		addrs = append(addrs, addr)
	}

	return message{
		cid:   c,
		addrs: addrs,
	}, nil
}
