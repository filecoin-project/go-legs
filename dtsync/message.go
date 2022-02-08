package dtsync

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-varint"
)

var ErrBadEncoding = errors.New("invalid message encoding")

type Message struct {
	Cid       cid.Cid
	Addrs     []multiaddr.Multiaddr
	ExtraData []byte `json:",omitempty"`
}

// EncodeMessage produces a binary encoding of the message. The binary format
// is as follows:
//
//   +----------------------------------------------------+
//   | CID   | nAddrs | addrLen | addr  | ... | ExtraData |
//   | bytes | varint | varint  | bytes | ... | bytes     |
//   +----------------------------------------------------+
//                    |                 |
//                    { repeat nAddrs X }
//
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

	n := varint.PutUvarint(vibuf, uint64(len(m.Addrs)))

	var b bytes.Buffer
	b.Grow(m.Cid.ByteLen() + n + size + len(m.ExtraData))
	// CID already contains encoded length, so no need to add length to data.
	_, err := m.Cid.WriteBytes(&b)
	if err != nil {
		panic(err)
	}

	// Write number of addrs
	b.Write(vibuf[:n])

	for _, addr := range m.Addrs {
		addrBytes := addr.Bytes()
		n = varint.PutUvarint(vibuf, uint64(len(addrBytes)))
		b.Write(vibuf[:n])
		b.Write(addrBytes)
	}

	if len(m.ExtraData) != 0 {
		b.Write(m.ExtraData)
	}

	return b.Bytes()
}

// DecodeMessage decodes a binary encoded message that was encoded by
// EncodeMessage.
func DecodeMessage(data []byte) (Message, error) {
	n, c, err := cid.CidFromBytes(data)
	if err != nil {
		return Message{}, err
	}
	if n > len(data) {
		return Message{}, ErrBadEncoding
	}
	data = data[n:]

	var addrs []multiaddr.Multiaddr
	var extraData []byte

	if len(data) != 0 {
		addrCount, n, err := varint.FromUvarint(data)
		if err != nil {
			return Message{}, fmt.Errorf("cannot read number of multiaddrs: %w", err)
		}
		if n > len(data) {
			return Message{}, ErrBadEncoding
		}
		data = data[n:]

		if addrCount != 0 {
			// Read multiaddrs if there is any more data in message data.  This allows
			// backward-compatability with publishers that do not supply address data.
			addrs = make([]multiaddr.Multiaddr, addrCount)
			for i := 0; i < int(addrCount); i++ {
				val, n, err := varint.FromUvarint(data)
				if err != nil {
					return Message{}, fmt.Errorf("cannot read multiaddrs length: %w", err)
				}
				if n > len(data) {
					return Message{}, ErrBadEncoding
				}
				data = data[n:]

				if len(data) < int(val) {
					return Message{}, ErrBadEncoding
				}
				addrs[i], err = multiaddr.NewMultiaddrBytes(data[:val])
				if err != nil {
					return Message{}, fmt.Errorf("cannot decode multiaddrs: %w", err)
				}
				data = data[val:]
			}
		}
		if len(data) != 0 {
			extraData = make([]byte, len(data))
			copy(extraData, data)
		}
	}

	return Message{
		Cid:       c,
		Addrs:     addrs,
		ExtraData: extraData,
	}, nil
}
