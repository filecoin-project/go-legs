package dtsync

import (
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
)

var ErrBadEncoding = errors.New("invalid message encoding")

type Message struct {
	Cid       cid.Cid
	Addrs     [][]byte
	ExtraData []byte
	OrigPeer  string
}

func (m *Message) SetAddrs(addrs []multiaddr.Multiaddr) {
	m.Addrs = make([][]byte, len(addrs))
	for i, a := range addrs {
		m.Addrs[i] = a.Bytes()
	}
}

func (m *Message) GetAddrs() ([]multiaddr.Multiaddr, error) {
	addrs := make([]multiaddr.Multiaddr, len(m.Addrs))
	for i := range m.Addrs {
		var err error
		addrs[i], err = multiaddr.NewMultiaddrBytes(m.Addrs[i])
		if err != nil {
			return nil, err
		}
	}
	return addrs, nil
}
