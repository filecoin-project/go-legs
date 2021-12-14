package dtsync

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

type syncer struct {
	peerID    peer.ID
	sync      *Sync
	topicName string
}

func (s *syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	// Query the peer for the latest CID
	return head.QueryRootCid(ctx, s.sync.host, s.topicName, s.peerID)
}

func (s *syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	syncDone := s.sync.notifyOnSyncDone(nextCid)

	v := Voucher{&nextCid}
	_, err := s.sync.dtManager.OpenPullDataChannel(ctx, s.peerID, &v, nextCid, sel)
	if err != nil {
		s.sync.signalSyncDone(nextCid, nil)
		return fmt.Errorf("cannot open data channel: %s", err)
	}

	// Wait for transfer finished signal.
	return <-syncDone
}
