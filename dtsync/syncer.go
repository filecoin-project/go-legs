package dtsync

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Syncer handles a single sync with a provider.
type Syncer struct {
	peerID    peer.ID
	sync      *Sync
	topicName string
}

// GetHead queries a provider for the latest CID.
func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	return head.QueryRootCid(ctx, s.sync.host, s.topicName, s.peerID)
}

// Sync opens a datatransfer data channel and uses the selector to pull data
// from the provider.
func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	syncDone := s.sync.notifyOnSyncDone(inProgressSyncKey{nextCid, s.peerID})

	log.Debugw("Starting data channel for message source", "cid", nextCid, "source_peer", s.peerID)

	v := Voucher{&nextCid}
	_, err := s.sync.dtManager.OpenPullDataChannel(ctx, s.peerID, &v, nextCid, sel)
	if err != nil {
		s.sync.signalSyncDone(inProgressSyncKey{nextCid, s.peerID}, nil)
		return fmt.Errorf("cannot open data channel: %s", err)
	}

	// Wait for transfer finished signal.
	return <-syncDone
}
