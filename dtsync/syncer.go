package dtsync

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	rate "golang.org/x/time/rate"
)

// Syncer handles a single sync with a provider.
type Syncer struct {
	peerID      peer.ID
	sync        *Sync
	topicName   string
	rateLimiter *rate.Limiter
}

// GetHead queries a provider for the latest CID.
func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	return head.QueryRootCid(ctx, s.sync.host, s.topicName, s.peerID)
}

// Sync opens a datatransfer data channel and uses the selector to pull data
// from the provider.
func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	if s.rateLimiter != nil {
		// We were passed in a specific rate limiter, let's use it instead of the default one.
		s.sync.overrideRateLimiterForMu.Lock()
		// In case there was a previous rate limiter override.
		originalOverrideRateLimiter := s.sync.overrideRateLimiterFor[s.peerID]
		s.sync.overrideRateLimiterFor[s.peerID] = s.rateLimiter
		s.sync.overrideRateLimiterForMu.Unlock()
		defer func() {
			s.sync.overrideRateLimiterForMu.Lock()
			s.sync.overrideRateLimiterFor[s.peerID] = originalOverrideRateLimiter
			s.sync.overrideRateLimiterForMu.Unlock()
		}()
	}

	inProgressSyncK := inProgressSyncKey{nextCid, s.peerID}

	for {
		// For loop to retry if we get rate limited.
		syncDone := s.sync.notifyOnSyncDone(inProgressSyncK)

		log.Debugw("Starting data channel for message source", "cid", nextCid, "source_peer", s.peerID)

		v := Voucher{&nextCid}
		_, err := s.sync.dtManager.OpenPullDataChannel(ctx, s.peerID, &v, nextCid, sel)
		if err != nil {
			s.sync.signalSyncDone(inProgressSyncK, nil)
			return fmt.Errorf("cannot open data channel: %w", err)
		}

		// Wait for transfer finished signal.
		err = <-syncDone
		if _, ok := err.(rateLimitErr); ok {
			log.Infow("hit rate limit. Waiting and will retry later", "cid", nextCid, "source_peer", s.peerID)

			// Wait until we've fully refilled our rate limit bucket since this is a relatively heavy operation (essentially restarting the sync).
			s.rateLimiter.WaitN(ctx, s.rateLimiter.Burst())
			continue
		}
		return err
	}
}
