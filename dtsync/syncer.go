package dtsync

import (
	"context"
	"fmt"
	"time"

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

	for {
		inProgressSyncK := inProgressSyncKey{nextCid, s.peerID}
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
		if err, ok := err.(rateLimitErr); ok {
			log.Infow("hit rate limit. Waiting and will retry later", "cid", nextCid, "source_peer", s.peerID)

			// Wait until the rate limit bucket is fully refilled since this is
			// a relatively heavy operation (essentially restarting the sync).
			// Note, cannot use s.rateLimiter.WaitN here because that waits,
			// but also consumes n tokens.
			waitTime := time.Second * time.Duration(s.rateLimiter.Burst()) / time.Duration(s.rateLimiter.Limit())
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			// Need to consume one token to make up for the previous Allow that
			// did not consume a token and triggered casued rate limit.
			s.rateLimiter.Allow()

			// Set the nextCid to be the cid that we stopped at becasuse of rate
			// limitting. This lets us pick up where we left off
			nextCid = err.stoppedAtCid
			continue
		}
		return err
	}
}
