package dtsync

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// Syncer handles a single sync with a provider.
type Syncer struct {
	peerID      peer.ID
	rateLimiter *rate.Limiter
	sync        *Sync
	topicName   string
}

// GetHead queries a provider for the latest CID.
func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	return head.QueryRootCid(ctx, s.sync.host, s.topicName, s.peerID)
}

// Sync opens a datatransfer data channel and uses the selector to pull data
// from the provider.
func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	if s.rateLimiter != nil {
		// Set the rate limiter to use for this sync of the peer. This limiter
		// is retrieved by getRateLimiter, called from wrapped block hook.
		s.sync.setRateLimiter(s.peerID, s.rateLimiter)
		// Remove rate limiter set above.
		defer s.sync.clearRateLimiter(s.peerID)
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
			// Wait until the rate limit bucket is fully refilled since this is
			// a relatively heavy operation (essentially restarting the sync).
			// Note, cannot use s.rateLimiter.WaitN here because that waits,
			// but also consumes n tokens.
			waitMsec := 1000.0 * float64(s.rateLimiter.Burst()) / float64(s.rateLimiter.Limit())
			var waitTime time.Duration
			waitTime = time.Duration(waitMsec) * time.Millisecond
			if waitTime == 0 {
				waitTime = time.Duration(1000*waitMsec) * time.Microsecond
			}
			log.Infow("Hit rate limit. Waiting and will retry later", "cid", nextCid, "source_peer", s.peerID, "delay", waitTime.String())
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			// Need to consume one token, since the stopped to make up for the
			// previous Allow that did not consume a token and triggered rate
			// limiting, even though the block was still downloaded. At next
			// restart the stopped at block will be local and will not count
			// toward rate limiting.
			s.rateLimiter.Allow()

			// Set the nextCid to be the cid that we stopped at becasuse of rate
			// limitting. This lets us pick up where we left off
			nextCid = err.stoppedAtCid
			continue
		}
		return err
	}
}
