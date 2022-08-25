package gpubsub

import (
	"context"
	"errors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"golang.org/x/crypto/blake2b"
)

var log = logging.Logger("go-legs-gpubsub")

// directConnectTicks makes pubsub check it's connected to direct peers every N seconds.
const directConnectTicks uint64 = 30

func MakePubsub(ctx context.Context, h host.Host, topic string) (*pubsub.Topic, error) {
	p, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerExchange(true),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			h, _ := blake2b.New256(nil)
			h.Write(pmsg.Data)
			return string(h.Sum(nil))
		}),
		pubsub.WithFloodPublish(true),
		pubsub.WithDirectConnectTicks(directConnectTicks),
		pubsub.WithRawTracer(&loggingTracer{log}),
	)
	if err != nil {
		msg := "failed to create pubsub"
		log.Errorw(msg, "topic", topic, "peer", h.ID(), "err", err)
		return nil, errors.New(msg)
	}

	log.Infof("Instantiated pubsub with peer ID %s", h.ID())
	t, err := p.Join(topic)
	if err != nil {
		msg := "failed to join topic"
		log.Errorw(msg, "topic", topic, "err", err)
		return nil, errors.New(msg)
	}
	log.Infof("Joined pubsub topic %s", topic)
	return t, nil
}
