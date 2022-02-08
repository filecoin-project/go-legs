package dtsync

import (
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// config contains all options for configuring dtsync.publisher.
type config struct {
	minerID string
	topic   *pubsub.Topic
}

type Option func(*config) error

// apply applies the given options to this config.
func (c *config) apply(opts []Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

// MinerID sets a miner ID to include in the pubsub message.
func MinerID(minerID string) Option {
	return func(c *config) error {
		c.minerID = minerID
		return nil
	}
}

// Topic provides an existing pubsub topic.
func Topic(topic *pubsub.Topic) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}
