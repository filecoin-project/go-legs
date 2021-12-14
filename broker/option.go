package broker

import (
	"fmt"
	"net/http"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// config contains all options for configuring LegBroker.
type config struct {
	addrTTL   time.Duration
	allowPeer AllowPeerFunc

	topic      *pubsub.Topic
	dtManager  dt.Manager
	httpClient *http.Client
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

// AllowPeer sets the function that determines whether to allow or reject
// messages from a peer.
func AllowPeer(allowPeer AllowPeerFunc) Option {
	return func(c *config) error {
		c.allowPeer = allowPeer
		return nil
	}
}

// AddrTTL sets the peerstore address time-to-live for addresses discovered
// from pubsub messages.
func AddrTTL(addrTTL time.Duration) Option {
	return func(c *config) error {
		c.addrTTL = addrTTL
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

// DtManager provides an existing datatransfer manager.
func DtManager(dt dt.Manager) Option {
	return func(c *config) error {
		c.dtManager = dt
		return nil
	}
}

func HttpClient(client *http.Client) Option {
	return func(c *config) error {
		c.httpClient = client
		return nil
	}
}
