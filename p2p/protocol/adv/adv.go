package adv

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	gostream "github.com/libp2p/go-libp2p-gostream"
)

var log = logging.Logger("go-legs-adv")

func deriveProtocolID(topic string) protocol.ID {
	return protocol.ID("/legs/adv/" + topic + "/0.0.1")
}

func (p *AdvPublisher) Serve(ctx context.Context, host host.Host, topic string) error {
	l, err := gostream.Listen(host, deriveProtocolID(topic))
	if err != nil {
		return err
	}

	p.server = &http.Server{Handler: http.Handler(p)}
	errChan := make(chan error)
	go func() {
		errChan <- p.server.Serve(l)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return p.server.Close()
	}
}

func newHttpClient(ctx context.Context, host host.Host, topic string, peer peer.ID) (http.Client, error) {
	transport := http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return gostream.Dial(ctx, host, peer, deriveProtocolID(topic))
		},
	}

	return http.Client{
		Transport: &transport,
	}, nil
}

type AdvClient struct {
	client *http.Client
}

func NewAdvClient(ctx context.Context, host host.Host, topic string, peer peer.ID) (*AdvClient, error) {
	client, err := newHttpClient(ctx, host, topic, peer)
	return &AdvClient{
		client: &client,
	}, err
}

func (c *AdvClient) QueryRootCid(ctx context.Context) (cid.Cid, error) {
	// The httpclient expects there to be a host here. `.invalid` is a reserved
	// TLD for this purpose. See
	// https://datatracker.ietf.org/doc/html/rfc2606#section-2
	resp, err := c.client.Get("http://unused.invalid/head")
	if err != nil {
		return cid.Undef, err
	}
	defer resp.Body.Close()

	var cidStr string
	err = json.NewDecoder(resp.Body).Decode(&cidStr)
	if err != nil {
		return cid.Undef, err
	}

	return cid.Decode(cidStr)
}

type AdvPublisher struct {
	rl     sync.RWMutex
	root   cid.Cid
	server *http.Server
}

func (p *AdvPublisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ask := path.Base(r.URL.Path)
	if ask == "head" {
		p.rl.RLock()
		defer p.rl.RUnlock()
		out, err := json.Marshal(p.root.String())
		if err != nil {
			w.WriteHeader(500)
			log.Infow("failed to serve root", "err", err)
		} else {
			_, _ = w.Write(out)
		}
	} else {
		w.WriteHeader(404)
	}
}

func (p *AdvPublisher) UpdateRoot(_ context.Context, c cid.Cid) error {
	p.rl.Lock()
	defer p.rl.Unlock()
	p.root = c
	return nil
}

func (p *AdvPublisher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.server.Shutdown(ctx)
}
