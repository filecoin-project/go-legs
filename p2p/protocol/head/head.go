package head

import (
	"context"
	"io"
	"net"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	gostream "github.com/libp2p/go-libp2p-gostream"
)

const closeTimeout = 30 * time.Second

func deriveProtocolID(topic string) protocol.ID {
	return protocol.ID("/legs/head/" + topic + "/0.0.1")
}

func (p *Publisher) Serve(host host.Host, topic string) error {
	l, err := gostream.Listen(host, deriveProtocolID(topic))
	if err != nil {
		return err
	}

	p.server = &http.Server{Handler: http.Handler(p)}
	return p.server.Serve(l)
}

func QueryRootCid(ctx context.Context, host host.Host, topic string, peer peer.ID) (cid.Cid, error) {
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return gostream.Dial(ctx, host, peer, deriveProtocolID(topic))
			},
		},
	}

	// The httpclient expects there to be a host here. `.invalid` is a reserved
	// TLD for this purpose. See
	// https://datatracker.ietf.org/doc/html/rfc2606#section-2
	resp, err := client.Get("http://unused.invalid/head")
	if err != nil {
		return cid.Undef, err
	}
	defer resp.Body.Close()

	cidStr, err := io.ReadAll(resp.Body)
	if err != nil {
		return cid.Undef, err
	}
	if len(cidStr) == 0 {
		return cid.Undef, nil
	}

	return cid.Decode(string(cidStr))
}

type Publisher struct {
	rl     sync.RWMutex
	root   cid.Cid
	server *http.Server
}

func (p *Publisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if path.Base(r.URL.Path) != "head" {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	p.rl.RLock()
	defer p.rl.RUnlock()
	var out []byte
	if p.root != cid.Undef {
		out = []byte(p.root.String())
	}

	_, _ = w.Write(out)
}

func (p *Publisher) UpdateRoot(_ context.Context, c cid.Cid) error {
	p.rl.Lock()
	defer p.rl.Unlock()
	p.root = c
	return nil
}

func (p *Publisher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	return p.server.Shutdown(ctx)
}
