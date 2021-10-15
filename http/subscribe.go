package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync"

	"github.com/filecoin-project/go-legs"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multicodec"
)

// NewHTTPSubscriber creates a legs subcriber that provides subscriptions
// from publishers identified by
func NewHTTPSubscriber(ctx context.Context, host *http.Client, publisher multiaddr.Multiaddr, lsys *ipld.LinkSystem, topic string) (legs.LegSubscriber, error) {
	url, err := toURL(publisher)
	if err != nil {
		return nil, err
	}
	hs := httpSubscriber{
		host,
		cid.Undef,
		url,

		lsys,
		sync.Mutex{},
		make([]chan cid.Cid, 1),
	}
	return &hs, nil
}

// toURL takes a multiaddr of the form:
// /dnsaddr/thing.com/http/path/to/root
// /ip/192.168.0.1/http
// TODO: doesn't support including a signing key definition yet
func toURL(ma multiaddr.Multiaddr) (string, error) {
	// host should be either the dns name or the IP
	_, host, err := manet.DialArgs(ma)
	if err != nil {
		return "", err
	}
	_, http := multiaddr.SplitFunc(ma, func(c multiaddr.Component) bool {
		return c.Protocol().Code == multiaddr.P_HTTP
	})
	if len(http.Bytes()) == 0 {
		return "", fmt.Errorf("publisher must be HTTP protocol for this subscriber, was: %s", ma)
	}
	_, path := multiaddr.SplitFirst(http)
	return fmt.Sprintf("https://%s/%s", host, path), nil
}

type httpSubscriber struct {
	*http.Client
	head cid.Cid
	root string

	lsys   *ipld.LinkSystem
	submtx sync.Mutex
	subs   []chan cid.Cid
}

func (h *httpSubscriber) OnChange() (chan cid.Cid, context.CancelFunc) {
	ch := make(chan cid.Cid)
	h.submtx.Lock()
	defer h.submtx.Unlock()
	h.subs = append(h.subs, ch)
	cncl := func() {
		h.submtx.Lock()
		defer h.submtx.Unlock()
		for i, ca := range h.subs {
			if ca == ch {
				h.subs[i] = h.subs[len(h.subs)-1]
				h.subs[len(h.subs)-1] = nil
				h.subs = h.subs[:len(h.subs)-1]
				close(ch)
				break
			}
		}
	}
	return ch, cncl
}

// Not supported, since gossip-sub is not supported by this handler.
// `Sync` must be called explicitly to trigger a fetch instead.
func (h *httpSubscriber) SetPolicyHandler(p legs.PolicyHandler) error {
	return nil
}

func (h *httpSubscriber) SetLatestSync(c cid.Cid) error {
	h.head = c
	return nil
}

func (h *httpSubscriber) Sync(ctx context.Context, p peer.ID, c cid.Cid) (chan cid.Cid, context.CancelFunc, error) {
	return nil, nil, nil
}

func (h *httpSubscriber) Close() error {
	return nil
}

func (h *httpSubscriber) fetch(url string, cb func(io.Reader) error) error {
	resp, err := h.Client.Get(path.Join(h.root, url))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non success http code at %s/%s: %d", h.root, url, resp.StatusCode)
	}

	defer resp.Body.Close()
	return cb(resp.Body)
}

func (h *httpSubscriber) fetchHead() (cid.Cid, error) {
	var cidStr string
	if err := h.fetch("head", func(msg io.Reader) error {
		return json.NewDecoder(msg).Decode(&cidStr)
	}); err != nil {
		return cid.Undef, err
	}

	return cid.Decode(cidStr)
}

// fetch an item into the datastore at c if not locally avilable.
func (h *httpSubscriber) fetchDag(c cid.Cid) error {
	n, err := h.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	// node is already present.
	if n != nil && err == nil {
		return nil
	}

	return h.fetch(c.String(), func(data io.Reader) error {
		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, data); err != nil {
			return err
		}
		b := basicnode.NewBytes(buf.Bytes())
		// we're storing just as 'raw', but will later interpret with c's codec
		pref := c.Prefix()
		pref.Codec = uint64(multicodec.Raw)
		ec, err := h.lsys.Store(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: pref}, b)
		if err != nil {
			return err
		}

		if ec.(cidlink.Link).Cid.Hash().B58String() != c.Hash().B58String() {
			return fmt.Errorf("content did not match expected hash: %s vs %s", ec, c)
		}
		return nil
	})
}
