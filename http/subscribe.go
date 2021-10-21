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
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multicodec"
)

var defaultPollTime = time.Hour

var log = logging.Logger("go-legs")

// NewHTTPSubscriber creates a legs subcriber that provides subscriptions
// from publishers identified by
func NewHTTPSubscriber(ctx context.Context, host *http.Client, publisher multiaddr.Multiaddr, lsys *ipld.LinkSystem, topic string, selector ipld.Node) (legs.LegSubscriber, error) {
	url, err := toURL(publisher)
	if err != nil {
		return nil, err
	}
	hs := httpSubscriber{
		host,
		cid.Undef,
		url,

		lsys,
		selector,
		sync.Mutex{},
		make(chan req, 1),
		make([]chan cid.Cid, 1),
	}
	go hs.background()
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

	lsys            *ipld.LinkSystem
	defaultSelector ipld.Node
	submtx          sync.Mutex
	// reqs is inbound requests for syncs from `Sync` calls
	reqs chan req
	subs []chan cid.Cid
}

type req struct {
	cid.Cid
	Selector ipld.Node
	ctx      context.Context
	resp     chan cid.Cid
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
	h.submtx.Lock()
	defer h.submtx.Unlock()
	h.head = c
	return nil
}

func (h *httpSubscriber) Sync(ctx context.Context, p peer.ID, c cid.Cid, selector ipld.Node) (chan cid.Cid, context.CancelFunc, error) {
	respChan := make(chan cid.Cid, 1)
	cctx, cncl := context.WithCancel(ctx)
	h.submtx.Lock()
	defer h.submtx.Unlock()
	// todo: error if reqs is full
	h.reqs <- req{
		Cid:      c,
		Selector: selector,
		ctx:      cctx,
		resp:     respChan,
	}
	return respChan, cncl, nil
}

func (h *httpSubscriber) Close() error {
	// cancel out subscribers.
	h.Client.CloseIdleConnections()
	h.submtx.Lock()
	defer h.submtx.Unlock()
	for _, ca := range h.subs {
		close(ca)
	}
	h.subs = make([]chan cid.Cid, 0)
	return nil
}

// background event loop for scheduling
// a. time-scheduled fetches to the provider
// b. interrupted fetches in response to synchronous 'Sync' calls.
func (h *httpSubscriber) background() {
	var nextCid cid.Cid
	var workResp chan cid.Cid
	var ctx context.Context
	var sel ipld.Node
	var xsel selector.Selector
	var err error
	for {
		select {
		case r := <-h.reqs:
			nextCid = r.Cid
			workResp = r.resp
			sel = r.Selector
			ctx = r.ctx
		case <-time.After(defaultPollTime):
			nextCid = cid.Undef
			workResp = nil
			ctx = context.Background()
			sel = nil
		}
		if nextCid == cid.Undef {
			nextCid, err = h.fetchHead(ctx)
		}
		if sel == nil {
			sel = h.defaultSelector
		}
		if err != nil {
			log.Warnf("failed to fetch new head: %s", err)
			err = nil
			goto next
		}
		sel = legs.ExploreRecursiveWithStopNode(selector.RecursionLimitNone(),
			sel,
			cidlink.Link{Cid: h.head})
		if err := h.fetchBlock(ctx, nextCid); err != nil {
			//log
			goto next
		}
		xsel, err = selector.CompileSelector(sel)
		if err != nil {
			//log
			goto next
		}

		err = h.walkFetch(ctx, nextCid, xsel)
		if err != nil {
			//log
			goto next
		}

	next:
		if workResp != nil {
			workResp <- nextCid
			close(workResp)
			workResp = nil
		}
	}
}

func (h *httpSubscriber) walkFetch(ctx context.Context, root cid.Cid, sel selector.Selector) error {
	getMissingLs := cidlink.DefaultLinkSystem()
	getMissingLs.TrustedStorage = true
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		r, err := h.lsys.StorageReadOpener(lc, l)
		if err == nil {
			return r, nil
		}
		// get.
		writer, committer, err := h.lsys.StorageWriteOpener(lc)
		if err != nil {
			return nil, err
		}
		c := l.(cidlink.Link).Cid
		if err := h.fetch(ctx, c.String(), func(r io.Reader) error {
			if _, err := io.Copy(writer, r); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}
		if err := committer(l); err != nil {
			return nil, err
		}
		return h.lsys.StorageReadOpener(lc, l)
	}

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     getMissingLs,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
		},
		Path: datamodel.NewPath([]datamodel.PathSegment{}),
		LastBlock: struct {
			Path datamodel.Path
			Link datamodel.Link
		}{
			Path: ipld.Path{},
			Link: nil,
		},
	}
	// get the direct node.
	rootNode, err := getMissingLs.Load(ipld.LinkContext{}, cidlink.Link{Cid: root}, basicnode.Prototype.Any)
	if err != nil {
		return err
	}
	return progress.WalkAdv(rootNode, sel, func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error {
		return nil
	})
}

func (h *httpSubscriber) fetch(ctx context.Context, url string, cb func(io.Reader) error) error {
	req, err := http.NewRequestWithContext(ctx, "GET", path.Join(h.root, url), nil)
	if err != nil {
		return err
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non success http code at %s/%s: %d", h.root, url, resp.StatusCode)
	}

	defer resp.Body.Close()
	return cb(resp.Body)
}

func (h *httpSubscriber) fetchHead(ctx context.Context) (cid.Cid, error) {
	var cidStr string
	if err := h.fetch(ctx, "head", func(msg io.Reader) error {
		return json.NewDecoder(msg).Decode(&cidStr)
	}); err != nil {
		return cid.Undef, err
	}

	return cid.Decode(cidStr)
}

// fetch an item into the datastore at c if not locally avilable.
func (h *httpSubscriber) fetchBlock(ctx context.Context, c cid.Cid) error {
	n, err := h.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	// node is already present.
	if n != nil && err == nil {
		return nil
	}

	return h.fetch(ctx, c.String(), func(data io.Reader) error {
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
