package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/filecoin-project/go-legs"
	maurl "github.com/filecoin-project/go-legs/http/multiaddr"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

var defaultPollTime = time.Hour

var log = logging.Logger("go-legs-http")

// NewHTTPSubscriber creates a legs subcriber that provides subscriptions
// from publishers identified by
func NewHTTPSubscriber(ctx context.Context, host *http.Client, publisher multiaddr.Multiaddr, lsys *ipld.LinkSystem, topic string, selector ipld.Node) (legs.LegSubscriber, error) {
	url, err := maurl.ToURL(publisher)
	if err != nil {
		return nil, err
	}
	hs := httpSubscriber{
		host,
		cid.Undef,
		*url,

		lsys,
		selector,
		sync.Mutex{},
		make(chan req, 1),
		make([]chan cid.Cid, 1),
	}
	go hs.background()
	return &hs, nil
}

type httpSubscriber struct {
	*http.Client
	head cid.Cid
	root url.URL

	lsys            *ipld.LinkSystem
	defaultSelector ipld.Node
	submtx          sync.Mutex
	// reqs is inbound requests for syncs from `Sync` calls
	reqs chan req
	subs []chan cid.Cid
}

var _ (legs.LegSubscriber) = (*httpSubscriber)(nil)

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
	defaultRate := time.NewTimer(defaultPollTime)
	for {
		// finish up from previous iteration
		if workResp != nil {
			workResp <- nextCid
			close(workResp)
			workResp = nil
		}
		if !defaultRate.Stop() {
			<-defaultRate.C
		}
		defaultRate.Reset(defaultPollTime)
		select {

		case r := <-h.reqs:
			nextCid = r.Cid
			workResp = r.resp
			sel = r.Selector
			ctx = r.ctx
		case <-defaultRate.C:
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
			continue
		}
		sel = legs.ExploreRecursiveWithStopNode(selector.RecursionLimitNone(),
			sel,
			cidlink.Link{Cid: h.head})
		if err := h.fetchBlock(ctx, nextCid); err != nil {
			log.Infow("failed to fetch requested block", "err", err)
			continue
		}
		xsel, err = selector.CompileSelector(sel)
		if err != nil {
			log.Infow("failed to compile selector", "err", err, "selector", sel)
			continue
		}

		err = h.walkFetch(ctx, nextCid, xsel)
		if err != nil {
			log.Infow("failed to walk requested dag", "err", err, "root", nextCid)
			continue
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
		c := l.(cidlink.Link).Cid
		if err := h.fetchBlock(ctx, c); err != nil {
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
		}{},
	}
	// get the direct node.
	rootNode, err := getMissingLs.Load(ipld.LinkContext{}, cidlink.Link{Cid: root}, basicnode.Prototype.Any)
	if err != nil {
		return err
	}
	return progress.WalkMatching(rootNode, sel, func(p traversal.Progress, n datamodel.Node) error {
		return nil
	})
}

func (h *httpSubscriber) fetch(ctx context.Context, rsrc string, cb func(io.Reader) error) error {
	localURL := h.root
	localURL.Path = path.Join(h.root.Path, rsrc)

	req, err := http.NewRequestWithContext(ctx, "GET", localURL.String(), nil)
	if err != nil {
		return err
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non success http code at %s: %d", localURL.String(), resp.StatusCode)
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
		writer, committer, err := h.lsys.StorageWriteOpener(ipld.LinkContext{})
		if err != nil {
			return err
		}
		if _, err := io.Copy(writer, data); err != nil {
			return err
		}
		return committer(cidlink.Link{Cid: c})
	})
}
