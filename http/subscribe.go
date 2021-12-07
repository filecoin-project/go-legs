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

// NewHTTPSubscriber creates a legs subscriber that provides subscriptions
// from publishers identified by
//
// A default selector sequence, dss, may optionally be specified. The selector sequence is used
// during sync traversal to define the extent by which a node is explored. If unspecified, all edges
// of nodes are recursively explored.
//
// Note that the default selector sequence is wrapped with a selector logic that will stop the
// traversal when the latest synced link is reached. Therefore, it must only specify the selection
// sequence itself.
//
// See: legs.ExploreRecursiveWithStopNode.
func NewHTTPSubscriber(ctx context.Context, host *http.Client, publisher multiaddr.Multiaddr, lsys *ipld.LinkSystem, topic string, dss ipld.Node) (legs.LegSubscriber, error) {
	url, err := maurl.ToURL(publisher)
	if err != nil {
		return nil, err
	}
	hs := httpSubscriber{
		Client: host,
		head:   cid.Undef,
		root:   *url,

		lsys: lsys,
		dss:  dss,
		mtx:  sync.Mutex{},
		reqs: make(chan req, 1),
		subs: make([]chan cid.Cid, 1),
	}
	go hs.background()
	return &hs, nil
}

type httpSubscriber struct {
	*http.Client
	root url.URL

	lsys *ipld.LinkSystem
	dss  ipld.Node
	// reqs is inbound requests for syncs from `Sync` calls
	reqs chan req

	// mtx protects state below accessed both by the background thread and public state
	mtx  sync.Mutex
	head cid.Cid
	subs []chan cid.Cid
}

var _ legs.LegSubscriber = (*httpSubscriber)(nil)

type req struct {
	cid.Cid
	dss  ipld.Node
	ctx  context.Context
	resp chan cid.Cid
}

func (h *httpSubscriber) OnChange() (chan cid.Cid, context.CancelFunc) {
	ch := make(chan cid.Cid)
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.subs = append(h.subs, ch)
	cncl := func() {
		h.mtx.Lock()
		defer h.mtx.Unlock()
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
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.head = c
	return nil
}

// Sync performs a one-off explicit sync from the given peer for a specific cid.
//
// Both cid and selector are optional parameters.
//
// If no cid is specified, i.e. the given cid equals cid.Undef, then the latest head is fetched from
// the remote publisher and used instead.
//
// If no selector is specified, the default selector sequence is used, wrapped with a logic that
// stops the traversal upon encountering the current head. See: legs.ExploreRecursiveWithStopNode.
// Otherwise, the given selector is used directly, without any wrapping.
//
// Note that if both the CID and the selector are unspecified this function behaves exactly like the
// background sync process, performing an explicit sync cycle for the latest head, updating the
// current head upon successful resolution.
//
// Specifying either a CID or a selector will not update the current head. This allows the caller to
// sync parts of a DAG selectively without updating the internal reference to the current head.
func (h *httpSubscriber) Sync(ctx context.Context, p peer.ID, c cid.Cid, selector ipld.Node) (<-chan cid.Cid, context.CancelFunc, error) {
	respChan := make(chan cid.Cid, 1)
	cctx, cncl := context.WithCancel(ctx)

	// todo: error if reqs is full
	h.reqs <- req{
		Cid:  c,
		dss:  selector,
		ctx:  cctx,
		resp: respChan,
	}
	return respChan, cncl, nil
}

func (h *httpSubscriber) Close() error {
	// cancel out subscribers.
	h.Client.CloseIdleConnections()
	h.mtx.Lock()
	defer h.mtx.Unlock()
	for _, ca := range h.subs {
		close(ca)
	}
	h.subs = make([]chan cid.Cid, 0)
	return nil
}

// background event loop for scheduling:
// a. time-scheduled fetches to the provider
// b. interrupted fetches in response to synchronous 'Sync' calls.
func (h *httpSubscriber) background() {
	var nextCid cid.Cid
	var workResp chan cid.Cid
	var ctx context.Context
	var sel ipld.Node
	var xsel selector.Selector
	var err error
	var updateHead bool
	defaultRate := time.NewTimer(defaultPollTime)
	for {
		// Finish up from previous iteration
		if workResp != nil {
			workResp <- nextCid
			close(workResp)
			workResp = nil
		}
		if !defaultRate.Stop() {
			<-defaultRate.C
		}
		defaultRate.Reset(defaultPollTime)

		// Get next request to handle
		select {
		case r := <-h.reqs:
			nextCid = r.Cid
			workResp = r.resp
			sel = r.dss
			ctx = r.ctx
			// Decide if successful resolution of nextCid should replace current head.
			// Replace the current head if both the selector and nextCid are absent.
			updateHead = sel == nil && nextCid == cid.Undef
		case <-defaultRate.C:
			nextCid = cid.Undef
			workResp = nil
			ctx = context.Background()
			sel = nil
			updateHead = true
		}

		// If no CID is given, use the latest head fetched from remote head publisher.
		if nextCid == cid.Undef {
			nextCid, err = h.fetchHead(ctx)
			if err != nil {
				log.Warnf("failed to fetch new head: %s", err)
				err = nil
				continue
			}
		}

		// If no selector is given, use the default selector sequence wrapped with stop logic
		if sel == nil {
			h.mtx.Lock()
			currHead := h.head
			h.mtx.Unlock()
			sel = legs.ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), h.dss, cidlink.Link{Cid: currHead})
		}

		if err := h.fetchBlock(ctx, nextCid); err != nil {
			log.Errorw("Failed to fetch requested block", "err", err)
			continue
		}

		xsel, err = selector.CompileSelector(sel)
		if err != nil {
			log.Errorw("Failed to compile selector", "err", err, "selector", sel)
			continue
		}

		err = h.walkFetch(ctx, nextCid, xsel)
		if err != nil {
			log.Errorw("Failed to traverse requested dag", "err", err, "root", nextCid)
			continue
		}

		// If head should be updated, make it so.
		if updateHead {
			h.mtx.Lock()
			h.head = nextCid
			h.mtx.Unlock()
		}
	}
}

func (h *httpSubscriber) walkFetch(ctx context.Context, root cid.Cid, sel selector.Selector) error {
	getMissingLs := cidlink.DefaultLinkSystem()
	// trusted because it'll be hashed/verified on the way into the link system when fetched.
	getMissingLs.TrustedStorage = true
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		r, err := h.lsys.StorageReadOpener(lc, l)
		if err == nil {
			log.Errorw("Failed to get block read opener", "err", err, "link", l)
			return r, nil
		}
		// get.
		c := l.(cidlink.Link).Cid
		if err := h.fetchBlock(ctx, c); err != nil {
			log.Errorw("Failed to fetch block", "err", err, "cid", c)
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
	}
	// get the direct node.
	rootNode, err := getMissingLs.Load(ipld.LinkContext{}, cidlink.Link{Cid: root}, basicnode.Prototype.Any)
	if err != nil {
		log.Errorw("Failed to load node", "root", root)
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
		log.Errorw("Failed to execute fetch request", "err", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("non success http code at %s: %d", localURL.String(), resp.StatusCode)
		log.Errorw("Fetch was not successful", "err", err)
		return err
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
			log.Errorw("Failed to get write opener", "err", err)
			return err
		}
		if _, err := io.Copy(writer, data); err != nil {
			return err
		}
		err = committer(cidlink.Link{Cid: c})
		if err != nil {
			log.Errorw("Failed to commit ")
		}
		return err
	})
}
