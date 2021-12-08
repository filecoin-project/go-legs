package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"sync"

	"github.com/filecoin-project/go-legs"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ma "github.com/multiformats/go-multiaddr"
)

type httpPublisher struct {
	rl   sync.RWMutex
	root cid.Cid
	lsys ipld.LinkSystem
}

var _ legs.LegPublisher = (*httpPublisher)(nil)
var _ http.Handler = (*httpPublisher)(nil)

// NewPublisher creates a new http publisher
func NewPublisher(ctx context.Context,
	ds datastore.Batching,
	lsys ipld.LinkSystem) (legs.LegPublisher, error) {
	hp := &httpPublisher{}
	hp.lsys = lsys
	return hp, nil
}

func (h *httpPublisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ask := path.Base(r.URL.Path)
	if ask == "head" {
		// serve the
		h.rl.RLock()
		defer h.rl.RUnlock()
		out, err := json.Marshal(h.root.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Errorw("Failed to serve root", "err", err)
		} else {
			_, _ = w.Write(out)
		}
		return
	}
	// interpret `ask` as a CID to serve.
	c, err := cid.Parse(ask)
	if err != nil {
		http.Error(w, "invalid request: not a cid", http.StatusBadRequest)
		return
	}
	item, err := h.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	if err != nil {
		if errors.Is(err, ipld.ErrNotExists{}) {
			http.Error(w, "cid not found", http.StatusNotFound)
			return
		}
		http.Error(w, "unable to load data for cid", http.StatusInternalServerError)
		log.Errorw("Failed to load requested block", "err", err)
		return
	}
	// marshal to json and serve.
	_ = dagjson.Encode(item, w)
}

func (h *httpPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	h.rl.Lock()
	defer h.rl.Unlock()
	h.root = c
	return nil
}

func (h *httpPublisher) UpdateRootWithAddrs(ctx context.Context, c cid.Cid, _ []ma.Multiaddr) error {
	return h.UpdateRoot(ctx, c)
}

func (h *httpPublisher) Close() error {
	return nil
}
