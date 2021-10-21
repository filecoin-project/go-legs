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
			w.WriteHeader(500)
			//todo: log
		} else {
			_, _ = w.Write(out)
		}
		return
	}
	// interpret `ask` as a CID to serve.
	c, err := cid.Parse(ask)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid request: not a cid"))
		return
	}
	item, err := h.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	if err != nil {
		if errors.Is(err, ipld.ErrNotExists{}) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("cid not found"))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("unable to load data for cid"))
		// todo: log
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

func (h *httpPublisher) Close() error {
	return nil
}
