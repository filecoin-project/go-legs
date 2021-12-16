package httpsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	maurl "github.com/filecoin-project/go-legs/httpsync/multiaddr"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/multiformats/go-multiaddr"
)

const defaultHttpTimeout = 10 * time.Second

var log = logging.Logger("go-legs-httpsync")

// Sync provides sync functionality for use with all http syncs.
type Sync struct {
	client *http.Client
	lsys   ipld.LinkSystem
}

func NewSync(lsys ipld.LinkSystem, client *http.Client) *Sync {
	if client == nil {
		client = &http.Client{
			Timeout: defaultHttpTimeout,
		}
	}
	return &Sync{
		client: client,
		lsys:   lsys,
	}
}

// NewSyncer creates a new Syncer to use for a single sync operation against a peer.
func (s *Sync) NewSyncer(publisher multiaddr.Multiaddr) (*Syncer, error) {
	rootURL, err := maurl.ToURL(publisher)
	if err != nil {
		return nil, err
	}

	return &Syncer{
		sync:    s,
		rootURL: *rootURL,
	}, nil
}

func (s *Sync) Close() {
	s.client.CloseIdleConnections()
}

type Syncer struct {
	sync    *Sync
	rootURL url.URL
}

func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	var cidStr string
	err := s.fetch(ctx, "head", func(msg io.Reader) error {
		return json.NewDecoder(msg).Decode(&cidStr)
	})
	if err != nil {
		return cid.Undef, err
	}

	return cid.Decode(cidStr)
}

func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	err := s.fetchBlock(ctx, nextCid)
	if err != nil {
		msg := "failed to fetch requested block"
		log.Errorw(msg, "err", err)
		return errors.New(msg)
	}

	xsel, err := selector.CompileSelector(sel)
	if err != nil {
		msg := "failed to compile selector"
		log.Errorw(msg, "err", err, "selector", sel)
		return errors.New(msg)
	}

	err = s.walkFetch(ctx, nextCid, xsel)
	if err != nil {
		msg := "failed to traverse requested dag"
		log.Errorw(msg, "err", err, "root", nextCid)
		return errors.New(msg)
	}

	s.sync.client.CloseIdleConnections()
	return nil
}

func (s *Syncer) walkFetch(ctx context.Context, rootCid cid.Cid, sel selector.Selector) error {
	getMissingLs := cidlink.DefaultLinkSystem()
	// trusted because it'll be hashed/verified on the way into the link system when fetched.
	getMissingLs.TrustedStorage = true
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		r, err := s.sync.lsys.StorageReadOpener(lc, l)
		if err != nil {
			log.Errorw("Failed to get block read opener", "err", err, "link", l)
			// get.
			c := l.(cidlink.Link).Cid
			if err := s.fetchBlock(ctx, c); err != nil {
				log.Errorw("Failed to fetch block", "err", err, "cid", c)
				return nil, err
			}
			return s.sync.lsys.StorageReadOpener(lc, l)
		}
		return r, nil
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
	rootNode, err := getMissingLs.Load(ipld.LinkContext{}, cidlink.Link{Cid: rootCid}, basicnode.Prototype.Any)
	if err != nil {
		log.Errorw("Failed to load node", "root", rootCid)
		return err
	}
	return progress.WalkMatching(rootNode, sel, func(p traversal.Progress, n datamodel.Node) error {
		return nil
	})
}

func (s *Syncer) fetch(ctx context.Context, rsrc string, cb func(io.Reader) error) error {
	localURL := s.rootURL
	localURL.Path = path.Join(s.rootURL.Path, rsrc)

	req, err := http.NewRequestWithContext(ctx, "GET", localURL.String(), nil)
	if err != nil {
		return err
	}

	resp, err := s.sync.client.Do(req)
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

// fetchBlock fetches an item into the datastore at c if not locally avilable.
func (s *Syncer) fetchBlock(ctx context.Context, c cid.Cid) error {
	n, err := s.sync.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	// node is already present.
	if n != nil && err == nil {
		return nil
	}

	return s.fetch(ctx, c.String(), func(data io.Reader) error {
		writer, committer, err := s.sync.lsys.StorageWriteOpener(ipld.LinkContext{})
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
