package httpsync

import (
	"context"
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
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
)

const defaultHttpTimeout = 10 * time.Second

var log = logging.Logger("go-legs-httpsync")

// purposely a type alias
type rateLimiterFor = func(publisher peer.ID) *rate.Limiter

// Sync provides sync functionality for use with all http syncs.
type Sync struct {
	blockHook func(peer.ID, cid.Cid)
	client    *http.Client
	lsys      ipld.LinkSystem
}

func NewSync(lsys ipld.LinkSystem, client *http.Client, blockHook func(peer.ID, cid.Cid)) *Sync {
	if client == nil {
		client = &http.Client{
			Timeout: defaultHttpTimeout,
		}
	}
	return &Sync{
		blockHook: blockHook,
		client:    client,
		lsys:      lsys,
	}
}

// NewSyncer creates a new Syncer to use for a single sync operation against a peer.
func (s *Sync) NewSyncer(peerID peer.ID, peerAddr multiaddr.Multiaddr, rateLimiter *rate.Limiter) (*Syncer, error) {
	rootURL, err := maurl.ToURL(peerAddr)
	if err != nil {
		return nil, err
	}

	return &Syncer{
		peerID:      peerID,
		rateLimiter: rateLimiter,
		rootURL:     *rootURL,
		sync:        s,
	}, nil
}

func (s *Sync) Close() {
	s.client.CloseIdleConnections()
}

var errHeadFromUnexpectedPeer = errors.New("found head signed from an unexpected peer")

type Syncer struct {
	peerID      peer.ID
	rateLimiter *rate.Limiter
	rootURL     url.URL
	sync        *Sync
}

func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	var head cid.Cid
	var pubKey ic.PubKey
	err := s.fetch(ctx, "head", func(msg io.Reader) error {
		var err error
		pubKey, head, err = openSignedHeadWithIncludedPubKey(msg)
		return err
	})

	if err != nil {
		return cid.Undef, err
	}

	peerIDFromSig, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return cid.Undef, err
	}

	if peerIDFromSig != s.peerID {
		return cid.Undef, errHeadFromUnexpectedPeer
	}

	return head, nil
}

func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
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

// walkFetch is run by a traversal of the selector.  For each block that the
// selector walks over, walkFetch will look to see if it can find it in the
// local data store. If it cannot, it will then go and get it over HTTP.  This
// emulates way libp2p/graphsync fetches data, but the actual fetch of data is
// done over HTTP.
func (s *Syncer) walkFetch(ctx context.Context, rootCid cid.Cid, sel selector.Selector) (err error) {
	// We run the block hook to emulate the behavior of graphsync's
	// `OnIncomingBlockHook` callback (gets called even if block is already stored
	// locally).

	// Track the order of cids we've seen during our traversal so we can call the
	// block hook function in the same order. We emulate the behavior of
	// graphsync's `OnIncomingBlockHook`, this means we call the blockhook even if
	// we have the block locally.
	// We are purposefully not doing this in the StorageReadOpener because the
	// hook can do anything, including deleting the block from the block store. If
	// it did that then we would not be able to continue our traversal. So instead
	// we remember the blocks seen during traversal and then call the hook at the
	// end when we no longer care what it does with the blocks.
	var traversalOrder []cid.Cid
	defer func() {
		if err == nil && s.sync.blockHook != nil {
			for _, c := range traversalOrder {
				s.sync.blockHook(s.peerID, c)
			}
		}
	}()
	getMissingLs := cidlink.DefaultLinkSystem()
	// trusted because it'll be hashed/verified on the way into the link system when fetched.
	getMissingLs.TrustedStorage = true
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (r io.Reader, err error) {
		defer func() {
			if err == nil && s.sync.blockHook != nil {
				traversalOrder = append(traversalOrder, l.(cidlink.Link).Cid)
			}
		}()

		r, err = s.sync.lsys.StorageReadOpener(lc, l)
		if err == nil {
			// Found block read opener, so return it.
			return r, nil
		}

		// Did not find block read opener, so fetch block via HTTP with re-try in case rate limit is
		// reached.
		c := l.(cidlink.Link).Cid
		for {
			if err := s.fetchBlock(ctx, c); err != nil {
				log.Errorw("Failed to fetch block", "err", err, "cid", c)
				if _, ok := err.(rateLimitErr); ok {
					//TODO: implement backoff to avoid potentially exhausting the HTTP source.
					continue
				}
				return nil, err
			}
			break
		}

		return s.sync.lsys.StorageReadOpener(lc, l)
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

type rateLimitErr struct {
	resource string
	rootURL  url.URL
	source   peer.ID
}

func (r rateLimitErr) Error() string {
	return fmt.Sprintf("rate limit reached when fetching %s from %s at %s", r.resource, r.source, r.rootURL.String())
}

func (s *Syncer) fetch(ctx context.Context, rsrc string, cb func(io.Reader) error) error {
	localURL := s.rootURL
	localURL.Path = path.Join(s.rootURL.Path, rsrc)

	if s.rateLimiter != nil {
		err := s.rateLimiter.Wait(ctx)
		if err != nil {
			return &rateLimitErr{
				resource: rsrc,
				rootURL:  s.rootURL,
				source:   s.peerID,
			}
		}
	}

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

// fetchBlock fetches an item into the datastore at c if not locally available.
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
			return err
		}
		return nil
	})
}
