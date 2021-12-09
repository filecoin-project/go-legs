package legs

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

const adminServerAddr = "0.0.0.0:49001"

type legPublisher struct {
	topic         *pubsub.Topic
	onClose       func() error
	host          host.Host
	headPublisher *head.Publisher
	adminSvr      *http.Server
}

// NewPublisher creates a new legs publisher
func NewPublisher(ctx context.Context,
	host host.Host,
	ds datastore.Batching,
	lsys ipld.LinkSystem,
	topic string) (LegPublisher, error) {
	ss, err := newSimpleSetup(ctx, host, ds, lsys, topic)
	if err != nil {
		log.Errorf("Failed to instantiate simple setup")
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)
	publisher := &legPublisher{topic: ss.t, onClose: ss.onClose, host: host, headPublisher: headPublisher}

	adminSvr := publisher.startAdminServer(adminServerAddr)
	publisher.adminSvr = adminSvr

	return publisher, nil
}

func startHeadPublisher(host host.Host, topic string, headPublisher *head.Publisher) {
	go func() {
		log.Infof("Starting head publisher on peer ID %s for topic %s", host.ID(), topic)
		err := headPublisher.Serve(host, topic)
		if err != http.ErrServerClosed {
			log.Errorf("Error head publisher stopped serving on peer ID %s for topic %s: %s", host.ID(), topic, err)
		}
		log.Infof("Stopped head publisher on peer ID %s for topic %s", host.ID(), topic)
	}()
}

// NewPublisherFromExisting instantiates go-legs publishing on an existing
// data transfer instance
func NewPublisherFromExisting(ctx context.Context,
	dt dt.Manager,
	host host.Host,
	topic string,
	lsys ipld.LinkSystem) (LegPublisher, error) {
	t, err := makePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}
	err = configureDataTransferForLegs(ctx, dt, lsys)
	if err != nil {
		return nil, err
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	publisher := &legPublisher{topic: t, onClose: t.Close, host: host, headPublisher: headPublisher}

	adminSvr := publisher.startAdminServer(adminServerAddr)
	publisher.adminSvr = adminSvr

	return publisher, nil
}

func (lp *legPublisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	err1 := lp.publish(ctx, c)
	err2 := lp.headPublisher.UpdateRoot(ctx, c)
	if err1 != nil {
		log.Errorw("Failed to publish root CID to pubsub channel", "root", c, "err", err1)
		return err1
	}

	log.Infow("Published root CID to pubsub channel", c)
	if err2 != nil {
		log.Errorw("Failed to update root in head publisher", "root", c, "err", err2)
		return err2
	}
	log.Infow("Updated root in head publisher", "root", c)
	return nil
}

func (lp *legPublisher) publish(ctx context.Context, c cid.Cid) error {
	msg := message{
		cid:   c,
		addrs: lp.host.Addrs(),
	}
	return lp.topic.Publish(ctx, encodeMessage(msg))
}

func (lp *legPublisher) Close() error {
	err1 := lp.headPublisher.Close()
	if err1 != nil {
		log.Errorw("Failed to close head publisher", "err", err1)
	}

	err2 := lp.onClose()
	if err2 != nil {
		log.Errorw("Failed to perform OnClose", "err", err2)
	}

	err3 := lp.adminSvr.Shutdown(context.Background())
	if err3 != nil {
		log.Errorw("Failed to shut down admin server", "err", err3)
	}

	if err1 != nil {
		return err1
	} else if err2 != nil {
		return err2
	}
	return err3
}

func (lp *legPublisher) startAdminServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/pub", func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {
		case http.MethodGet:
			root := lp.headPublisher.GetRoot()
			var body string
			if root == cid.Undef {
				log.Info("Root CID is not defined; nothing to get")
				body = "cid.Undef"
			} else {
				body = root.String()
			}
			if _, err := fmt.Fprintln(w, body); err != nil {
				log.Errorw("Failed to write root in response to GET /pub", "root", body, "err", err)
				return
			}
			log.Infow("Successfully responded to get root via http GET", "root", root)
		case http.MethodPost:

			var target cid.Cid

			q := r.URL.Query()
			cids := q.Get("cid")

			if cids == "" {
				target = lp.headPublisher.GetRoot()
				if target == cid.Undef {
					log.Info("Root CID is not defined; nothing to publish")
					http.Error(w, "cid.Undef: nothing to do. alternatively, specify explicit cid using `cid` query parameter.", http.StatusUnprocessableEntity)
					return
				}
			} else {
				decode, err := cid.Decode(cids)
				if err != nil {
					log.Errorw("Failed to parse given CID", "cid", cids, "err", err)
					http.Error(w, fmt.Sprintf("failed to parse cid %s: %s", cids, err), http.StatusUnprocessableEntity)
					return
				}
				target = decode
			}

			log.Info("Attempting to explicitly publish cid", "cid", target)
			if err := lp.publish(context.Background(), target); err != nil {
				log.Errorw("Failed to explicitly publish root via http POST", "cid", target, "err", err)
				http.Error(w, fmt.Sprintf("failed to publish cid %s on pubsub: %s", target, err), http.StatusInternalServerError)
				return
			}
			log.Infow("Successfully published cid via http POST", "cid", target)
			_, _ = fmt.Fprintf(w, "published cid %s to topic %s\n", target, lp.topic.String())
		default:
			http.Error(w, "unknown route", http.StatusNotFound)
		}
	})

	mux.HandleFunc("/admin/pub/topic/peers", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:

			peers := lp.topic.ListPeers()
			for _, p := range peers {
				_, _ = fmt.Fprintln(w, p.String())
			}
			_, _ = fmt.Fprintf(w, "topic has %d peers\n", len(peers))
			log.Infow("Listed topic peers ", "topic", lp.topic.String(), "peers", peers)
		default:
			http.Error(w, "unknown route", http.StatusNotFound)
		}
	})

	mux.HandleFunc("/admin/pub/peerstore", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			peerstore := lp.host.Peerstore()
			peers := peerstore.Peers()
			for _, p := range peers {
				info := peerstore.PeerInfo(p)
				protocols, err := peerstore.GetProtocols(p)
				if err != nil {
					log.Errorw("failed to get protocols for peer", "peer", p, "err", err)
				}
				_, _ = fmt.Fprintf(w, "%s: %v\n\tprotocols: %v\n", info.ID, info.Addrs, protocols)
			}
			_, _ = fmt.Fprintf(w, "peerstore has %d peers\n", peers.Len())
			log.Infow("Listed peerstore peers ", "peers", peers)
		case http.MethodPut:

			q := r.URL.Query()
			ids := q.Get("id")
			id, err := peer.Decode(ids)
			if err != nil {
				log.Errorw("invalid peer id", "id", ids, "err", err)
				http.Error(w, fmt.Sprintf("invalid peer id: %s %s", ids, err), http.StatusBadRequest)
				return
			}

			maddrs := q.Get("maddr")
			maddr, err := multiaddr.NewMultiaddr(maddrs)
			if err != nil {
				log.Errorw("invalid multiaddr", "maddr", maddrs, "err", err)
				http.Error(w, fmt.Sprintf("invalid multiaddr: %s %s", maddrs, err), http.StatusBadRequest)
				return
			}

			ttls := q.Get("ttl")
			ttl := time.Hour
			if ttls != "" {
				ttl, err = time.ParseDuration(ttls)
				if err != nil {
					log.Errorw("invalid ttl", "ttl", ttls, "err", err)
					http.Error(w, fmt.Sprintf("invalid ttl: %s %s", ttls, err), http.StatusBadRequest)
					return
				}
			} else {
				log.Infof("Using default ttl", "ttl", ttl)
			}

			peerstore := lp.host.Peerstore()
			peerstore.AddAddr(id, maddr, ttl)
			_, _ = fmt.Fprintf(w, "peer addr added:\n%s: %v (%v TTL)\n", id, maddr, ttl)
			log.Infow("Added peer address with ttl", "peer", id, "maddr", maddr, "ttl", ttl)

			protos := q.Get("protos")
			if protos != "" {
				protosS := strings.Split(protos, ",")
				if err := peerstore.AddProtocols(id, protosS...); err != nil {
					log.Errorw("Failed to add protocols", "peer", id, "protocols", protosS)
					_, _ = fmt.Fprintf(w, "failed to add protocols: %v\n\t%s\n", protosS, err)
					return
				}
				_, _ = fmt.Fprintf(w, "added protocols: %v", protosS)
			} else {
				_, _ = fmt.Fprintln(w, "no protocol for peer specified; skipped adding protocols")
			}
		default:
			http.Error(w, "unknown route", http.StatusNotFound)
		}
	})

	mux.HandleFunc("/admin/pub/connect", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			q := r.URL.Query()

			maddrs := q.Get("maddr")
			maddr, err := multiaddr.NewMultiaddr(maddrs)
			if err != nil {
				log.Errorw("invalid multiaddr", "maddr", maddrs, "err", err)
				http.Error(w, fmt.Sprintf("invalid multiaddr: %s %s", maddrs, err), http.StatusBadRequest)
				return
			}

			info, err := peer.AddrInfoFromP2pAddr(maddr)
			if err != nil {
				log.Errorw("Failed to get peer info from maddr", "maddr", maddr, "err", err)
				http.Error(w, fmt.Sprintf("cannot get addrinfo from multiaddr %s: %s", maddr, err), http.StatusBadRequest)
				return
			}

			if err := lp.host.Connect(context.Background(), *info); err != nil {
				log.Errorw("Failed to connect to peer", "addrinfo", info, "err", err)
				http.Error(w, fmt.Sprintf("failed to connect to %s: %s", info, err), http.StatusInternalServerError)
				return
			}
			if _, err := fmt.Fprintf(w, "connected successfully: %v\n", info); err != nil {
				log.Errorw("Failed to write HTTP response", "err", err)
			}
			log.Infow("successfully connected to peer", "peerinfo", info)
		default:
			http.Error(w, "unknown route", http.StatusNotFound)
		}
	})
	svr := &http.Server{Addr: addr, Handler: mux}
	go func() {

		err := svr.ListenAndServe()
		if err == http.ErrServerClosed {
			log.Infow("Stopped admin server", "addr", addr)
		}
		log.Errorw("Admin server failed", "addr", addr, "err", err)
	}()
	log.Infow("Started admin server", "addr", addr)
	return svr
}
