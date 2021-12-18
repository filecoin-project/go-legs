package dtsync

import (
	"context"
	"errors"
	"os"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("go-legs-dtsync")

type Sync struct {
	cancel      context.CancelFunc
	dtManager   dt.Manager
	gsExchange  graphsync.GraphExchange
	host        host.Host
	tmpDir      string
	unsubEvents dt.Unsubscribe

	// Map of CID of in-progress sync to sync done channel.
	syncDoneChans map[cid.Cid]chan<- error
	syncDoneMutex sync.Mutex
}

// Sync provides sync functionality for use with all datatransfer syncs.
func NewSync(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, dtManager dt.Manager) (*Sync, error) {
	ctx, cancel := context.WithCancel(context.Background())

	dtManager, gs, tmpDir, err := makeDataTransfer(ctx, host, ds, lsys, dtManager)
	if err != nil {
		cancel()
		return nil, err
	}

	s := &Sync{
		cancel:     cancel,
		dtManager:  dtManager,
		gsExchange: gs,
		host:       host,
		tmpDir:     tmpDir,
	}

	s.unsubEvents = dtManager.SubscribeToEvents(s.onEvent)
	return s, nil
}

func (s *Sync) GraphSync() graphsync.GraphExchange {
	return s.gsExchange
}

func (s *Sync) DataTransfer() dt.Manager {
	return s.dtManager
}

func (s *Sync) Close() error {
	s.unsubEvents()

	var err, errs error
	// If tmpDir is non-empty, that means this Sync started the dtManager and
	// it is ok to stop it and clean up the tmpDir.
	if s.tmpDir != "" {
		err = s.dtManager.Stop(context.Background())
		if err != nil {
			log.Errorf("Failed to stop datatransfer manager: %s", err)
			errs = multierror.Append(errs, err)
		}
		if err = os.RemoveAll(s.tmpDir); err != nil {
			log.Errorf("Failed to remove temp dir: %s", err)
			errs = multierror.Append(errs, err)
		}
	}

	// Dismiss any handlers waiting completion of sync.
	s.syncDoneMutex.Lock()
	if len(s.syncDoneChans) != 0 {
		log.Warnf("Closing datatransfer sync with %d syncs in progress", len(s.syncDoneChans))
	}
	for _, ch := range s.syncDoneChans {
		close(ch)
	}
	s.syncDoneChans = nil
	s.syncDoneMutex.Unlock()

	s.cancel()
	return errs
}

// NewSyncer creates a new Syncer to use for a single sync operation against a peer.
func (s *Sync) NewSyncer(peerID peer.ID, topicName string) *Syncer {
	return &Syncer{
		peerID:    peerID,
		sync:      s,
		topicName: topicName,
	}
}

// notifyOnSyncDone returns a channel that sync done notification is sent on.
func (s *Sync) notifyOnSyncDone(c cid.Cid) <-chan error {
	syncDone := make(chan error, 1)

	s.syncDoneMutex.Lock()
	defer s.syncDoneMutex.Unlock()

	if s.syncDoneChans == nil {
		s.syncDoneChans = make(map[cid.Cid]chan<- error)
	}
	s.syncDoneChans[c] = syncDone

	return syncDone
}

// signalSyncDone removes and closes the channel when the pending sync has
// completed.  Returns true if a channel was found.
func (s *Sync) signalSyncDone(c cid.Cid, err error) bool {
	s.syncDoneMutex.Lock()
	defer s.syncDoneMutex.Unlock()

	syncDone, ok := s.syncDoneChans[c]
	if !ok {
		return false
	}
	if len(s.syncDoneChans) == 1 {
		s.syncDoneChans = nil
	} else {
		delete(s.syncDoneChans, c)
	}

	if err != nil {
		syncDone <- err
	}
	close(syncDone)
	return true
}

// onEvent is called by the datatransfer manager to send events.
func (s *Sync) onEvent(event dt.Event, channelState dt.ChannelState) {
	var err error
	switch event.Code {
	default:
		// Ignore unrecognized event type.
		return
	case dt.FinishTransfer:
		// Tell the waiting handler that the sync has finished.
	case dt.Error:
		// Communicate the error back to the waiting handler.
		err = errors.New(event.Message)
	}

	// Send the FinishTransfer signal to the handler.  This will allow its
	// handle goroutine to distribute the update and exit.
	//
	// It is not necessary to return the channelState CID, since we already
	// know it is the correct on since it was used to look up this syncDone
	// channel.
	if !s.signalSyncDone(channelState.BaseCID(), err) {
		log.Errorw("Could not find channel for completed transfer notice", "cid", channelState.BaseCID())
		return
	}
}
