package dtsync

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("go-legs-dtsync")

type inProgressSyncKey struct {
	c    cid.Cid
	peer peer.ID
}

// Sync provides sync functionality for use with all datatransfer syncs.
type Sync struct {
	dtManager   dt.Manager
	dtClose     dtCloseFunc
	host        host.Host
	unsubEvents dt.Unsubscribe
	unregHook   graphsync.UnregisterHookFunc

	// Map of CID of in-progress sync to sync done channel.
	syncDoneChans map[inProgressSyncKey]chan<- error
	syncDoneMutex sync.Mutex
}

// NewSyncWithDT creates a new Sync with a datatransfer.Manager provided by the
// caller.
func NewSyncWithDT(host host.Host, dtManager dt.Manager) (*Sync, error) {
	registerVoucher(dtManager)
	s := &Sync{
		host:      host,
		dtManager: dtManager,
	}

	s.unsubEvents = dtManager.SubscribeToEvents(s.onEvent)
	return s, nil
}

// NewSync creates a new Sync with its own datatransfer.Manager.
func NewSync(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, blockHook func(peer.ID, cid.Cid)) (*Sync, error) {
	dtManager, gs, dtClose, err := makeDataTransfer(host, ds, lsys)
	if err != nil {
		return nil, err
	}

	s := &Sync{
		host:      host,
		dtManager: dtManager,
		dtClose:   dtClose,
	}

	if blockHook != nil {
		s.unregHook = gs.RegisterIncomingBlockHook(makeIncomingBlockHook(blockHook))
	}

	s.unsubEvents = dtManager.SubscribeToEvents(s.onEvent)
	return s, nil
}

func makeIncomingBlockHook(blockHook func(peer.ID, cid.Cid)) graphsync.OnIncomingBlockHook {
	return func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		blockHook(p, blockData.Link().(cidlink.Link).Cid)
	}
}

// Close unregisters datatransfer event notification. If this Sync owns the
// datatransfer.Manager then the Manager is stopped.
func (s *Sync) Close() error {
	s.unsubEvents()
	if s.unregHook != nil {
		s.unregHook()
	}

	var err error
	if s.dtClose != nil {
		err = s.dtClose()
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

	return err
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
func (s *Sync) notifyOnSyncDone(k inProgressSyncKey) <-chan error {
	syncDone := make(chan error, 1)

	s.syncDoneMutex.Lock()
	defer s.syncDoneMutex.Unlock()

	if s.syncDoneChans == nil {
		s.syncDoneChans = make(map[inProgressSyncKey]chan<- error)
	}
	s.syncDoneChans[k] = syncDone

	return syncDone
}

// signalSyncDone removes and closes the channel when the pending sync has
// completed.  Returns true if a channel was found.
func (s *Sync) signalSyncDone(k inProgressSyncKey, err error) bool {
	s.syncDoneMutex.Lock()
	defer s.syncDoneMutex.Unlock()

	syncDone, ok := s.syncDoneChans[k]
	if !ok {
		return false
	}
	if len(s.syncDoneChans) == 1 {
		s.syncDoneChans = nil
	} else {
		delete(s.syncDoneChans, k)
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
	case dt.Cancel, dt.RequestCancelled, dt.RequestTimedOut, dt.SendDataError, dt.ReceiveDataError, dt.Disconnected:
		// The request was canceled; inform waiting handler.
		err = fmt.Errorf("%s: %s", dt.Events[event.Code], event.Message)
	case dt.Error:
		// Communicate the error back to the waiting handler.
		if strings.HasSuffix(event.Message, "content not found") {
			err = errors.New("datatransfer error: content not found")
		} else {
			err = fmt.Errorf("datatransfer error: %s", event.Message)
		}
	}

	// Send the FinishTransfer signal to the handler.  This will allow its
	// handle goroutine to distribute the update and exit.
	//
	// It is not necessary to return the channelState CID, since we already
	// know it is the correct on since it was used to look up this syncDone
	// channel.
	if !s.signalSyncDone(inProgressSyncKey{channelState.BaseCID(), channelState.OtherPeer()}, err) {
		// No channel to return error on, so log it here.
		if err != nil {
			log.Error(err.Error())
		}
		log.Errorw("Could not find channel for completed transfer notice", "cid", channelState.BaseCID())
		return
	}
}
