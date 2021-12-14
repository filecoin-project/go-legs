package dtsync

import (
	"context"
	"os"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs/gpubsub"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type simpleSetup struct {
	ctx    context.Context
	t      *pubsub.Topic
	dt     dt.Manager
	tmpDir string
}

func newSimpleSetup(ctx context.Context, host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string) (*simpleSetup, error) {
	t, err := gpubsub.MakePubsub(ctx, host, topic)
	if err != nil {
		return nil, err
	}
	dt, _, tmpDir, err := makeDataTransfer(ctx, host, ds, lsys, nil)
	if err != nil {
		return nil, err
	}

	return &simpleSetup{
		ctx, t, dt, tmpDir,
	}, nil
}

func (ss *simpleSetup) onClose() error {
	log.Debug("Closing legs simple setup")

	var err, errs error

	// If tmpDir is non-empty, that means the simpleSetup started the dtManager and
	// it is ok to stop is and clean up the tmpDir.
	if ss.tmpDir != "" {
		err = ss.dt.Stop(ss.ctx)
		if err != nil {
			log.Errorf("Failed to stop datatransfer manager: %s", err)
			errs = multierror.Append(errs, err)
		}
		if err = os.RemoveAll(ss.tmpDir); err != nil {
			log.Errorf("Failed to remove temp dir: %s", err)
			errs = multierror.Append(errs, err)
		}
	}

	// If simpleSetup owns the pubsub topic, then close it.
	if ss.t != nil {
		if err = ss.t.Close(); err != nil {
			log.Errorf("Failed to close pubsub topic: %s", err)
			errs = multierror.Append(errs, err)
		}
	}

	return errs
}
