package chainrpc

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/lnrpc"
	"google.golang.org/grpc"
	"gopkg.in/macaroon-bakery.v2/bakery"
)

const (
	// subServerName is the name of the RPC sub-server. We'll use this name
	// to register ourselves, and we also require that the main
	// SubServerConfigDispatcher instance recognize this as the name of the
	// config file that we need.
	subServerName = "ChainRPC"
)

var (
	// macaroonOps are the set of capabilities that our minted macaroon (if
	// it doesn't already exist) will have.
	macaroonOps = []bakery.Op{
		{
			Entity: "chainnotifier",
			Action: "generate",
		},
	}

	// macPermissions maps RPC calls to the permissions they require.
	macPermissions = map[string][]bakery.Op{
		// TODO(wilmer): macaroons.
	}

	// DefaultChainNotifierMacFilename is the default name of the chain
	// notifier macaroon that we expect to find via a file handle within the
	// main configuration file in this package.
	DefaultChainNotifierMacFilename = "chainnotifier.macaroon"

	// ErrChainNotifierServerShuttingDown is an error returned when we are
	// waiting for a notification to arrive but the chain notifier server
	// has been shut down.
	ErrChainNotifierServerShuttingDown = errors.New("ChainNotifierServer " +
		"shutting down")
)

// fileExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// Server is a sub-server of the main RPC server: the chain notifier RPC. This
// RPC sub-server allows external callers to access the full chain notifier
// capabilities of lnd. This allows callers to create custom protocols, external
// to lnd, even backed by multiple distinct lnd across independent failure
// domains.
type Server struct {
	started uint32
	stopped uint32

	cfg Config

	quit chan struct{}
}

// New returns a new instance of the chainrpc ChainNotifier sub-server. We also
// return the set of permissions for the macaroons that we may create within
// this method. If the macaroons we need aren't found in the filepath, then
// we'll create them on start up. If we're unable to locate, or create the
// macaroons we need, then we'll return with an error.
func New(cfg *Config) (*Server, lnrpc.MacaroonPerms, error) {
	// If the path of the chain notifier macaroon wasn't generated, then
	// we'll assume that it's found at the default network directory.
	if cfg.ChainNotifierMacPath == "" {
		cfg.ChainNotifierMacPath = filepath.Join(
			cfg.NetworkDir, DefaultChainNotifierMacFilename,
		)
	}

	// Now that we know the full path of the chain notifier macaroon, we can
	// check to see if we need to create it or not.
	macFilePath := cfg.ChainNotifierMacPath
	if cfg.MacService != nil && !fileExists(macFilePath) {
		log.Infof("Baking macaroons for ChainNotifier RPC Server at: %v",
			macFilePath)

		// At this point, we know that the chain notifier macaroon
		// doesn't yet, exist, so we need to create it with the help of
		// the main macaroon service.
		chainNotifierMac, err := cfg.MacService.Oven.NewMacaroon(
			context.Background(), bakery.LatestVersion, nil,
			macaroonOps...,
		)
		if err != nil {
			return nil, nil, err
		}
		chainNotifierMacBytes, err := chainNotifierMac.M().MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
		err = ioutil.WriteFile(macFilePath, chainNotifierMacBytes, 0644)
		if err != nil {
			os.Remove(macFilePath)
			return nil, nil, err
		}
	}

	return &Server{cfg: *cfg}, macPermissions, nil
}

// A compile-time check to ensure that Server fully implements the
// ChainNotifierServer gRPC service.
var _ ChainNotifierServer = (*Server)(nil)

// A compile-time check to ensure that Server fully implements the
// lnrpc.SubServer interface.
var _ lnrpc.SubServer = (*Server)(nil)

// Start launches any helper goroutines required for the server to function.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Start() error {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return nil
	}

	return nil
}

// Stop signals any active goroutines for a graceful closure.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapUint32(&s.stopped, 0, 1) {
		return nil
	}

	close(s.quit)

	return nil
}

// Name returns a unique string representation of the sub-server. This can be
// used to identify the sub-server and also de-duplicate them.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Name() string {
	return subServerName
}

// RegisterWithRootServer will be called by the root gRPC server to direct a RPC
// sub-server to register itself with the main gRPC root server. Until this is
// called, each sub-server won't be able to have requests routed towards it.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) RegisterWithRootServer(grpcServer *grpc.Server) error {
	// We make sure that we register it with the main gRPC server to ensure
	// all our methods are routed properly.
	RegisterChainNotifierServer(grpcServer, s)

	log.Debug("ChainNotifier RPC server successfully register with root " +
		"gRPC server")

	return nil
}

// RegisterConfirmationsNtfn ...
//
// NOTE: This is part of the chainrpc.ChainNotifierService interface.
func (s *Server) RegisterConfirmationsNtfn(in *ConfRequest,
	confStream ChainNotifier_RegisterConfirmationsNtfnServer) error {

	var (
		confEvent *chainntnfs.ConfirmationEvent
		err       error
	)

	var txid chainhash.Hash
	copy(txid[:], in.Txid)

	if txid == chainntnfs.ZeroHash {
		confEvent, err = s.cfg.ChainNotifier.RegisterConfirmationsNtfn(
			nil, in.Script, in.NumConfs, in.HeightHint,
		)
	} else {
		confEvent, err = s.cfg.ChainNotifier.RegisterConfirmationsNtfn(
			&txid, in.Script, in.NumConfs, in.HeightHint,
		)
	}
	if err != nil {
		return err
	}

	// TODO(wilmer): implement Cancel?

	for {
		// TODO(wilmer): need to signal when event is no longer under
		// reorg limit.
		select {
		case confDetails := <-confEvent.Confirmed:
			conf := &ConfEvent{
				Event: &ConfEvent_Conf{
					Conf: &ConfDetails{
						BlockHash:   confDetails.BlockHash[:],
						BlockHeight: confDetails.BlockHeight,
						TxIndex:     confDetails.TxIndex,
						// TODO(wilmer): return raw tx.
					},
				},
			}
			if err := confStream.Send(conf); err != nil {
				return err
			}

		case reorgDepth := <-confEvent.NegativeConf:
			reorg := &ConfEvent{
				Event: &ConfEvent_Reorg{
					Reorg: &Reorg{
						Depth: uint32(reorgDepth),
					},
				},
			}
			if err := confStream.Send(reorg); err != nil {
				return err
			}

		case <-s.quit:
			return ErrChainNotifierServerShuttingDown
		}
	}

	return nil
}

// RegisterSpendNtfn ...
//
// NOTE: This is part of the chainrpc.ChainNotifierService interface.
func (s *Server) RegisterSpendNtfn(in *SpendRequest,
	spendStream ChainNotifier_RegisterSpendNtfnServer) error {

	var (
		spendEvent *chainntnfs.SpendEvent
		err        error
	)

	var txid chainhash.Hash
	copy(txid[:], in.Outpoint.Hash)
	op := wire.OutPoint{Hash: txid, Index: in.Outpoint.Index}

	if op == chainntnfs.ZeroOutPoint {
		spendEvent, err = s.cfg.ChainNotifier.RegisterSpendNtfn(
			nil, in.Script, in.HeightHint,
		)
	} else {
		spendEvent, err = s.cfg.ChainNotifier.RegisterSpendNtfn(
			&op, in.Script, in.HeightHint,
		)
	}
	if err != nil {
		return err
	}
	defer spendEvent.Cancel()

	for {
		// TODO(wilmer): need to signal when event is no longer under
		// reorg limit.
		select {
		case spendDetails := <-spendEvent.Spend:
			spend := &SpendEvent{
				Event: &SpendEvent_Spend{
					Spend: &SpendDetails{
						SpendingOutpoint: &Outpoint{
							Hash:  spendDetails.SpentOutPoint.Hash[:],
							Index: spendDetails.SpentOutPoint.Index,
						},
						// TODO(wilmer): return raw tx.
						SpendingTxHash:     spendDetails.SpenderTxHash[:],
						SpendingInputIndex: spendDetails.SpenderInputIndex,
						SpendingHeight:     uint32(spendDetails.SpendingHeight),
					},
				},
			}
			if err := spendStream.Send(spend); err != nil {
				return err
			}

		case _ = <-spendEvent.Reorg:
			reorg := &SpendEvent{
				Event: &SpendEvent_Reorg{
					Reorg: &Reorg{
						// TODO(wilmer): set this correctly.
						Depth: 0,
					},
				},
			}
			if err := spendStream.Send(reorg); err != nil {
				return err
			}

		case <-s.quit:
			return ErrChainNotifierServerShuttingDown
		}
	}

	return nil
}

// RegisterBlockEpochNtfn ...
//
// NOTE: This is part of the chainrpc.ChainNotifierService interface.
func (s *Server) RegisterBlockEpochNtfn(in *BlockEpoch,
	epochStream ChainNotifier_RegisterBlockEpochNtfnServer) error {

	var hash chainhash.Hash
	copy(hash[:], in.Hash)

	var blockEpoch *chainntnfs.BlockEpoch
	if hash != chainntnfs.ZeroHash && in.Height != 0 {
		blockEpoch = &chainntnfs.BlockEpoch{
			Hash:   &hash,
			Height: int32(in.Height),
		}
	}

	epochEvent, err := s.cfg.ChainNotifier.RegisterBlockEpochNtfn(blockEpoch)
	if err != nil {
		return err
	}
	defer epochEvent.Cancel()

	for {
		select {
		case blockEpoch := <-epochEvent.Epochs:
			epoch := &BlockEpoch{
				Hash:   blockEpoch.Hash[:],
				Height: uint32(blockEpoch.Height),
			}
			if err := epochStream.Send(epoch); err != nil {
				return err
			}
		case <-s.quit:
			return ErrChainNotifierServerShuttingDown
		}
	}

	return nil
}
