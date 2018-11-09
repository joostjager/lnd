// +build invoicesrpc

package invoicesrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"gopkg.in/macaroon-bakery.v2/bakery"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/contractcourt"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnwire"
)

const (
	// subServerName is the name of the sub rpc server. We'll use this name
	// to register ourselves, and we also require that the main
	// SubServerConfigDispatcher instance recognize tt as the name of our
	// RPC service.
	subServerName = "InvoicesRPC"
)

var (
	// macaroonOps are the set of capabilities that our minted macaroon (if
	// it doesn't already exist) will have.
	macaroonOps = []bakery.Op{
		{
			Entity: "invoices",
			Action: "write",
		},
		{
			Entity: "invoices",
			Action: "read",
		},
	}

	// macPermissions maps RPC calls to the permissions they require.
	macPermissions = map[string][]bakery.Op{
		"/invoicesrpc.Invoices/SubscribeSingleInvoice": {{
			Entity: "invoices",
			Action: "read",
		}},
		"/invoicesrpc.Invoices/SettleInvoice": {{
			Entity: "invoices",
			Action: "write",
		}},
		"/invoicesrpc.Invoices/CancelInvoice": {{
			Entity: "invoices",
			Action: "write",
		}},
	}

	// DefaultInvoicesMacFilename is the default name of the invoices
	// macaroon that we expect to find via a file handle within the main
	// configuration file in this package.
	DefaultInvoicesMacFilename = "invoices.macaroon"
)

// Server is a sub-server of the main RPC server: the invoices RPC. This sub
// RPC server allows external callers to access the status of the invoices
// currently active within lnd, as well as configuring it at runtime.
type Server struct {
	started  int32 // To be used atomically.
	shutdown int32 // To be used atomically.

	quit chan struct{}

	cfg *Config
}

// A compile time check to ensure that Server fully implements the
// InvoicesServer gRPC service.
var _ InvoicesServer = (*Server)(nil)

// fileExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// New returns a new instance of the Invoices sub-server. We also
// return the set of permissions for the macaroons that we may create within
// this method. If the macaroons we need aren't found in the filepath, then
// we'll create them on start up. If we're unable to locate, or create the
// macaroons we need, then we'll return with an error.
func New(cfg *Config) (*Server, lnrpc.MacaroonPerms, error) {
	// If the path of the invoices macaroon wasn't specified, then we'll
	// assume that it's found at the default network directory.
	macFilePath := filepath.Join(
		cfg.NetworkDir, DefaultInvoicesMacFilename,
	)

	// Now that we know the full path of the invoices macaroon, we can
	// check to see if we need to create it or not.
	if !fileExists(macFilePath) && cfg.MacService != nil {
		log.Infof("Baking macaroons for invoices RPC Server at: %v",
			macFilePath)

		// At this point, we know that the invoices macaroon doesn't
		// yet, exist, so we need to create it with the help of the
		// main macaroon service.
		invoicesMac, err := cfg.MacService.Oven.NewMacaroon(
			context.Background(), bakery.LatestVersion, nil,
			macaroonOps...,
		)
		if err != nil {
			return nil, nil, err
		}
		invoicesMacBytes, err := invoicesMac.M().MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
		err = ioutil.WriteFile(macFilePath, invoicesMacBytes, 0644)
		if err != nil {
			os.Remove(macFilePath)
			return nil, nil, err
		}
	}

	server := &Server{
		cfg:  cfg,
		quit: make(chan struct{}, 1),
	}

	return server, macPermissions, nil
}

// Start launches any helper goroutines required for the Server to function.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Start() error {
	return nil
}

// Stop signals any active goroutines for a graceful closure.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Stop() error {
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

// RegisterWithRootServer will be called by the root gRPC server to direct a
// sub RPC server to register itself with the main gRPC root server. Until this
// is called, each sub-server won't be able to have
// requests routed towards it.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) RegisterWithRootServer(grpcServer *grpc.Server) error {
	// We make sure that we register it with the main gRPC server to ensure
	// all our methods are routed properly.
	RegisterInvoicesServer(grpcServer, s)

	log.Debugf("Invoices RPC server successfully registered with root " +
		"gRPC server")

	return nil
}

// SubscribeInvoices returns a uni-directional stream (server -> client) for
// notifying the client of invoice state changes.
func (s *Server) SubscribeSingleInvoice(req *lnrpc.PaymentHash,
	updateStream Invoices_SubscribeSingleInvoiceServer) error {

	hash, err := chainhash.NewHash(req.RHash)
	if err != nil {
		return err
	}

	invoiceClient := s.cfg.InvoiceRegistry.SubscribeSingleInvoice(*hash)
	defer invoiceClient.Cancel()

	for {
		select {
		case newInvoice := <-invoiceClient.Updates:
			rpcInvoice, err := lnrpc.CreateRPCInvoice(
				newInvoice, s.cfg.ChainParams,
			)
			if err != nil {
				return err
			}

			if err := updateStream.Send(rpcInvoice); err != nil {
				return err
			}

		case <-s.quit:
			return nil
		}
	}
}

func (s *Server) SettleInvoice(ctx context.Context,
	in *SettleInvoiceMsg) (*SettleInvoiceResp, error) {

	if len(in.PreImage) != 32 {
		return nil, fmt.Errorf("invalid preimage length")
	}

	paymentHash := chainhash.Hash(sha256.Sum256(in.PreImage))

	invoice, _, err := s.cfg.InvoiceRegistry.LookupInvoice(
		paymentHash,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query invoice registry: "+
			" %v", err)
	}

	switch invoice.Terms.State {
	case channeldb.ContractSettled:
		return &SettleInvoiceResp{}, nil
	case channeldb.ContractCanceled:
		return nil, errors.New("invoice has been removed")
	case channeldb.ContractOpen:
		return nil, errors.New("invoice not yet accepted")
	}

	var preimage [32]byte
	copy(preimage[:], in.PreImage)
	err = s.cfg.Switch.ProcessContractResolution(
		contractcourt.ResolutionMsg{
			SourceChan: htlcswitch.SwitchSettleHop,
			HtlcIndex:  invoice.AddIndex,
			PreImage:   &preimage,
		},
	)
	if err != nil {
		log.Errorf("unable to settle htlc: %v", err)
		return nil, err
	}

	// TODO: Set amount in accepted stage already
	err = s.cfg.InvoiceRegistry.SettleInvoice(
		paymentHash, lnwire.MilliSatoshi(0),
	)
	if err != nil {
		log.Errorf("unable to settle invoice: %v", err)
		return nil, err
	}

	log.Infof("Settled invoice %x", paymentHash)

	// TODO: update preimage in invoice?

	return &SettleInvoiceResp{}, nil
}

func (s *Server) CancelInvoice(ctx context.Context,
	in *CancelInvoiceMsg) (*CancelInvoiceResp, error) {

	if len(in.PaymentHash) != 32 {
		return nil, fmt.Errorf("invalid hash length")
	}

	var paymentHash [32]byte
	copy(paymentHash[:], in.PaymentHash)

	invoice, _, err := s.cfg.InvoiceRegistry.LookupInvoice(
		paymentHash,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query invoice registry: "+
			" %v", err)
	}

	switch invoice.Terms.State {
	case channeldb.ContractSettled:
		return nil, errors.New("invoice already settled")
	case channeldb.ContractCanceled:
		return &CancelInvoiceResp{}, nil
	}

	s.cfg.InvoiceRegistry.CancelInvoice(paymentHash)

	err = s.cfg.Switch.ProcessContractResolution(
		contractcourt.ResolutionMsg{
			SourceChan: htlcswitch.SwitchSettleHop,
			HtlcIndex:  invoice.AddIndex,
			Failure:    lnwire.FailUnknownPaymentHash{},
		},
	)
	if err != nil {
		log.Errorf("unable to cancel htlc: %v", err)
		return nil, err
	}

	// TODO: Move invoice to canceled state / remove

	log.Infof("Canceled invoice %x", paymentHash)

	return &CancelInvoiceResp{}, nil
}
