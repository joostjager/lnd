// +build invoicesrpc

package invoicesrpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"github.com/btcsuite/btcutil"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing"
	"github.com/lightningnetwork/lnd/zpay32"
	"google.golang.org/grpc"
	"gopkg.in/macaroon-bakery.v2/bakery"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/lnrpc"
)

const (
	// subServerName is the name of the sub rpc server. We'll use this name
	// to register ourselves, and we also require that the main
	// SubServerConfigDispatcher instance recognize it as the name of our
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

// New returns a new instance of the invoicesrpc Invoices sub-server. We also
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
	if !lnrpc.FileExists(macFilePath) && cfg.MacService != nil {
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

// RegisterWithRootServer will be called by the root gRPC server to direct a sub
// RPC server to register itself with the main gRPC root server. Until this is
// called, each sub-server won't be able to have requests routed towards it.
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

func (s *Server) CancelInvoice(ctx context.Context,
	in *CancelInvoiceMsg) (*CancelInvoiceResp, error) {

	if len(in.PaymentHash) != 32 {
		return nil, fmt.Errorf("invalid hash length")
	}

	var paymentHash [32]byte
	copy(paymentHash[:], in.PaymentHash)

	err := s.cfg.InvoiceRegistry.CancelInvoice(paymentHash)
	if err != nil {
		return nil, err
	}

	log.Infof("Canceled invoice %x", paymentHash)

	return &CancelInvoiceResp{}, nil
}

// AddInvoice attempts to add a new invoice to the invoice database. Any
// duplicated invoices are rejected, therefore all invoices *must* have a
// unique payment preimage.
func (s *Server) AddInvoice(ctx context.Context,
	invoice *lnrpc.Invoice) (*lnrpc.AddInvoiceResponse, error) {

	var paymentPreimage [32]byte

	switch {
	// If a preimage wasn't specified, then we'll generate a new preimage
	// from fresh cryptographic randomness.
	case len(invoice.RPreimage) == 0:
		if _, err := rand.Read(paymentPreimage[:]); err != nil {
			return nil, err
		}

	// Otherwise, if a preimage was specified, then it MUST be exactly
	// 32-bytes.
	case len(invoice.RPreimage) > 0 && len(invoice.RPreimage) != 32:
		return nil, fmt.Errorf("payment preimage must be exactly "+
			"32 bytes, is instead %v", len(invoice.RPreimage))

	// If the preimage meets the size specifications, then it can be used
	// as is.
	default:
		copy(paymentPreimage[:], invoice.RPreimage[:])
	}

	// The size of the memo, receipt and description hash attached must not
	// exceed the maximum values for either of the fields.
	if len(invoice.Memo) > channeldb.MaxMemoSize {
		return nil, fmt.Errorf("memo too large: %v bytes "+
			"(maxsize=%v)", len(invoice.Memo), channeldb.MaxMemoSize)
	}
	if len(invoice.Receipt) > channeldb.MaxReceiptSize {
		return nil, fmt.Errorf("receipt too large: %v bytes "+
			"(maxsize=%v)", len(invoice.Receipt), channeldb.MaxReceiptSize)
	}
	if len(invoice.DescriptionHash) > 0 && len(invoice.DescriptionHash) != 32 {
		return nil, fmt.Errorf("description hash is %v bytes, must be %v",
			len(invoice.DescriptionHash), channeldb.MaxPaymentRequestSize)
	}

	// The value of the invoice must not be negative.
	if invoice.Value < 0 {
		return nil, fmt.Errorf("payments of negative value "+
			"are not allowed, value is %v", invoice.Value)
	}

	amt := btcutil.Amount(invoice.Value)
	amtMSat := lnwire.NewMSatFromSatoshis(amt)

	// The value of the invoice must also not exceed the current soft-limit
	// on the largest payment within the network.
	if amtMSat > s.cfg.MaxPaymentMSat {
		return nil, fmt.Errorf("payment of %v is too large, max "+
			"payment allowed is %v", amt,
			s.cfg.MaxPaymentMSat.ToSatoshis(),
		)
	}

	// Next, generate the payment hash itself from the preimage. This will
	// be used by clients to query for the state of a particular invoice.
	rHash := sha256.Sum256(paymentPreimage[:])

	// We also create an encoded payment request which allows the
	// caller to compactly send the invoice to the payer. We'll create a
	// list of options to be added to the encoded payment request. For now
	// we only support the required fields description/description_hash,
	// expiry, fallback address, and the amount field.
	var options []func(*zpay32.Invoice)

	// We only include the amount in the invoice if it is greater than 0.
	// By not including the amount, we enable the creation of invoices that
	// allow the payee to specify the amount of satoshis they wish to send.
	if amtMSat > 0 {
		options = append(options, zpay32.Amount(amtMSat))
	}

	// If specified, add a fallback address to the payment request.
	if len(invoice.FallbackAddr) > 0 {
		addr, err := btcutil.DecodeAddress(invoice.FallbackAddr,
			s.cfg.ChainParams)
		if err != nil {
			return nil, fmt.Errorf("invalid fallback address: %v",
				err)
		}
		options = append(options, zpay32.FallbackAddr(addr))
	}

	// If expiry is set, specify it. If it is not provided, no expiry time
	// will be explicitly added to this payment request, which will imply
	// the default 3600 seconds.
	if invoice.Expiry > 0 {

		// We'll ensure that the specified expiry is restricted to sane
		// number of seconds. As a result, we'll reject an invoice with
		// an expiry greater than 1 year.
		maxExpiry := time.Hour * 24 * 365
		expSeconds := invoice.Expiry

		if float64(expSeconds) > maxExpiry.Seconds() {
			return nil, fmt.Errorf("expiry of %v seconds "+
				"greater than max expiry of %v seconds",
				float64(expSeconds), maxExpiry.Seconds())
		}

		expiry := time.Duration(invoice.Expiry) * time.Second
		options = append(options, zpay32.Expiry(expiry))
	}

	// If the description hash is set, then we add it do the list of options.
	// If not, use the memo field as the payment request description.
	if len(invoice.DescriptionHash) > 0 {
		var descHash [32]byte
		copy(descHash[:], invoice.DescriptionHash[:])
		options = append(options, zpay32.DescriptionHash(descHash))
	} else {
		// Use the memo field as the description. If this is not set
		// this will just be an empty string.
		options = append(options, zpay32.Description(invoice.Memo))
	}

	// We'll use our current default CLTV value unless one was specified as
	// an option on the command line when creating an invoice.
	switch {
	case invoice.CltvExpiry > math.MaxUint16:
		return nil, fmt.Errorf("CLTV delta of %v is too large, max "+
			"accepted is: %v", invoice.CltvExpiry, math.MaxUint16)
	case invoice.CltvExpiry != 0:
		options = append(options,
			zpay32.CLTVExpiry(invoice.CltvExpiry))
	default:
		// TODO(roasbeef): assumes set delta between versions
		defaultDelta := s.cfg.DefaultCLTVExpiry
		options = append(options, zpay32.CLTVExpiry(uint64(defaultDelta)))
	}

	// If we were requested to include routing hints in the invoice, then
	// we'll fetch all of our available private channels and create routing
	// hints for them.
	if invoice.Private {
		openChannels, err := s.cfg.ChanDB.FetchAllChannels()
		if err != nil {
			return nil, fmt.Errorf("could not fetch all channels")
		}

		graph := s.cfg.ChanDB.ChannelGraph()

		numHints := 0
		for _, channel := range openChannels {
			// We'll restrict the number of individual route hints
			// to 20 to avoid creating overly large invoices.
			if numHints > 20 {
				break
			}

			// Since we're only interested in our private channels,
			// we'll skip public ones.
			isPublic := channel.ChannelFlags&lnwire.FFAnnounceChannel != 0
			if isPublic {
				continue
			}

			// Make sure the counterparty has enough balance in the
			// channel for our amount. We do this in order to reduce
			// payment errors when attempting to use this channel
			// as a hint.
			chanPoint := lnwire.NewChanIDFromOutPoint(
				&channel.FundingOutpoint,
			)
			if amtMSat >= channel.LocalCommitment.RemoteBalance {
				log.Debugf("Skipping channel %v due to "+
					"not having enough remote balance",
					chanPoint)
				continue
			}

			// Make sure the channel is active.
			link, err := s.cfg.Switch.GetLink(chanPoint)
			if err != nil {
				log.Errorf("Unable to get link for "+
					"channel %v: %v", chanPoint, err)
				continue
			}

			if !link.EligibleToForward() {
				log.Debugf("Skipping channel %v due to not "+
					"being eligible to forward payments",
					chanPoint)
				continue
			}

			// To ensure we don't leak unadvertised nodes, we'll
			// make sure our counterparty is publicly advertised
			// within the network. Otherwise, we'll end up leaking
			// information about nodes that intend to stay
			// unadvertised, like in the case of a node only having
			// private channels.
			var remotePub [33]byte
			copy(remotePub[:], channel.IdentityPub.SerializeCompressed())
			isRemoteNodePublic, err := graph.IsPublicNode(remotePub)
			if err != nil {
				log.Errorf("Unable to determine if node %x "+
					"is advertised: %v", remotePub, err)
				continue
			}

			if !isRemoteNodePublic {
				log.Debugf("Skipping channel %v due to "+
					"counterparty %x being unadvertised",
					chanPoint, remotePub)
				continue
			}

			// Fetch the policies for each end of the channel.
			chanID := channel.ShortChanID().ToUint64()
			info, p1, p2, err := graph.FetchChannelEdgesByID(chanID)
			if err != nil {
				log.Errorf("Unable to fetch the routing "+
					"policies for the edges of the channel "+
					"%v: %v", chanPoint, err)
				continue
			}

			// Now, we'll need to determine which is the correct
			// policy for HTLCs being sent from the remote node.
			var remotePolicy *channeldb.ChannelEdgePolicy
			if bytes.Equal(remotePub[:], info.NodeKey1Bytes[:]) {
				remotePolicy = p1
			} else {
				remotePolicy = p2
			}

			// If for some reason we don't yet have the edge for
			// the remote party, then we'll just skip adding this
			// channel as a routing hint.
			if remotePolicy == nil {
				continue
			}

			// Finally, create the routing hint for this channel and
			// add it to our list of route hints.
			hint := routing.HopHint{
				NodeID:      channel.IdentityPub,
				ChannelID:   chanID,
				FeeBaseMSat: uint32(remotePolicy.FeeBaseMSat),
				FeeProportionalMillionths: uint32(
					remotePolicy.FeeProportionalMillionths,
				),
				CLTVExpiryDelta: remotePolicy.TimeLockDelta,
			}

			// Include the route hint in our set of options that
			// will be used when creating the invoice.
			routeHint := []routing.HopHint{hint}
			options = append(options, zpay32.RouteHint(routeHint))

			numHints++
		}

	}

	// Create and encode the payment request as a bech32 (zpay32) string.
	creationDate := time.Now()
	payReq, err := zpay32.NewInvoice(
		s.cfg.ChainParams, rHash, creationDate, options...,
	)
	if err != nil {
		return nil, err
	}

	payReqString, err := payReq.Encode(
		zpay32.MessageSigner{
			SignCompact: s.cfg.NodeSigner.SignDigestCompact,
		},
	)
	if err != nil {
		return nil, err
	}

	newInvoice := &channeldb.Invoice{
		CreationDate:   creationDate,
		Memo:           []byte(invoice.Memo),
		Receipt:        invoice.Receipt,
		PaymentRequest: []byte(payReqString),
		Terms: channeldb.ContractTerm{
			Value: amtMSat,
		},
	}
	copy(newInvoice.Terms.PaymentPreimage[:], paymentPreimage[:])

	log.Tracef("[addinvoice] adding new invoice %v",
		newLogClosure(func() string {
			return spew.Sdump(newInvoice)
		}),
	)

	// With all sanity checks passed, write the invoice to the database.
	addIndex, err := s.cfg.InvoiceRegistry.AddInvoice(newInvoice, rHash)
	if err != nil {
		return nil, err
	}

	return &lnrpc.AddInvoiceResponse{
		RHash:          rHash[:],
		PaymentRequest: payReqString,
		AddIndex:       addIndex,
	}, nil
}
