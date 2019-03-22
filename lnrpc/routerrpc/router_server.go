// +build routerrpc

package routerrpc

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/btcsuite/btcutil"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing"
	"github.com/lightningnetwork/lnd/routing/route"
	"google.golang.org/grpc"
	"gopkg.in/macaroon-bakery.v2/bakery"
)

const (
	// subServerName is the name of the sub rpc server. We'll use this name
	// to register ourselves, and we also require that the main
	// SubServerConfigDispatcher instance recognize as the name of our
	subServerName = "RouterRPC"
)

var (
	// macaroonOps are the set of capabilities that our minted macaroon (if
	// it doesn't already exist) will have.
	macaroonOps = []bakery.Op{
		{
			Entity: "offchain",
			Action: "write",
		},
	}

	// macPermissions maps RPC calls to the permissions they require.
	macPermissions = map[string][]bakery.Op{
		"/routerrpc.Router/SendPayment": {{
			Entity: "offchain",
			Action: "write",
		}},
		"/routerrpc.Router/LookupPayment": {{
			Entity: "offchain",
			Action: "write",
		}},
		"/routerrpc.Router/EstimateRouteFee": {{
			Entity: "offchain",
			Action: "read",
		}},
	}

	// DefaultRouterMacFilename is the default name of the router macaroon
	// that we expect to find via a file handle within the main
	// configuration file in this package.
	DefaultRouterMacFilename = "router.macaroon"
)

type paymentResult struct {
	err      error
	preimage lntypes.Preimage
	route    *route.Route
}

// Server is a stand alone sub RPC server which exposes functionality that
// allows clients to route arbitrary payment through the Lightning Network.
type Server struct {
	cfg *Config

	pendingPayments map[lntypes.Hash]chan paymentResult
	outcomes        map[lntypes.Hash]paymentResult
	lock            sync.Mutex
}

// A compile time check to ensure that Server fully implements the RouterServer
// gRPC service.
var _ RouterServer = (*Server)(nil)

// fileExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// New creates a new instance of the RouterServer given a configuration struct
// that contains all external dependencies. If the target macaroon exists, and
// we're unable to create it, then an error will be returned. We also return
// the set of permissions that we require as a server. At the time of writing
// of this documentation, this is the same macaroon as as the admin macaroon.
func New(cfg *Config) (*Server, lnrpc.MacaroonPerms, error) {
	// If the path of the router macaroon wasn't generated, then we'll
	// assume that it's found at the default network directory.
	if cfg.RouterMacPath == "" {
		cfg.RouterMacPath = filepath.Join(
			cfg.NetworkDir, DefaultRouterMacFilename,
		)
	}

	// Now that we know the full path of the router macaroon, we can check
	// to see if we need to create it or not.
	macFilePath := cfg.RouterMacPath
	if !fileExists(macFilePath) && cfg.MacService != nil {
		log.Infof("Making macaroons for Router RPC Server at: %v",
			macFilePath)

		// At this point, we know that the router macaroon doesn't yet,
		// exist, so we need to create it with the help of the main
		// macaroon service.
		routerMac, err := cfg.MacService.Oven.NewMacaroon(
			context.Background(), bakery.LatestVersion, nil,
			macaroonOps...,
		)
		if err != nil {
			return nil, nil, err
		}
		routerMacBytes, err := routerMac.M().MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
		err = ioutil.WriteFile(macFilePath, routerMacBytes, 0644)
		if err != nil {
			os.Remove(macFilePath)
			return nil, nil, err
		}
	}

	routerServer := &Server{
		cfg:             cfg,
		pendingPayments: make(map[lntypes.Hash]chan paymentResult),
		outcomes:        make(map[lntypes.Hash]paymentResult),
	}

	return routerServer, macPermissions, nil
}

// Start launches any helper goroutines required for the rpcServer to function.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Start() error {
	return nil
}

// Stop signals any active goroutines for a graceful closure.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Stop() error {
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
// is called, each sub-server won't be able to have requests routed towards it.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) RegisterWithRootServer(grpcServer *grpc.Server) error {
	// We make sure that we register it with the main gRPC server to ensure
	// all our methods are routed properly.
	RegisterRouterServer(grpcServer, s)

	log.Debugf("Router RPC server successfully register with root gRPC " +
		"server")

	return nil
}

// SendPayment attempts to route a payment described by the passed
// PaymentRequest to the final destination. If we are unable to route the
// payment, or cannot find a route that satisfies the constraints in the
// PaymentRequest, then an error will be returned. Otherwise, the payment
// pre-image, along with the final route will be returned.
func (s *Server) SendPayment(ctx context.Context,
	req *PaymentRequest) (*PaymentResponse, error) {

	payment, err := s.cfg.RouterBackend.ExtractIntentFromSendRequest(req)
	if err != nil {
		return nil, err
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.pendingPayments[payment.PaymentHash]; ok {
		return nil, errors.New("payment already in flight")
	}

	c := make(chan paymentResult)
	s.pendingPayments[payment.PaymentHash] = c

	go func() {
		preImage, route, err := s.cfg.Router.SendPayment(payment)

		c <- paymentResult{
			err:      err,
			preimage: preImage,
			route:    route,
		}
	}()

	return &PaymentResponse{}, nil
}

// EstimateRouteFee allows callers to obtain a lower bound w.r.t how much it
// may cost to send an HTLC to the target end destination.
func (s *Server) EstimateRouteFee(ctx context.Context,
	req *RouteFeeRequest) (*RouteFeeResponse, error) {

	if len(req.Dest) != 33 {
		return nil, errors.New("invalid length destination key")
	}
	var destNode route.Vertex
	copy(destNode[:], req.Dest)

	// Next, we'll convert the amount in satoshis to mSAT, which are the
	// native unit of LN.
	amtMsat := lnwire.NewMSatFromSatoshis(btcutil.Amount(req.AmtSat))

	// Pick a fee limit
	//
	// TODO: Change this into behaviour that makes more sense.
	feeLimit := lnwire.NewMSatFromSatoshis(btcutil.SatoshiPerBitcoin)

	// Finally, we'll query for a route to the destination that can carry
	// that target amount, we'll only request a single route.
	routes, err := s.cfg.Router.FindRoutes(
		s.cfg.RouterBackend.SelfNode, destNode, amtMsat,
		&routing.RestrictParams{
			FeeLimit: feeLimit,
		}, 1,
	)
	if err != nil {
		return nil, err
	}

	if len(routes) == 0 {
		return nil, fmt.Errorf("unable to find route to dest: %v", err)
	}

	return &RouteFeeResponse{
		RoutingFeeMsat: int64(routes[0].TotalFees),
		TimeLockDelay:  int64(routes[0].TotalTimeLock),
	}, nil
}

func (s *Server) LookupPayment(hash *lnrpc.PaymentHash,
	stream Router_LookupPaymentServer) error {

	paymentHash, err := lntypes.MakeHash(hash.RHash)
	if err != nil {
		return err
	}

	log.Debugf("LookupPayment %v", paymentHash)
	s.lock.Lock()
	c, ok := s.pendingPayments[paymentHash]
	s.lock.Unlock()
	if !ok {
		// If payment is not yet initiated, poll for its start.
		log.Debugf("LookupPayment %v unknown", paymentHash)
		err := stream.Send(&PaymentStatus{
			State: PaymentState_UNKNOWN,
		})
		if err != nil {
			return err
		}

		log.Debugf("LookupPayment %v waiting for initiation", paymentHash)
		initiated := make(chan struct{})
		go func() {
			var ok bool
			for !ok {
				time.Sleep(time.Second)
				s.lock.Lock()
				c, ok = s.pendingPayments[paymentHash]
				s.lock.Unlock()
			}
			close(initiated)
		}()
		select {
		case <-initiated:
		case <-stream.Context().Done():
			log.Debugf("LookupPayment %v canceled", paymentHash)
			return stream.Context().Err()
		}
	}

	// If outcome is already available, fake a channel with that outcome.
	s.lock.Lock()
	result, ok := s.outcomes[paymentHash]
	s.lock.Unlock()
	if ok {
		c = make(chan paymentResult, 1)
		c <- result
	} else {
		log.Debugf("LookupPayment %v in flight", paymentHash)
		err = stream.Send(&PaymentStatus{
			State: PaymentState_IN_FLIGHT,
		})
		if err != nil {
			return err
		}
	}

	select {
	case result := <-c:
		s.lock.Lock()
		s.outcomes[paymentHash] = result
		s.lock.Unlock()

		if result.err == nil {
			log.Debugf("LookupPayment %v success", paymentHash)
			route := s.cfg.RouterBackend.MarshallRoute(result.route)
			return stream.Send(&PaymentStatus{
				State:    PaymentState_SUCCEEDED,
				Preimage: result.preimage[:],
				Route:    route,
			})
		}

		// TODO: Interpret result.err and return proper final state.
		log.Debugf("LookupPayment %v failed", paymentHash)
		return stream.Send(&PaymentStatus{
			State: PaymentState_FAILED_NO_ROUTE,
		})
	case <-stream.Context().Done():
		log.Debugf("LookupPayment canceled", paymentHash)
		return stream.Context().Err()
	}
}
