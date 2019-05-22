// +build routerrpc

package routerrpc

import (
	"time"

	"github.com/lightningnetwork/lnd/lnwire"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcutil"
	"github.com/lightningnetwork/lnd/macaroons"
	"github.com/lightningnetwork/lnd/routing"
)

// Config is the main configuration file for the router RPC server. It contains
// all the items required for the router RPC server to carry out its duties.
// The fields with struct tags are meant to be parsed as normal configuration
// options, while if able to be populated, the latter fields MUST also be
// specified.
type Config struct {
	// RouterMacPath is the path for the router macaroon. If unspecified
	// then we assume that the macaroon will be found under the network
	// directory, named DefaultRouterMacFilename.
	RouterMacPath string `long:"routermacaroonpath" description:"Path to the router macaroon"`

	// MinProbability is the minimum required route probability to attempt
	// the payment.
	MinProbability float64 `long:"minprobability" description:"Minimum required route probability to attempt the payment"`

	// PenaltyHalfLife defines after how much time a penalized node or
	// channel is back at 50% probability.
	PenaltyHalfLife time.Duration `long:"penaltyhalflife" description:"Defines the duration after which a penalized node or channel is back at 50% probability"`

	// AttemptCost is the virtual cost in path finding weight units of
	// executing a payment attempt that fails. It is used to trade off
	// potentially better routes against their probability of succeeding.
	AttemptCost int64 `long:"attemptcost" description:"The (virtual) cost in sats of a failed payment attempt"`

	// NetworkDir is the main network directory wherein the router rpc
	// server will find the macaroon named DefaultRouterMacFilename.
	NetworkDir string

	// ActiveNetParams are the network parameters of the primary network
	// that the route is operating on. This is necessary so we can ensure
	// that we receive payment requests that send to destinations on our
	// network.
	ActiveNetParams *chaincfg.Params

	// MacService is the main macaroon service that we'll use to handle
	// authentication for the Router rpc server.
	MacService *macaroons.Service

	// Router is the main channel router instance that backs this RPC
	// server.
	//
	// TODO(roasbeef): make into pkg lvl interface?
	//
	// TODO(roasbeef): assumes router handles saving payment state
	Router *routing.ChannelRouter

	// RouterBackend contains shared logic between this sub server and the
	// main rpc server.
	RouterBackend *RouterBackend
}

// DefaultConfig defines the config defaults.
func DefaultConfig() *Config {
	return &Config{
		MinProbability:  routing.DefaultMinProbability,
		PenaltyHalfLife: routing.DefaultPenaltyHalfLife,
		AttemptCost: int64(
			routing.DefaultPaymentAttemptPenalty.ToSatoshis(),
		),
	}
}

// GetMissionControlConfig returns the mission control config based on this sub
// server config.
func GetMissionControlConfig(cfg *Config) *routing.MissionControlConfig {
	return &routing.MissionControlConfig{
		MinProbability: cfg.MinProbability,
		PaymentAttemptPenalty: lnwire.NewMSatFromSatoshis(
			btcutil.Amount(cfg.AttemptCost),
		),
		PenaltyHalfLife: cfg.PenaltyHalfLife,
	}
}
