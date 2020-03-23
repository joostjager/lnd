package routing

import (
	"errors"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
)

// BlockPadding is used to increment the finalCltvDelta value for the last hop
// to prevent an HTLC being failed if some blocks are mined while it's in-flight.
const BlockPadding uint16 = 3

var (
	// errEmptyPaySession is returned when the epty payment session is
	// queried for a route.
	errEmptyPaySession = errors.New("empty payment session")
)

// PaymentSession is used during SendPayment attempts to provide routes to
// attempt. It also defines methods to give the PaymentSession additional
// information learned during the previous attempts.
type PaymentSession interface {
	// RequestRoute returns the next route to attempt for routing the
	// specified HTLC payment to the target node.
	RequestRoute(amt, feeLimit lnwire.MilliSatoshi,
		height uint32) (*route.Route, error)
}

// paymentSession is used during an HTLC routings session to prune the local
// chain view in response to failures, and also report those failures back to
// MissionControl. The snapshot copied for this session will only ever grow,
// and will now be pruned after a decay like the main view within mission
// control. We do this as we want to avoid the case where we continually try a
// bad edge or route multiple times in a session. This can lead to an infinite
// loop if payment attempts take long enough. An additional set of edges can
// also be provided to assist in reaching the payment's destination.
type paymentSession struct {
	additionalEdges map[route.Vertex][]*channeldb.ChannelEdgePolicy

	getBandwidthHints func() (map[uint64]lnwire.MilliSatoshi, error)

	payment *LightningPayment

	empty bool

	pathFinder pathFinder

	getRoutingGraph func() (routingGraph, func(), error)

	// pathFindingConfig defines global parameters that control the
	// trade-off in path finding between fees and probabiity.
	pathFindingConfig PathFindingConfig

	missionControl MissionController
}

// RequestRoute returns a route which is likely to be capable for successfully
// routing the specified HTLC payment to the target node. Initially the first
// set of paths returned from this method may encounter routing failure along
// the way, however as more payments are sent, mission control will start to
// build an up to date view of the network itself. With each payment a new area
// will be explored, which feeds into the recommendations made for routing.
//
// NOTE: This function is safe for concurrent access.
// NOTE: Part of the PaymentSession interface.
func (p *paymentSession) RequestRoute(amt, feeLimit lnwire.MilliSatoshi,
	height uint32) (*route.Route, error) {

	if p.empty {
		return nil, errEmptyPaySession
	}

	// Add BlockPadding to the finalCltvDelta so that the receiving node
	// does not reject the HTLC if some blocks are mined while it's in-flight.
	finalCltvDelta := p.payment.FinalCLTVDelta
	finalCltvDelta += BlockPadding

	// We need to subtract the final delta before passing it into path
	// finding. The optimal path is independent of the final cltv delta and
	// the path finding algorithm is unaware of this value.
	cltvLimit := p.payment.CltvLimit - uint32(finalCltvDelta)

	// TODO(roasbeef): sync logic amongst dist sys

	// Taking into account this prune view, we'll attempt to locate a path
	// to our destination, respecting the recommendations from
	// MissionControl.
	restrictions := &RestrictParams{
		ProbabilitySource: p.missionControl.GetProbability,
		FeeLimit:          feeLimit,
		OutgoingChannelID: p.payment.OutgoingChannelID,
		LastHop:           p.payment.LastHop,
		CltvLimit:         cltvLimit,
		DestCustomRecords: p.payment.DestCustomRecords,
		DestFeatures:      p.payment.DestFeatures,
		PaymentAddr:       p.payment.PaymentAddr,
	}

	// We'll also obtain a set of bandwidthHints from the lower layer for
	// each of our outbound channels. This will allow the path finding to
	// skip any links that aren't active or just don't have enough bandwidth
	// to carry the payment. New bandwidth hints are queried for every new
	// path finding attempt, because concurrent payments may change
	// balances.
	bandwidthHints, err := p.getBandwidthHints()
	if err != nil {
		return nil, err
	}

	finalHtlcExpiry := int32(height) + int32(finalCltvDelta)

	routingGraph, cleanup, err := p.getRoutingGraph()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	sourceVertex := routingGraph.sourceNode()

	path, err := p.pathFinder(
		p.additionalEdges,
		bandwidthHints,
		routingGraph,
		restrictions, &p.pathFindingConfig,
		sourceVertex, p.payment.Target,
		amt, finalHtlcExpiry,
	)
	if err != nil {
		return nil, err
	}

	// TODO: move out?

	// With the next candidate path found, we'll attempt to turn this into
	// a route by applying the time-lock and fee requirements.
	route, err := newRoute(
		sourceVertex, path, height,
		finalHopParams{
			amt:         amt,
			cltvDelta:   finalCltvDelta,
			records:     p.payment.DestCustomRecords,
			paymentAddr: p.payment.PaymentAddr,
		},
	)
	if err != nil {
		// TODO(roasbeef): return which edge/vertex didn't work
		// out
		return nil, err
	}

	return route, err
}
