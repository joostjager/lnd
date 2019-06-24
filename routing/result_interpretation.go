package routing

import (
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
)

type ChannelResultType byte

const (
	ChannelResultSuccess ChannelResultType = iota

	ChannelResultFail

	ChannelResultFailBalance

	ChannelResultFailRelay
)

type pairResult struct {
	// amount is the amount that was sent across the channel.
	amount lnwire.MilliSatoshi

	// resultType is the type of channel result that collected from the
	// payment attempt.
	resultType ChannelResultType

	// directionReverse indicates the direction the htlc went. If false it
	// went from a to b.
	directionReverse bool
}

type interpretedResult struct {
	nodeFailures  map[route.Vertex]struct{}
	pairResults   map[NodePair]pairResult
	final         bool
	policyFailure *DirectedNodePair
}

func newInterpretedResult(initiate *paymentInitiate,
	result *paymentResult) *interpretedResult {

	i := &interpretedResult{
		nodeFailures: make(map[route.Vertex]struct{}),
		pairResults:  make(map[NodePair]pairResult),
	}

	if result.success {
		i.processSuccess(
			initiate.route,
		)
	} else {
		i.processFail(
			initiate.route, result.errorSourceIndex,
			result.failure,
		)
	}
	return i
}

func (i *interpretedResult) processSuccess(route *route.Route) {

	for hop := range route.Hops {
		i.reportChannelResult(
			route, hop, ChannelResultSuccess,
		)
	}

	i.final = true
}

func (i *interpretedResult) processFail(
	route *route.Route, errSourceIdx *int,
	failure lnwire.FailureMessage) {

	if errSourceIdx == nil {
		i.processPaymentOutcomeUnknown(route)
		return
	}

	switch *errSourceIdx {

	// We don't keep a reputation for ourselves, as information about
	// channels should be available directly in links. Just retry with local
	// info that should now be updated.
	case 0:
		log.Warnf("Routing error for local channel %v occurred",
			route.Hops[0].ChannelID)

	// An error from the final hop was received.
	case len(route.Hops):
		i.processPaymentOutcomeFinal(
			route, failure,
		)

	// An intermediate hop failed. Interpret the outcome, update reputation
	// and try again.
	default:
		i.processPaymentOutcomeIntermediate(
			route, *errSourceIdx, failure,
		)
	}
}

// processPaymentOutcomeFinal handles failures sent by the final hop.
func (i *interpretedResult) processPaymentOutcomeFinal(
	route *route.Route, failure lnwire.FailureMessage) {

	n := len(route.Hops)

	reportNode := func() {
		i.nodeFailures[route.Hops[n-1].PubKeyBytes] = struct{}{}
	}

	// If a failure from the final node is received, we will fail the
	// payment in almost all cases. Only when the penultimate node sends an
	// incorrect htlc, we want to retry via another route. Invalid onion
	// failures are not expected, because the final node wouldn't be able to
	// encrypt that failure.
	switch failure.(type) {

	// Expiry or amount of the HTLC doesn't match the onion, try another
	// route.
	case *lnwire.FailFinalIncorrectCltvExpiry,
		*lnwire.FailFinalIncorrectHtlcAmount:

		// We trust ourselves. If this is a direct payment, we penalize
		// the final node and fail the payment.
		if n == 1 {
			reportNode()
			i.final = true
			return
		}

		// Otherwise penalize the last channel of the route and retry.
		i.reportChannelResult(
			route, n-1, ChannelResultFail,
		)

	// We are using wrong payment hash or amount, fail the payment.
	case *lnwire.FailIncorrectPaymentAmount,
		*lnwire.FailUnknownPaymentHash:

		i.final = true

	// The HTLC that was extended to the final hop expires too soon. Fail
	// the payment, because we may be using the wrong final cltv delta.
	case *lnwire.FailFinalExpiryTooSoon:
		// TODO(roasbeef): can happen to to race condition, try again
		// with recent block height

		// TODO(joostjager): can also happen because a node delayed
		// deliberately. What to penalize?
		i.final = true

	default:
		// All other errors are considered terminal if coming from the
		// final hop. They indicate that something is wrong at the
		// recipient, so we do apply a penalty.

		reportNode()
		i.final = true
	}
}

// processPaymentOutcomeIntermediate handles failures sent by an intermediate
// hop.
func (i *interpretedResult) processPaymentOutcomeIntermediate(
	route *route.Route, errorSourceIdx int,
	failure lnwire.FailureMessage) {

	reportNode := func() {
		i.nodeFailures[route.Hops[errorSourceIdx-1].PubKeyBytes] =
			struct{}{}
	}

	reportOutgoingWithAmt := func() {
		i.reportChannelResult(
			route, errorSourceIdx, ChannelResultFailBalance,
		)
	}

	reportOutgoing := func() {
		i.reportChannelResult(
			route, errorSourceIdx, ChannelResultFail,
		)
	}

	reportIncoming := func() {
		// We trust ourselves. If the error comes from the first hop, we
		// can penalize the whole node. In that case there is no
		// uncertainty as to which node to blame.
		if errorSourceIdx == 1 {
			reportNode()
			return
		}

		// Otherwise report the incoming channel.
		i.reportChannelResult(
			route, errorSourceIdx-1, ChannelResultFail,
		)
	}

	switch failure.(type) {

	// If a hop reports onion payload corruption or an invalid version, we
	// will report the outgoing channel of that node. It may be either their
	// or the next node's fault.
	case *lnwire.FailInvalidOnionVersion,
		*lnwire.FailInvalidOnionHmac,
		*lnwire.FailInvalidOnionKey:

		reportOutgoing()

	// If the next hop in the route wasn't known or offline, we'll only
	// penalize the channel which we attempted to route over. This is
	// conservative, and it can handle faulty channels between nodes
	// properly. Additionally, this guards against routing nodes returning
	// errors in order to attempt to black list another node.
	case *lnwire.FailUnknownNextPeer:
		reportOutgoing()

	// If we get a permanent channel or node failure, then
	// we'll prune the channel in both directions and
	// continue with the rest of the routes.
	case *lnwire.FailPermanentChannelFailure:
		reportOutgoing()

	// If we get a failure due to violating the channel policy, we request a
	// second chance because our graph may be out of date. An attached
	// channel update should have been applied by now. If the second chance
	// is granted, we try again. Otherwise either the error source or its
	// predecessor sending an incorrect htlc is to blame.
	case *lnwire.FailAmountBelowMinimum,
		*lnwire.FailFeeInsufficient,
		*lnwire.FailIncorrectCltvExpiry,
		*lnwire.FailChannelDisabled:

		i.policyFailure = &DirectedNodePair{
			From: route.Hops[errorSourceIdx-1].PubKeyBytes,
			To:   route.Hops[errorSourceIdx].PubKeyBytes,
		}

		// If no second chance, report incoming channel.
		reportIncoming()

	// If the outgoing channel doesn't have enough capacity, we penalize.
	// But we penalize only in a single direction and only for amounts
	// greater than the attempted amount.
	case *lnwire.FailTemporaryChannelFailure:
		reportOutgoingWithAmt()

	// TODO(joostjager): Retry when FailExpiryTooSoon is received. Could
	// also be any node that delayed.

	default:
		// In all other cases, we report the whole node. These are all
		// failures that should not happen.
		reportNode()
	}
}

// processPaymentOutcomeUnknown processes a payment outcome for which no failure
// message or source is available.
func (i *interpretedResult) processPaymentOutcomeUnknown(route *route.Route) {

	// Penalize all channels in the route to make sure the responsible node
	// is at least hit too.
	for hop := range route.Hops {
		i.reportChannelResult(
			route, hop, ChannelResultFail,
		)
	}
}

// reportChannelFailure reports a bidirectional failure of a channel.
func (i *interpretedResult) reportChannelResult(
	rt *route.Route, channelIdx int,
	resultType ChannelResultType) {

	channelID := rt.Hops[channelIdx].ChannelID
	log.Debugf("Reporting channel failure to Mission Control: "+
		"chan=%v", channelID)

	nodeB := rt.Hops[channelIdx].PubKeyBytes
	var (
		nodeA route.Vertex
		amt   lnwire.MilliSatoshi
	)

	if channelIdx == 0 {
		nodeA = rt.SourcePubKey
		amt = rt.TotalAmount
	} else {
		nodeA = rt.Hops[channelIdx-1].PubKeyBytes
		amt = rt.Hops[channelIdx-1].AmtToForward
	}

	pair, reverse := newNodePair(nodeA, nodeB)

	i.pairResults[pair] = pairResult{
		amount:           amt,
		directionReverse: reverse,
		resultType:       resultType,
	}
}
