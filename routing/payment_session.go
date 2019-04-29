package routing

import (
	"fmt"
	"time"

	"github.com/lightningnetwork/lnd/lntypes"

	"github.com/davecgh/go-spew/spew"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
)

// paymentSession is used during an HTLC routings session to prune the local
// chain view in response to failures, and also report those failures back to
// missionControl. The snapshot copied for this session will only ever grow,
// and will now be pruned after a decay like the main view within mission
// control. We do this as we want to avoid the case where we continually try a
// bad edge or route multiple times in a session. This can lead to an infinite
// loop if payment attempts take long enough. An additional set of edges can
// also be provided to assist in reaching the payment's destination.
type paymentSession struct {
	pruneViewSnapshot graphPruneView

	additionalEdges map[route.Vertex][]*channeldb.ChannelEdgePolicy

	bandwidthHints map[uint64]lnwire.MilliSatoshi

	// errFailedFeeChans is a map of the short channel IDs that were the
	// source of policy related routing failures during this payment attempt.
	// We'll use this map to prune out channels when the first error may not
	// require pruning, but any subsequent ones do.
	errFailedPolicyChans map[EdgeLocator]struct{}

	mc *missionControl

	haveRoutes     bool
	preBuiltRoutes []*route.Route

	pathFinder pathFinder

	payment *LightningPayment

	// Attempt is the latest recorded payment attempt.
	//
	// NOTE: can be nil.
	currentAttempt *channeldb.AttemptInfo

	cfg *Config

	timeoutChan       <-chan time.Time
	payAttemptTimeout time.Duration

	// We'll continue until either our payment succeeds, or we encounter a
	// critical error during path finding.
	lastError error

	quit chan struct{}
}

// ReportVertexFailure adds a vertex to the graph prune view after a client
// reports a routing failure localized to the vertex. The time the vertex was
// added is noted, as it'll be pruned from the shared view after a period of
// vertexDecay. However, the vertex will remain pruned for the *local* session.
// This ensures we don't retry this vertex during the payment attempt.
func (p *paymentSession) ReportVertexFailure(v route.Vertex) {
	log.Debugf("Reporting vertex %v failure to Mission Control", v)

	// First, we'll add the failed vertex to our local prune view snapshot.
	p.pruneViewSnapshot.vertexes[v] = struct{}{}

	// With the vertex added, we'll now report back to the global prune
	// view, with this new piece of information so it can be utilized for
	// new payment sessions.
	p.mc.Lock()
	p.mc.failedVertexes[v] = time.Now()
	p.mc.Unlock()
}

// ReportChannelFailure adds a channel to the graph prune view. The time the
// channel was added is noted, as it'll be pruned from the global view after a
// period of edgeDecay. However, the edge will remain pruned for the duration
// of the *local* session. This ensures that we don't flap by continually
// retrying an edge after its pruning has expired.
//
// TODO(roasbeef): also add value attempted to send and capacity of channel
func (p *paymentSession) ReportEdgeFailure(e *EdgeLocator) {
	log.Debugf("Reporting edge %v failure to Mission Control", e)

	// First, we'll add the failed edge to our local prune view snapshot.
	p.pruneViewSnapshot.edges[*e] = struct{}{}

	// With the edge added, we'll now report back to the global prune view,
	// with this new piece of information so it can be utilized for new
	// payment sessions.
	p.mc.Lock()
	p.mc.failedEdges[*e] = time.Now()
	p.mc.Unlock()
}

// ReportChannelPolicyFailure handles a failure message that relates to a
// channel policy. For these types of failures, the policy is updated and we
// want to keep it included during path finding. This function does mark the
// edge as 'policy failed once'. The next time it fails, the whole node will be
// pruned. This is to prevent nodes from keeping us busy by continuously sending
// new channel updates.
func (p *paymentSession) ReportEdgePolicyFailure(
	errSource route.Vertex, failedEdge *EdgeLocator) {

	// Check to see if we've already reported a policy related failure for
	// this channel. If so, then we'll prune out the vertex.
	_, ok := p.errFailedPolicyChans[*failedEdge]
	if ok {
		// TODO(joostjager): is this aggresive pruning still necessary?
		// Just pruning edges may also work unless there is a huge
		// number of failing channels from that node?
		p.ReportVertexFailure(errSource)

		return
	}

	// Finally, we'll record a policy failure from this node and move on.
	p.errFailedPolicyChans[*failedEdge] = struct{}{}
}

// RequestRoute returns a route which is likely to be capable for successfully
// routing the specified HTLC payment to the target node. Initially the first
// set of paths returned from this method may encounter routing failure along
// the way, however as more payments are sent, mission control will start to
// build an up to date view of the network itself. With each payment a new area
// will be explored, which feeds into the recommendations made for routing.
//
// NOTE: This function is safe for concurrent access.
func (p *paymentSession) RequestRoute(payment *LightningPayment,
	height, finalCltvDelta int32) (*route.Route, error) {

	switch {
	// If we have a set of pre-built routes, then we'll just pop off the
	// next route from the queue, and use it directly.
	case p.haveRoutes && len(p.preBuiltRoutes) > 0:
		nextRoute := p.preBuiltRoutes[0]
		p.preBuiltRoutes[0] = nil // Set to nil to avoid GC leak.
		p.preBuiltRoutes = p.preBuiltRoutes[1:]

		return nextRoute, nil

	// If we were instantiated with a set of pre-built routes, and we've
	// run out, then we'll return a terminal error.
	case p.haveRoutes && len(p.preBuiltRoutes) == 0:
		return nil, fmt.Errorf("pre-built routes exhausted")
	}

	// Otherwise we actually need to perform path finding, so we'll obtain
	// our current prune view snapshot. This view will only ever grow
	// during the duration of this payment session, never shrinking.
	pruneView := p.pruneViewSnapshot

	log.Debugf("Mission Control session using prune view of %v "+
		"edges, %v vertexes", len(pruneView.edges),
		len(pruneView.vertexes))

	// If a route cltv limit was specified, we need to subtract the final
	// delta before passing it into path finding. The optimal path is
	// independent of the final cltv delta and the path finding algorithm is
	// unaware of this value.
	var cltvLimit *int32
	if payment.CltvLimit != nil {
		limit := *payment.CltvLimit - finalCltvDelta
		cltvLimit = &limit
	}

	// TODO(roasbeef): sync logic amongst dist sys

	// Taking into account this prune view, we'll attempt to locate a path
	// to our destination, respecting the recommendations from
	// missionControl.
	path, err := p.pathFinder(
		&graphParams{
			graph:           p.mc.graph,
			additionalEdges: p.additionalEdges,
			bandwidthHints:  p.bandwidthHints,
		},
		&RestrictParams{
			IgnoredNodes:      pruneView.vertexes,
			IgnoredEdges:      pruneView.edges,
			FeeLimit:          payment.FeeLimit,
			OutgoingChannelID: payment.OutgoingChannelID,
			CltvLimit:         cltvLimit,
		},
		p.mc.selfNode.PubKeyBytes, payment.Target,
		payment.Amount,
	)
	if err != nil {
		return nil, err
	}

	// With the next candidate path found, we'll attempt to turn this into
	// a route by applying the time-lock and fee requirements.
	sourceVertex := route.Vertex(p.mc.selfNode.PubKeyBytes)
	route, err := newRoute(
		payment.Amount, sourceVertex, path, height, finalCltvDelta,
	)
	if err != nil {
		// TODO(roasbeef): return which edge/vertex didn't work
		// out
		return nil, err
	}

	return route, err
}

// NewAttempt records a new payment attempt with the underlying ControlTower.
func (p *paymentSession) NewAttempt(a *channeldb.AttemptInfo) error {
	/*var c *channeldb.CreationInfo

	// If this payment is not in flight, we record its CreationInfo with
	// the ControlTower.
	if !p.inFlight {
		c = &p.Info
	}

	err := p.mc.control.RegisterAttempt(p.Info.PaymentHash, c, a)
	if err != nil {
		return err
	}

	// Now keep the latest attempt, and note that it is considered
	// InFlight, such that successive calls to NewAttempt won't record the
	// CreationInfo.
	p.Attempt = a
	p.inFlight = true
	return nil*/

	// TODO: ENABLE
	return nil

}

// Success marks the payment succeeded with the given preimage.
func (p *paymentSession) Success(preimage lntypes.Preimage) error {
	/*	var c *channeldb.CreationInfo

		// If this payment is not in flight, we record its CreationInfo with
		// the ControlTower.
		if !p.inFlight {
			c = &p.Info
		}
		return p.mc.control.Success(p.Info.PaymentHash, c, preimage)
	*/
	// TODO: ENABLE
	return nil

}

// Fail records the payment as failed.
func (p *paymentSession) Fail() error {
	/*var c *channeldb.CreationInfo

	// If this payment is not in flight, we record its CreationInfo with
	// the ControlTower.
	if !p.inFlight {
		c = &p.Info
	}

	return p.mc.control.Fail(p.Info.PaymentHash, c)*/

	// TODO: ENABLE
	return nil
}

// sendPayment attempts to send a payment as described within the passed
// LightningPayment. This function is blocking and will return either: when the
// payment is successful, or all candidates routes have been attempted and
// resulted in a failed payment. If the payment succeeds, then a non-nil Route
// will be returned which describes the path the successful payment traversed
// within the network to reach the destination. Additionally, the payment
// preimage will also be returned.
//
// This method relies on the ControlTower's internal payment state machine to
// carry out its execution. After restarts it is safe, and assumed, that the
// router will call this method for every payment still in-flight according to
// the ControlTower.
func (p *paymentSession) sendPayment() (
	[32]byte, *route.Route, error) {

	log.Tracef("Dispatching route for lightning payment: %v",
		newLogClosure(func() string {
			for _, routeHint := range p.payment.RouteHints {
				for _, hopHint := range routeHint {
					hopHint.NodeID.Curve = nil
				}
			}
			return spew.Sdump(p.payment)
		}),
	)

	if p.payment.PayAttemptTimeout == time.Duration(0) {
		p.payAttemptTimeout = defaultPayAttemptTimeout
	} else {
		p.payAttemptTimeout = p.payment.PayAttemptTimeout
	}

	p.timeoutChan = time.After(p.payAttemptTimeout)

	paymentHash := p.payment.PaymentHash

	switch {

	// If this payment had no existing payment ID, we make a new attempt.
	case p.currentAttempt == nil:
		if err := p.sendNewAttempt(); err != nil {
			return [32]byte{}, nil, err
		}

	// Otherwise we'll check if there's a result available for the already
	// existing payment ID.
	default:
		log.Infof("Got existing attempt for pid=%v", p.currentAttempt.PaymentID)
	}

	for {
		// We'll ask the switch whether this is a known paymentID.
		result, err := p.cfg.GetPaymentResult(p.currentAttempt.PaymentID)
		switch {

		// If this payment ID is unknown to the Switch, it means it was
		// never checkpointed and forwarded by the switch before a
		// restart. In this case we can safely send a new payment
		// attempt, and wait for its result to be available.
		case err == htlcswitch.ErrPaymentIDNotFound:
			log.Debugf("Payment ID %v for hash %x not found in "+
				"the Switch, retrying.", p.currentAttempt.PaymentID,
				paymentHash)

			if err := p.sendNewAttempt(); err != nil {
				return [32]byte{}, nil, err
			}

			continue

		// A critical, unexpected error was encountered.
		case err != nil:
			return [32]byte{}, nil, err
		}

		// In case of a payment failure, we use the error to decidee
		// whether we should retry.
		if result.Error != nil {
			log.Errorf("Attempt (pid=%v) to send "+
				"payment %x failed: %v", p.currentAttempt.PaymentID,
				paymentHash, result.Error)

			finalOutcome := r.processSendError(
				p, &p.currentAttempt.Route, result.Error,
			)
			if finalOutcome {
				log.Errorf("Payment %x failed with "+
					"final outcome: %v",
					paymentHash, result.Error)

				// Mark the payment failed.
				err := p.Fail()
				if err != nil {
					return [32]byte{}, nil, err
				}

				// Terminal state, return the error we
				// encountered.
				return [32]byte{}, nil, result.Error
			}

			p.lastError = result.Error

			// We make another payment attempt.
			if err := p.sendNewAttempt(); err != nil {
				return [32]byte{}, nil, err
			}

			continue
		}

		// We successfully got a payment result back from the switch.
		log.Debugf("Payment %x succeeded with pid=%v",
			paymentHash, p.currentAttempt.PaymentID)

		// In case of success we atomically store the db payment and
		// move the payment to the success state.
		err = p.Success(result.Preimage)
		if err != nil {
			log.Errorf("Unable to succeed payment "+
				"attempt: %v", err)
			return [32]byte{}, nil, err
		}

		// Terminal state, return the preimage and the route
		// taken.
		return result.Preimage, &p.currentAttempt.Route, nil
	}
}

// sendNewAttempt is a helper method that creates and sends a new
// payment to the switch. We use this when no payment has been sent to
// the switch already, or the previous attempt failed.
func (p *paymentSession) sendNewAttempt() error {
	// Before we attempt this next payment, we'll check to see if
	// either we've gone past the payment attempt timeout, or the
	// router is exiting. In either case, we'll stop this payment
	// attempt short.
	select {
	case <-p.timeoutChan:
		// Mark the payment as failed.
		err := p.Fail()
		if err != nil {
			return err
		}

		errStr := fmt.Sprintf("payment attempt not completed "+
			"before timeout of %v", p.payAttemptTimeout)

		// Terminal state, return.
		return newErr(ErrPaymentAttemptTimeout, errStr)

	case <-p.quit:
		// The payment will be resumed from the current state
		// after restart.
		return ErrRouterShuttingDown

	default:
		// Fall through if we haven't hit our time limit, or
		// are expiring.
	}

	// We'll also fetch the current block height so we can properly
	// calculate the required HTLC time locks within the route.
	_, currentHeight, err := p.cfg.Chain.GetBestBlock()
	if err != nil {
		return err
	}

	// Create a new payment attempt from the given payment session.
	route, err := p.RequestRoute(
		p.payment, currentHeight, p.payment.FinalCLTVDelta,
	)
	if err != nil {
		// If we're unable to successfully make a payment using
		// any of the routes we've found, then mark the payment
		// as permanently failed.
		saveErr := p.Fail()
		if saveErr != nil {
			return saveErr
		}

		// If there was an error already recorded for this
		// payment, we'll return that.
		if p.lastError != nil {
			return fmt.Errorf("unable to route payment "+
				"to destination: %v", p.lastError)
		}

		// Terminal state, return.
		return err
	}

	// We generate a new, unique payment ID that we will use for
	// this HTLC.
	paymentID, err := p.cfg.NextPaymentID()
	if err != nil {
		return err
	}

	attempt := &channeldb.AttemptInfo{
		PaymentID: paymentID,
		Route:     *route,
	}

	// Before sending this HTLC to the switch, we checkpoint the
	// fresh paymentID and route to the DB. This lets us know on
	// startup the ID of the payment that we attempted to send,
	// such that we can query the Switch for its whereabouts. The
	// route is needed to handle the result when it eventually
	// comes back.
	err = p.NewAttempt(attempt)
	if err != nil {
		return err
	}

	log.Tracef("Attempting to send payment %x (pid=%v), "+
		"using route: %v", p.payment.PaymentHash,
		p.currentAttempt.PaymentID,
		newLogClosure(func() string {
			return spew.Sdump(route)
		}),
	)

	// Send it to the Switch. When this method returns we assume
	// the Switch successfully has persisted the payment attempt,
	// such that we can resume waiting for the result after a
	// restart.
	err = p.cfg.SendToSwitch(route, p.payment.PaymentHash,
		p.currentAttempt.PaymentID)
	if err != nil {
		log.Errorf("Failed sending payment to switch: %v",
			err)
		return err
	}

	log.Debugf("Payment %x (pid=%v) successfully sent to switch",
		p.payment.PaymentHash, p.currentAttempt.PaymentID)

	return nil
}
