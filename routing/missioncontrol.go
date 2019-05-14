package routing

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
)

const (
	// DefaultPenaltyHalfLife is the default half-life duration. The
	// half-life duration defines after how much time a penalized node or
	// channel is back at 50% probability.
	DefaultPenaltyHalfLife = time.Hour

	// minSecondChanceInterval is the minimum time required between
	// second-chance failures.
	minSecondChanceInterval = time.Minute
)

// MissionControl contains state which summarizes the past attempts of HTLC
// routing by external callers when sending payments throughout the network. It
// acts as a shared memory during routing attempts with the goal to optimize the
// payment attempt success rate.
//
// Failed payment attempts are reported to mission control. These reports are
// used to track the time of the last node or channel level failure. The time
// since the last failure is used to estimate a success probability that is fed
// into the path finding process for subsequent payment attempts.
type MissionControl struct {
	history map[route.Vertex]*nodeHistory

	secondChanceChannels map[uint64]struct{}

	paymentAttempts map[uint64]*paymentInitiate

	// now is expected to return the current time. It is supplied as an
	// external function to enable deterministic unit tests.
	now func() time.Time

	cfg *MissionControlConfig

	store *bboltMissionControlStore

	sync.Mutex

	// TODO(roasbeef): further counters, if vertex continually unavailable,
	// add to another generation

	// TODO(roasbeef): also add favorable metrics for nodes
}

// MissionControlConfig defines parameters that control mission control
// behaviour.
type MissionControlConfig struct {
	// PenaltyHalfLife defines after how much time a penalized node or
	// channel is back at 50% probability.
	PenaltyHalfLife time.Duration

	// AprioriHopProbability is the assumed success probability of a hop in
	// a route when no other information is available.
	AprioriHopProbability float64
}

// nodeHistory contains a summary of payment attempt outcomes involving a
// particular node.
type nodeHistory struct {
	// lastFail is the last time a node level failure occurred, if any.
	lastFail *time.Time

	// channelLastFail tracks history per channel, if available for that
	// channel.
	channelLastFail map[uint64]*channelHistory

	channelLastSecondChance map[uint64]time.Time
}

// channelHistory contains a summary of payment attempt outcomes involving a
// particular channel.
type channelHistory struct {
	// lastFail is the last time a channel level failure occurred.
	lastFail time.Time

	// minPenalizeAmt is the minimum amount for which to take this failure
	// into account.
	minPenalizeAmt lnwire.MilliSatoshi

	// lastSecondChance is the last time this channel got a second chance.
	lastSecondChance time.Time
}

// MissionControlSnapshot contains a snapshot of the current state of mission
// control.
type MissionControlSnapshot struct {
	// Nodes contains the per node information of this snapshot.
	Nodes []MissionControlNodeSnapshot
}

// MissionControlNodeSnapshot contains a snapshot of the current node state in
// mission control.
type MissionControlNodeSnapshot struct {
	// Node pubkey.
	Node route.Vertex

	// Lastfail is the time of last failure, if any.
	LastFail *time.Time

	// Channels is a list of channels for which specific information is
	// logged.
	Channels []MissionControlChannelSnapshot

	// OtherChanSuccessProb is the success probability for channels not in
	// the Channels slice.
	OtherChanSuccessProb float64
}

// MissionControlChannelSnapshot contains a snapshot of the current channel
// state in mission control.
type MissionControlChannelSnapshot struct {
	// ChannelID is the short channel id of the snapshot.
	ChannelID uint64

	// LastFail is the time of last failure.
	LastFail time.Time

	// MinPenalizeAmt is the minimum amount for which the channel will be
	// penalized.
	MinPenalizeAmt lnwire.MilliSatoshi

	// SuccessProb is the success probability estimation for this channel.
	SuccessProb float64
}

// paymentInitiate contains information that is available when a payment attempt
// is initiated.
type paymentInitiate struct {
	id        uint64
	timestamp time.Time
	route     *route.Route
}

// paymentResult is the information that becomes available when a payment
// attempt completes.
type paymentResult struct {
	id               uint64
	timestamp        time.Time
	errorSourceIndex *int
	failure          lnwire.FailureMessage
}

// NewMissionControl returns a new instance of missionControl.
func NewMissionControl(db *bbolt.DB, cfg *MissionControlConfig) (
	*MissionControl, error) {

	log.Debugf("Instantiating mission control with config: "+
		"PenaltyHalfLife=%v, AprioriHopProbability=%v",
		cfg.PenaltyHalfLife, cfg.AprioriHopProbability)

	store, err := newMissionControlStore(db)
	if err != nil {
		return nil, err
	}

	mc := &MissionControl{
		history:              make(map[route.Vertex]*nodeHistory),
		secondChanceChannels: make(map[uint64]struct{}),
		paymentAttempts:      make(map[uint64]*paymentInitiate),
		now:                  time.Now,
		cfg:                  cfg,
		store:                store,
	}

	if err := mc.init(); err != nil {
		return nil, err
	}

	return mc, nil
}

// init initializes mission control with historical data.
func (m *MissionControl) init() error {
	initiates, results, err := m.store.Fetch()
	if err != nil {
		return err
	}

	for _, initiate := range initiates {
		m.processPaymentInitiate(initiate)
	}
	for _, result := range results {
		m.processPaymentResult(result)
	}

	return nil
}


// ResetHistory resets the history of MissionControl returning it to a state as
// if no payment attempts have been made.
func (m *MissionControl) ResetHistory() error {
	m.Lock()
	defer m.Unlock()

	if err := m.store.Clear(); err != nil {
		return err
	}

	m.history = make(map[route.Vertex]*nodeHistory)

	log.Debugf("Mission control history cleared")

	return nil
}

// getEdgeProbability is expected to return the success probability of a payment
// from fromNode along edge.
func (m *MissionControl) getEdgeProbability(fromNode route.Vertex,
	edge EdgeLocator, amt lnwire.MilliSatoshi) float64 {

	m.Lock()
	defer m.Unlock()

	// Get the history for this node. If there is no history available,
	// assume that it's success probability is a constant a priori
	// probability. After the attempt new information becomes available to
	// adjust this probability.
	nodeHistory, ok := m.history[fromNode]
	if !ok {
		return m.cfg.AprioriHopProbability
	}

	return m.getEdgeProbabilityForNode(nodeHistory, edge.ChannelID, amt)
}

// getEdgeProbabilityForNode estimates the probability of successfully
// traversing a channel based on the node history.
func (m *MissionControl) getEdgeProbabilityForNode(nodeHistory *nodeHistory,
	channelID uint64, amt lnwire.MilliSatoshi) float64 {

	// Calculate the last failure of the given edge. A node failure is
	// considered a failure that would have affected every edge. Therefore
	// we insert a node level failure into the history of every channel.
	lastFailure := nodeHistory.lastFail

	// Take into account a minimum penalize amount. For balance errors, a
	// failure may be reported with such a minimum to prevent too aggresive
	// penalization. We only take into account a previous failure if the
	// amount that we currently get the probability for is greater or equal
	// than the minPenalizeAmt of the previous failure.
	channelHistory, ok := nodeHistory.channelLastFail[channelID]
	if ok && channelHistory.minPenalizeAmt <= amt {

		// If there is both a node level failure recorded and a channel
		// level failure is applicable too, we take the most recent of
		// the two.
		if lastFailure == nil ||
			channelHistory.lastFail.After(*lastFailure) {

			lastFailure = &channelHistory.lastFail
		}
	}

	if lastFailure == nil {
		return m.cfg.AprioriHopProbability
	}

	timeSinceLastFailure := m.now().Sub(*lastFailure)

	// Calculate success probability. It is an exponential curve that brings
	// the probability down to zero when a failure occurs. From there it
	// recovers asymptotically back to the a priori probability. The rate at
	// which this happens is controlled by the penaltyHalfLife parameter.
	exp := -timeSinceLastFailure.Hours() / m.cfg.PenaltyHalfLife.Hours()
	probability := m.cfg.AprioriHopProbability * (1 - math.Pow(2, exp))

	return probability
}

// createHistoryIfNotExists returns the history for the given node. If the node
// is yet unknown, it will create an empty history structure.
func (m *MissionControl) createHistoryIfNotExists(vertex route.Vertex) *nodeHistory {
	if node, ok := m.history[vertex]; ok {
		return node
	}

	node := &nodeHistory{
		channelLastFail: make(map[uint64]*channelHistory),
	}
	m.history[vertex] = node

	return node
}

// reportVertexFailure reports a node level failure.
func (m *MissionControl) reportVertexFailure(timestamp time.Time, v route.Vertex) {
	log.Debugf("Reporting vertex %v failure to Mission Control", v)

	m.Lock()
	defer m.Unlock()

	history := m.createHistoryIfNotExists(v)
	history.lastFail = &timestamp
}

func (m *MissionControl) requestSecondChance(timestamp time.Time,
	fromNode route.Vertex, channelID uint64) bool {

	m.Lock()
	defer m.Unlock()

	// Look up previous second chance time.
	history := m.createHistoryIfNotExists(fromNode)

	chanHistory, ok := history.channelLastFail[channelID]
	if !ok {
		chanHistory = &channelHistory{}
		history.channelLastFail[channelID] = chanHistory
	}

	// If the channel hasn't already be given a second chance or its last
	// second chance was long ago, we give it another chance.
	if chanHistory.lastSecondChance.IsZero() ||
		timestamp.Sub(chanHistory.lastSecondChance) >
			minSecondChanceInterval {

		chanHistory.lastSecondChance = timestamp

		log.Debugf("Second chance granted for chan=%v", channelID)

		return true

		// Otherwise penalize the channel, because we don't allow
		// channel updates that are that frequent. This is to prevent
		// nodes from keeping us busy by continuously sending new
		// channel updates.
	}

	log.Debugf("Second chance denied for chan=%v", channelID)

	return false
}

// reportEdgeFailure reports a channel level failure. The validUpdateAttached
// parameter should indicate whether the attached update - if any - was valid.
//
// TODO(roasbeef): also add value attempted to send and capacity of channel
func (m *MissionControl) reportEdgeFailure(timestamp time.Time,
	fromNode route.Vertex, channelID uint64,
	minPenalizeAmt lnwire.MilliSatoshi) {

	log.Debugf("Reporting edge failure to Mission Control: "+
		"node=%v, chan=%v", fromNode, channelID)

	m.Lock()
	defer m.Unlock()

	history := m.createHistoryIfNotExists(fromNode)

	history.channelLastFail[channelID] = &channelHistory{
		lastFail:       timestamp,
		minPenalizeAmt: minPenalizeAmt,
	}
}

// reportChannelFailure reports a bidirectional failure of a channel.
func (m *MissionControl) reportChannelFailure(timestamp time.Time,
	rt *route.Route, channelIdx int) {

	channelID := rt.Hops[channelIdx].ChannelID
	log.Debugf("Reporting channel failure to Mission Control: "+
		"chan=%v", channelID)

	nodeB := rt.Hops[channelIdx].PubKeyBytes
	var nodeA route.Vertex
	if channelIdx == 0 {
		nodeA = rt.SourcePubKey
	} else {
		nodeA = rt.Hops[channelIdx-1].PubKeyBytes
	}

	m.Lock()
	defer m.Unlock()

	nodes := []route.Vertex{nodeA, nodeB}
	for _, n := range nodes {
		history := m.createHistoryIfNotExists(n)

		history.channelLastFail[channelID] = &channelHistory{
			lastFail: timestamp,
		}
	}
}

// GetHistorySnapshot takes a snapshot from the current mission control state
// and actual probability estimates.
func (m *MissionControl) GetHistorySnapshot() *MissionControlSnapshot {
	m.Lock()
	defer m.Unlock()

	log.Debugf("Requesting history snapshot from mission control: "+
		"node_count=%v", len(m.history))

	nodes := make([]MissionControlNodeSnapshot, 0, len(m.history))

	for v, h := range m.history {
		channelSnapshot := make([]MissionControlChannelSnapshot, 0,
			len(h.channelLastFail),
		)

		for id, lastFail := range h.channelLastFail {
			// Show probability assuming amount meets min
			// penalization amount.
			prob := m.getEdgeProbabilityForNode(
				h, id, lastFail.minPenalizeAmt,
			)

			channelSnapshot = append(channelSnapshot,
				MissionControlChannelSnapshot{
					ChannelID:      id,
					LastFail:       lastFail.lastFail,
					MinPenalizeAmt: lastFail.minPenalizeAmt,
					SuccessProb:    prob,
				},
			)
		}

		otherProb := m.getEdgeProbabilityForNode(h, 0, 0)

		nodes = append(nodes,
			MissionControlNodeSnapshot{
				Node:                 v,
				LastFail:             h.lastFail,
				OtherChanSuccessProb: otherProb,
				Channels:             channelSnapshot,
			},
		)
	}

	snapshot := MissionControlSnapshot{
		Nodes: nodes,
	}

	return &snapshot
}

// reportPaymentAttempt reports a payment attempt to mission control.
func (m *MissionControl) reportPaymentInitiate(paymentID uint64,
	rt *route.Route) error {

	timestamp := m.now()

	initiate := paymentInitiate{
		id:        paymentID,
		route:     rt,
		timestamp: timestamp,
	}

	err := m.store.AddInitiate(&initiate)
	if err != nil {
		return err
	}

	return m.processPaymentInitiate(&initiate)
}

// processPaymentAttempt processes an initiated payment attempt.
func (m *MissionControl) processPaymentInitiate(
	initiate *paymentInitiate) error {

	m.Lock()
	defer m.Unlock()

	if _, exists := m.paymentAttempts[initiate.id]; exists {
		return fmt.Errorf("payment attempt %v already exists",
			initiate.id)
	}

	m.paymentAttempts[initiate.id] = initiate

	return nil
}

// reportPaymentOutcome reports a failed payment to mission control as input for
// future probability estimates. It returns a bool indicating whether this error
// is a final error and no further payment attempts need to be made.
func (m *MissionControl) reportPaymentResult(paymentID uint64,
	errorSourceIndex *int, failure lnwire.FailureMessage) (bool, error) {

	timestamp := m.now()

	result := &paymentResult{
		timestamp:        timestamp,
		errorSourceIndex: errorSourceIndex,
		failure:          failure,
		id:               paymentID,
	}

	err := m.store.AddResult(result)
	if err != nil {
		return false, err
	}

	return m.processPaymentResult(result)
}

// processPaymentOutcomeFinal handles failures sent by the final hop.
func (m *MissionControl) processPaymentOutcomeFinal(timestamp time.Time,
	route *route.Route, failure lnwire.FailureMessage) bool {

	n := len(route.Hops)

	reportNode := func() {
		m.reportVertexFailure(
			timestamp, route.Hops[n-1].PubKeyBytes,
		)
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
			return true
		}

		// Otherwise penalize the last channel of the route and retry.
		m.reportChannelFailure(
			timestamp,
			route, n-1,
		)

		return false

	// We are using wrong payment hash or amount, fail the payment.
	case *lnwire.FailIncorrectPaymentAmount,
		*lnwire.FailUnknownPaymentHash:

		return true

	// The HTLC that was extended to the final hop expires too soon. Fail
	// the payment, because we may be using the wrong final cltv delta.
	case *lnwire.FailFinalExpiryTooSoon:
		// TODO(roasbeef): can happen to to race condition, try again
		// with recent block height

		// TODO(joostjager): can also happen because a node delayed
		// deliberately. What to penalize?
		return true

	default:
		// All other errors are considered terminal if coming from the
		// final hop. They indicate that something is wrong at the
		// recipient, so we do apply a penalty.

		reportNode()
		return true
	}
}

// processPaymentOutcomeIntermediate handles failures sent by an intermediate
// hop.
func (m *MissionControl) processPaymentOutcomeIntermediate(timestamp time.Time,
	route *route.Route, errorSourceIdx int,
	failure lnwire.FailureMessage) {

	reportNode := func() {
		m.reportVertexFailure(
			timestamp, route.Hops[errorSourceIdx-1].PubKeyBytes,
		)
	}

	reportOutgoingWithAmt := func() {
		m.reportEdgeFailure(
			timestamp, route.Hops[errorSourceIdx-1].PubKeyBytes,
			route.Hops[errorSourceIdx].ChannelID,
			route.Hops[errorSourceIdx-1].AmtToForward,
		)
	}

	reportOutgoing := func() {
		m.reportChannelFailure(timestamp, route, errorSourceIdx)
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
		m.reportChannelFailure(timestamp, route, errorSourceIdx-1)
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

		// Ask for second chance for using the outgoing channel of the
		// error source.
		if m.requestSecondChance(
			timestamp, route.Hops[errorSourceIdx-1].PubKeyBytes,
			route.Hops[errorSourceIdx].ChannelID,
		) {
			return
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
func (m *MissionControl) processPaymentOutcomeUnknown(timestamp time.Time,
	route *route.Route) {

	// Penalize all channels in the route to make sure the responsible node
	// is at least hit too.
	for i := range route.Hops {
		m.reportChannelFailure(timestamp, route, i)
	}
}

// processPaymentResult processes a payment result as input for future
// probability estimates. It returns a bool indicating whether this error is a
// final error and no further payment attempts need to be made.
func (m *MissionControl) processPaymentResult(result *paymentResult) (
	bool, error) {

	initiate, ok := m.paymentAttempts[result.id]
	if !ok {
		return false, fmt.Errorf("initiate not found for payment %v",
			result.id)
	}

	if result.errorSourceIndex == nil {
		m.processPaymentOutcomeUnknown(result.timestamp, initiate.route)

		return false, nil
	}

	switch *result.errorSourceIndex {

	// We don't keep a reputation for ourselves, as information about
	// channels should be available directly in links. Just retry with local
	// info that should now be updated.
	case 0:
		log.Warnf("Routing error for local channel %v occurred",
			initiate.route.Hops[0].ChannelID)

		return false, nil

	// An error from the final hop was received.
	case len(initiate.route.Hops):
		return m.processPaymentOutcomeFinal(
			result.timestamp, initiate.route, result.failure,
		), nil
	}

	// An intermediate hop failed. Interpret the outcome, update reputation
	// and try again.
	m.processPaymentOutcomeIntermediate(
		result.timestamp, initiate.route, *result.errorSourceIndex,
		result.failure,
	)

	return false, nil
}
