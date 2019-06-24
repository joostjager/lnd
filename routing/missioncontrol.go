package routing

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/coreos/bbolt"
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

	prevSuccessProbability = 1.0
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
	lastPairResult map[NodePair]pairHistory

	lastNodeFailure map[route.Vertex]time.Time

	lastSecondChance map[DirectedNodePair]time.Time

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

type pairHistory struct {
	timestamp time.Time

	pairResult
}

type DirectedNodePair struct {
	From, To route.Vertex
}

type NodePair struct {
	A, B route.Vertex
}

// newNodePair instantiates a new nodePair struct. It makes sure that node a is
// the node with the lower pubkey. A second return parameters indicates whether
// the given node ordering is reversed or not.
func newNodePair(a, b route.Vertex) (NodePair, bool) {
	if bytes.Compare(a[:], b[:]) == 1 {
		return NodePair{
			A: b,
			B: a,
		}, true
	}

	return NodePair{
		A: a,
		B: b,
	}, false
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

// MissionControlSnapshot contains a snapshot of the current state of mission
// control.
type MissionControlSnapshot struct {
	// Nodes contains the per node information of this snapshot.
	Nodes []MissionControlNodeSnapshot

	// Pairs is a list of channels for which specific information is
	// logged.
	Pairs []MissionControlChannelSnapshot
}

// MissionControlNodeSnapshot contains a snapshot of the current node state in
// mission control.
type MissionControlNodeSnapshot struct {
	// Node pubkey.
	Node route.Vertex

	// Lastfail is the time of last failure, if any.
	LastFail time.Time

	// OtherChanSuccessProb is the success probability for channels not in
	// the Channels slice.
	OtherChanSuccessProb float64
}

// MissionControlChannelSnapshot contains a snapshot of the current channel
// state in mission control.
type MissionControlChannelSnapshot struct {
	NodeA, NodeB route.Vertex

	// Timestamp is the time of last outcome.
	Timestamp time.Time

	// Amount is the minimum amount for which the channel will be
	// penalized.
	Amount lnwire.MilliSatoshi

	ResultType ChannelResultType

	DirectionReverse bool

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
	success          bool
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
		lastPairResult:   make(map[NodePair]pairHistory),
		lastNodeFailure:  make(map[route.Vertex]time.Time),
		lastSecondChance: make(map[DirectedNodePair]time.Time),
		paymentAttempts:  make(map[uint64]*paymentInitiate),
		now:              time.Now,
		cfg:              cfg,
		store:            store,
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

	m.lastPairResult = make(map[NodePair]pairHistory)
	m.lastNodeFailure = make(map[route.Vertex]time.Time)
	m.lastSecondChance = make(map[DirectedNodePair]time.Time)

	log.Debugf("Mission control history cleared")

	return nil
}

// getEdgeProbability is expected to return the success probability of a payment
// from fromNode along edge.
func (m *MissionControl) getEdgeProbability(fromNode, toNode route.Vertex,
	amt lnwire.MilliSatoshi) float64 {

	m.Lock()
	defer m.Unlock()

	return m.getEdgeProbabilityForNode(fromNode, toNode, amt)
}

func (m *MissionControl) getProbAfterFail(lastFailure time.Time) float64 {
	if lastFailure.IsZero() {
		return m.cfg.AprioriHopProbability
	}

	timeSinceLastFailure := m.now().Sub(lastFailure)

	// Calculate success probability. It is an exponential curve that brings
	// the probability down to zero when a failure occurs. From there it
	// recovers asymptotically back to the a priori probability. The rate at
	// which this happens is controlled by the penaltyHalfLife parameter.
	exp := -timeSinceLastFailure.Hours() / m.cfg.PenaltyHalfLife.Hours()
	probability := m.cfg.AprioriHopProbability * (1 - math.Pow(2, exp))

	return probability
}

// getEdgeProbabilityForNode estimates the probability of successfully
// traversing a channel based on the node history.
func (m *MissionControl) getEdgeProbabilityForNode(fromNode,
	toNode route.Vertex, amt lnwire.MilliSatoshi) float64 {

	// Start by getting the last node level failure. If there is none,
	// lastFail will be zero.
	lastFail := m.lastNodeFailure[fromNode]

	// Retrieve the last pair outcome.
	pair, _ := newNodePair(fromNode, toNode)
	lastPairResult, lastPairResultExists := m.lastPairResult[pair]

	// If there is none or it happened before the last node level failure,
	// the node level failure is the most recent and thus returned.
	if lastPairResultExists && lastPairResult.timestamp.After(lastFail) {
		switch lastPairResult.resultType {
		case ChannelResultSuccess:
			return prevSuccessProbability

		// If the last pair outcome is a balance failure and the current
		// amount is less than the failed amount, ignore this as a
		// failure.
		case ChannelResultFailBalance:
			if amt >= lastPairResult.amount {
				lastFail = lastPairResult.timestamp
			}

		case ChannelResultFail:
			lastFail = lastPairResult.timestamp
		}
	}

	return m.getProbAfterFail(lastFail)
}

func (m *MissionControl) requestSecondChance(timestamp time.Time,
	fromNode, toNode route.Vertex) bool {

	m.Lock()
	defer m.Unlock()

	// Look up previous second chance time.
	pair := DirectedNodePair{
		From: fromNode,
		To:   toNode,
	}
	lastSecondChance := m.lastSecondChance[pair]

	// If the channel hasn't already be given a second chance or its last
	// second chance was long ago, we give it another chance.
	if lastSecondChance.IsZero() ||
		timestamp.Sub(lastSecondChance) >
			minSecondChanceInterval {

		m.lastSecondChance[pair] = timestamp

		log.Debugf("Second chance granted for %v->%v", fromNode, toNode)

		return true

		// Otherwise penalize the channel, because we don't allow
		// channel updates that are that frequent. This is to prevent
		// nodes from keeping us busy by continuously sending new
		// channel updates.
	}

	log.Debugf("Second chance denied for %v->%v", fromNode, toNode)

	return false
}

// GetHistorySnapshot takes a snapshot from the current mission control state
// and actual probability estimates.
func (m *MissionControl) GetHistorySnapshot() *MissionControlSnapshot {
	m.Lock()
	defer m.Unlock()

	log.Debugf("Requesting history snapshot from mission control: "+
		"node_count=%v", len(m.lastPairResult))

	nodes := make([]MissionControlNodeSnapshot, 0, len(m.lastNodeFailure))
	for v, h := range m.lastNodeFailure {
		otherProb := m.getEdgeProbabilityForNode(v, route.Vertex{}, 0)

		nodes = append(nodes, MissionControlNodeSnapshot{
			Node:                 v,
			LastFail:             h,
			OtherChanSuccessProb: otherProb,
		})
	}

	pairs := make([]MissionControlChannelSnapshot, 0, len(m.lastPairResult))

	for v, h := range m.lastPairResult {
		// Show probability assuming amount meets min
		// penalization amount.
		prob := m.getEdgeProbabilityForNode(
			v.A, v.B, h.amount,
		)

		pair := MissionControlChannelSnapshot{
			NodeA:            v.A,
			NodeB:            v.B,
			DirectionReverse: h.directionReverse,
			Amount:           h.amount,
			Timestamp:        h.timestamp,
			ResultType:       h.resultType,
			SuccessProb:      prob,
		}

		pairs = append(pairs, pair)
	}

	snapshot := MissionControlSnapshot{
		Nodes: nodes,
		Pairs: pairs,
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

// reportPaymentFail reports a failed payment to mission control as input for
// future probability estimates. It returns a bool indicating whether this error
// is a final error and no further payment attempts need to be made.
func (m *MissionControl) reportPaymentFail(paymentID uint64,
	errorSourceIndex *int, failure lnwire.FailureMessage) (bool, error) {

	timestamp := m.now()

	result := &paymentResult{
		timestamp:        timestamp,
		id:               paymentID,
		success:          false,
		errorSourceIndex: errorSourceIndex,
		failure:          failure,
	}

	err := m.store.AddResult(result)
	if err != nil {
		return false, err
	}

	return m.processPaymentResult(result)
}

// reportPaymentSuccess reports a successful payment to mission control as input
// for future probability estimates.
func (m *MissionControl) reportPaymentSuccess(paymentID uint64) error {
	timestamp := m.now()

	result := &paymentResult{
		timestamp: timestamp,
		id:        paymentID,
		success:   true,
	}

	if err := m.store.AddResult(result); err != nil {
		return err
	}

	_, err := m.processPaymentResult(result)
	return err
}

// applyInterpretation processes a payment result as input for future
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

	i := newInterpretedResult(initiate, result)

	if i.policyFailure != nil {
		if m.requestSecondChance(
			result.timestamp,
			i.policyFailure.From, i.policyFailure.To,
		) {
			return false, nil
		}
	}

	m.applyInterpretation(result.timestamp, i)

	return i.final, nil
}

// probability estimates. It returns a bool indicating whether this error is a
// final error and no further payment attempts need to be made.
func (m *MissionControl) applyInterpretation(timestamp time.Time,
	i *interpretedResult) {

	m.Lock()
	defer m.Unlock()

	for node := range i.nodeFailures {
		m.lastNodeFailure[node] = timestamp
	}

	for pair, result := range i.pairResults {
		m.lastPairResult[pair] = pairHistory{
			pairResult: result,
			timestamp:  timestamp,
		}
	}
}
