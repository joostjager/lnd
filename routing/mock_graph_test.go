package routing

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/btcsuite/btcutil"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
)

// nextTestPubkey is global variable that is used to deterministically generate
// test keys.
var nextTestPubkey byte

// createPubkey return a new test pubkey.
func createPubkey() route.Vertex {
	pubkey := route.Vertex{nextTestPubkey}
	nextTestPubkey++
	return pubkey
}

// mockChannel holds the channel state of a channel in the mock graph.
type mockChannel struct {
	id       uint64
	capacity btcutil.Amount
	balance  lnwire.MilliSatoshi
}

// mockNode holds a set of mock channels and routing policies for a node in the
// mock graph.
type mockNode struct {
	channels map[route.Vertex]*mockChannel
	baseFee  lnwire.MilliSatoshi
	pubkey   route.Vertex
}

// newMockNode instantiates a new mock node with a newly generated pubkey.
func newMockNode() *mockNode {
	pubkey := createPubkey()
	return &mockNode{
		channels: make(map[route.Vertex]*mockChannel),
		pubkey:   pubkey,
	}
}

// fwd simulates an htlc forward through this node. If the from parameter is
// nil, this node is considered to be the sender of the payment. The route
// parameter describes the remaining route from this node onwards. If route.next
// is nil, this node is the final hop.
func (m *mockNode) fwd(from *mockNode, route *hop) (htlcResult, error) {
	next := route.next

	// Get the incoming channel, if any.
	var inChan *mockChannel
	if from != nil {
		inChan = m.channels[from.pubkey]
	}

	// If there is no next node, this is the final node and we can settle the htlc.
	if next == nil {
		// Update the incoming balance.
		inChan.balance += route.amtToFwd

		return htlcResult{}, nil
	}

	// Check if the outgoing channel has enough balance.
	outChan, ok := m.channels[next.node.pubkey]
	if !ok {
		return htlcResult{},
			fmt.Errorf("%v: unknown next %v",
				m.pubkey, next.node.pubkey)
	}
	if outChan.balance < route.amtToFwd {
		return htlcResult{
			failureSource: m.pubkey,
			failure:       lnwire.NewTemporaryChannelFailure(nil),
		}, nil
	}

	// Htlc can be forwarded, update channel balances.
	outChan.balance -= route.amtToFwd
	if inChan != nil {
		inChan.balance += route.amtToFwd
	}

	// Recursively forward down the given route.
	result, err := next.node.fwd(m, route.next)
	if err != nil {
		return htlcResult{}, err
	}

	// Revert balances when a failure occurs.
	if result.failure != nil {
		outChan.balance += route.amtToFwd
		if inChan != nil {
			inChan.balance -= route.amtToFwd
		}
	}

	return result, nil
}

// mockGraph contains a set of nodes that together for a mocked graph.
type mockGraph struct {
	t          *testing.T
	nodes      map[route.Vertex]*mockNode
	nextChanID uint64
	source     *mockNode
}

// newMockGraph instantiates a new mock graph.
func newMockGraph(t *testing.T) *mockGraph {
	return &mockGraph{
		nodes: make(map[route.Vertex]*mockNode),
		t:     t,
	}
}

// addNode adds the given mock node to the network.
func (m *mockGraph) addNode(node *mockNode) {
	m.nodes[node.pubkey] = node
}

// addChannel adds a new channel between two existing nodes on the network. It
// sets the channel balance to 50/50%.
func (m *mockGraph) addChannel(node1, node2 *mockNode, capacity btcutil.Amount) {
	id := m.nextChanID
	m.nextChanID++

	m.nodes[node1.pubkey].channels[node2.pubkey] = &mockChannel{
		capacity: capacity,
		id:       id,
		balance:  lnwire.NewMSatFromSatoshis(capacity / 2),
	}
	m.nodes[node2.pubkey].channels[node1.pubkey] = &mockChannel{
		capacity: capacity,
		id:       id,
		balance:  lnwire.NewMSatFromSatoshis(capacity / 2),
	}
}

// forEachNodeChannel calls the callback for every channel of the given node.
//
// NOTE: Part of the routingGraph interface.
func (m *mockGraph) forEachNodeChannel(nodePub route.Vertex,
	cb func(*channeldb.ChannelEdgeInfo, *channeldb.ChannelEdgePolicy,
		*channeldb.ChannelEdgePolicy) error) error {

	// Look up the mock node.
	node, ok := m.nodes[nodePub]
	if !ok {
		return channeldb.ErrGraphNodeNotFound
	}

	// Iterate over all of its channels.
	for peer, channel := range node.channels {
		// Lexicographically sort the pubkeys.
		var node1, node2 route.Vertex
		if bytes.Compare(nodePub[:], peer[:]) == -1 {
			node1, node2 = peer, nodePub
		} else {
			node1, node2 = nodePub, peer
		}

		peerNode := m.nodes[peer]

		// Call the per channel callback.
		err := cb(
			&channeldb.ChannelEdgeInfo{
				NodeKey1Bytes: node1,
				NodeKey2Bytes: node2,
			},
			&channeldb.ChannelEdgePolicy{
				ChannelID: channel.id,
				Node: &channeldb.LightningNode{
					PubKeyBytes: peer,
					Features:    lnwire.EmptyFeatureVector(),
				},
				FeeBaseMSat: node.baseFee,
			},
			&channeldb.ChannelEdgePolicy{
				ChannelID: channel.id,
				Node: &channeldb.LightningNode{
					PubKeyBytes: nodePub,
					Features:    lnwire.EmptyFeatureVector(),
				},
				FeeBaseMSat: peerNode.baseFee,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// sourceNode returns the source node of the graph.
//
// NOTE: Part of the routingGraph interface.
func (m *mockGraph) sourceNode() route.Vertex {
	return m.source.pubkey
}

// fetchNodeFeatures returns the features of the given node.
//
// NOTE: Part of the routingGraph interface.
func (m *mockGraph) fetchNodeFeatures(nodePub route.Vertex) (
	*lnwire.FeatureVector, error) {

	return lnwire.EmptyFeatureVector(), nil
}

// htlcResult describes the resolution of an htlc. If failure is nil, the htlc
// was settled.
type htlcResult struct {
	failureSource route.Vertex
	failure       lnwire.FailureMessage
}

// hop describes one hop of a route.
type hop struct {
	node     *mockNode
	amtToFwd lnwire.MilliSatoshi
	next     *hop
}

// sendHtlc sends out an htlc on the mock network and synchronously returns the
// final resolution of the htlc.
func (m *mockGraph) sendHtlc(route *route.Route) (htlcResult, error) {
	var next *hop

	// Convert the route into a structure that is suitable for recursive
	// processing.
	for i := len(route.Hops) - 1; i >= 0; i-- {
		routeHop := route.Hops[i]
		node := m.nodes[routeHop.PubKeyBytes]
		next = &hop{
			node:     node,
			next:     next,
			amtToFwd: routeHop.AmtToForward,
		}
	}

	// Create the starting hop instance.
	source := m.nodes[route.SourcePubKey]
	next = &hop{
		node:     source,
		next:     next,
		amtToFwd: route.TotalAmount,
	}

	// Recursively walk the path and obtain the htlc resolution.
	return source.fwd(nil, next)
}

// Compile-time check for the routingGraph interface.
var _ routingGraph = &mockGraph{}
