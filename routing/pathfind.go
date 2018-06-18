package routing

import (
	"encoding/binary"
	"fmt"
	"math"

	"container/heap"

	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lightning-onion"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/roasbeef/btcd/btcec"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
)

const (
	// HopLimit is the maximum number hops that is permissible as a route.
	// Any potential paths found that lie above this limit will be rejected
	// with an error. This value is computed using the current fixed-size
	// packet length of the Sphinx construction.
	HopLimit = 20

	// infinity is used as a starting distance in our shortest path search.
	infinity = math.MaxInt64
)

// HopHint is a routing hint that contains the minimum information of a channel
// required for an intermediate hop in a route to forward the payment to the
// next. This should be ideally used for private channels, since they are not
// publicly advertised to the network for routing.
type HopHint struct {
	// NodeID is the public key of the node at the start of the channel.
	NodeID *btcec.PublicKey

	// ChannelID is the unique identifier of the channel.
	ChannelID uint64

	// FeeBaseMSat is the base fee of the channel in millisatoshis.
	FeeBaseMSat uint32

	// FeeProportionalMillionths is the fee rate, in millionths of a
	// satoshi, for every satoshi sent through the channel.
	FeeProportionalMillionths uint32

	// CLTVExpiryDelta is the time-lock delta of the channel.
	CLTVExpiryDelta uint16
}

// ChannelHop is an intermediate hop within the network with a greater
// multi-hop payment route. This struct contains the relevant routing policy of
// the particular edge, as well as the total capacity, and origin chain of the
// channel itself.
type ChannelHop struct {
	// Bandwidth is an estimate of the maximum amount that can be sent
	// through the channel in the direction indicated by ChannelEdgePolicy.
	// It is based on the on-chain capacity of the channel, bandwidth
	// hints passed in via SendRoute RPC and/or running amounts that
	// represent pending payments. These running amounts have msat as
	// unit. Therefore this property is expressed in msat too.
	Bandwidth lnwire.MilliSatoshi

	// Chain is a 32-byte has that denotes the base blockchain network of
	// the channel. The 32-byte hash is the "genesis" block of the
	// blockchain, or the very first block in the chain.
	//
	// TODO(roasbeef): store chain within edge info/policy in database.
	Chain chainhash.Hash

	*channeldb.ChannelEdgePolicy
}

// Hop represents the forwarding details at a particular position within the
// final route. This struct houses the values necessary to create the HTLC
// which will travel along this hop, and also encode the per-hop payload
// included within the Sphinx packet.
type Hop struct {
	// Channel is the active payment channel edge that this hop will travel
	// along. This is the _incoming_ channel to this hop.
	Channel *ChannelHop

	// OutgoingTimeLock is the timelock value that should be used when
	// crafting the _outgoing_ HTLC from this hop.
	OutgoingTimeLock uint32

	// AmtToForward is the amount that this hop will forward to the next
	// hop. This value is less than the value that the incoming HTLC
	// carries as a fee will be subtracted by the hop.
	AmtToForward lnwire.MilliSatoshi

	// Fee is the total fee that this hop will subtract from the incoming
	// payment, this difference nets the hop fees for forwarding the
	// payment.
	Fee lnwire.MilliSatoshi
}

// edgePolicyWithSource is a helper struct to keep track of the source node
// of a channel edge. ChannelEdgePolicy only contains to destination node
// of the edge.
type edgePolicyWithSource struct {
	sourceNode channeldb.Vertex
	edge       *channeldb.ChannelEdgePolicy
}

// computeFee computes the fee to forward an HTLC of `amt` milli-satoshis over
// the passed active payment channel. This value is currently computed as
// specified in BOLT07, but will likely change in the near future.
func computeFee(amt lnwire.MilliSatoshi,
	edge *channeldb.ChannelEdgePolicy) lnwire.MilliSatoshi {

	return edge.FeeBaseMSat + (amt*edge.FeeProportionalMillionths)/1000000
}

// isSamePath returns true if path1 and path2 travel through the exact same
// edges, and false otherwise.
func isSamePath(path1, path2 []*ChannelHop) bool {
	if len(path1) != len(path2) {
		return false
	}

	for i := 0; i < len(path1); i++ {
		if path1[i].ChannelID != path2[i].ChannelID {
			return false
		}
	}

	return true
}

// Route represents a path through the channel graph which runs over one or
// more channels in succession. This struct carries all the information
// required to craft the Sphinx onion packet, and send the payment along the
// first hop in the path. A route is only selected as valid if all the channels
// have sufficient capacity to carry the initial payment amount after fees are
// accounted for.
type Route struct {
	// TotalTimeLock is the cumulative (final) time lock across the entire
	// route. This is the CLTV value that should be extended to the first
	// hop in the route. All other hops will decrement the time-lock as
	// advertised, leaving enough time for all hops to wait for or present
	// the payment preimage to complete the payment.
	TotalTimeLock uint32

	// TotalFees is the sum of the fees paid at each hop within the final
	// route. In the case of a one-hop payment, this value will be zero as
	// we don't need to pay a fee to ourself.
	TotalFees lnwire.MilliSatoshi

	// TotalAmount is the total amount of funds required to complete a
	// payment over this route. This value includes the cumulative fees at
	// each hop. As a result, the HTLC extended to the first-hop in the
	// route will need to have at least this many satoshis, otherwise the
	// route will fail at an intermediate node due to an insufficient
	// amount of fees.
	TotalAmount lnwire.MilliSatoshi

	// Hops contains details concerning the specific forwarding details at
	// each hop.
	Hops []*Hop

	// nodeIndex is a map that allows callers to quickly look up if a node
	// is present in this computed route or not.
	nodeIndex map[channeldb.Vertex]struct{}

	// chanIndex is an index that allows callers to determine if a channel
	// is present in this route or not. Channels are identified by the
	// uint64 version of the short channel ID.
	chanIndex map[uint64]struct{}

	// nextHop maps a node, to the next channel that it will pass the HTLC
	// off to. With this map, we can easily look up the next outgoing
	// channel or node for pruning purposes.
	nextHopMap map[channeldb.Vertex]*ChannelHop

	// prevHop maps a node, to the channel that was directly before it
	// within the route. With this map, we can easily look up the previous
	// channel or node for pruning purposes.
	prevHopMap map[channeldb.Vertex]*ChannelHop
}

// nextHopVertex returns the next hop (by Vertex) after the target node. If the
// target node is not found in the route, then false is returned.
func (r *Route) nextHopVertex(n *btcec.PublicKey) (channeldb.Vertex, bool) {
	hop, ok := r.nextHopMap[channeldb.NewVertex(n)]
	return hop.Node, ok
}

// nextHopChannel returns the uint64 channel ID of the next hop after the
// target node. If the target node is not found in the route, then false is
// returned.
func (r *Route) nextHopChannel(n *btcec.PublicKey) (*ChannelHop, bool) {
	hop, ok := r.nextHopMap[channeldb.NewVertex(n)]
	return hop, ok
}

// prevHopChannel returns the uint64 channel ID of the before hop after the
// target node. If the target node is not found in the route, then false is
// returned.
func (r *Route) prevHopChannel(n *btcec.PublicKey) (*ChannelHop, bool) {
	hop, ok := r.prevHopMap[channeldb.NewVertex(n)]
	return hop, ok
}

// containsNode returns true if a node is present in the target route, and
// false otherwise.
func (r *Route) containsNode(v channeldb.Vertex) bool {
	_, ok := r.nodeIndex[v]
	return ok
}

// containsChannel returns true if a channel is present in the target route,
// and false otherwise. The passed chanID should be the converted uint64 form
// of lnwire.ShortChannelID.
func (r *Route) containsChannel(chanID uint64) bool {
	_, ok := r.chanIndex[chanID]
	return ok
}

// ToHopPayloads converts a complete route into the series of per-hop payloads
// that is to be encoded within each HTLC using an opaque Sphinx packet.
func (r *Route) ToHopPayloads() []sphinx.HopData {
	hopPayloads := make([]sphinx.HopData, len(r.Hops))

	// For each hop encoded within the route, we'll convert the hop struct
	// to the matching per-hop payload struct as used by the sphinx
	// package.
	for i, hop := range r.Hops {
		hopPayloads[i] = sphinx.HopData{
			// TODO(roasbeef): properly set realm, make sphinx type
			// an enum actually?
			Realm:         0,
			ForwardAmount: uint64(hop.AmtToForward),
			OutgoingCltv:  hop.OutgoingTimeLock,
		}

		// As a base case, the next hop is set to all zeroes in order
		// to indicate that the "last hop" as no further hops after it.
		nextHop := uint64(0)

		// If we aren't on the last hop, then we set the "next address"
		// field to be the channel that directly follows it.
		if i != len(r.Hops)-1 {
			nextHop = r.Hops[i+1].Channel.ChannelID
		}

		binary.BigEndian.PutUint64(hopPayloads[i].NextAddress[:],
			nextHop)
	}

	return hopPayloads
}

// newRoute returns a fully valid route between the source and target that's
// capable of supporting a payment of `amtToSend` after fees are fully
// computed. If the route is too long, or the selected path cannot support the
// fully payment including fees, then a non-nil error is returned.
//
// NOTE: The passed slice of ChannelHops MUST be sorted in forward order: from
// the source to the target node of the path finding attempt.
func newRoute(amtToSend, feeLimit lnwire.MilliSatoshi, sourceVertex channeldb.Vertex,
	pathEdges []*ChannelHop, currentHeight uint32,
	finalCLTVDelta uint16) (*Route, error) {

	// First, we'll create a new empty route with enough hops to match the
	// amount of path edges. We set the TotalTimeLock to the current block
	// height, as this is the basis that all of the time locks will be
	// calculated from.
	route := &Route{
		Hops:          make([]*Hop, len(pathEdges)),
		TotalTimeLock: currentHeight,
		nodeIndex:     make(map[channeldb.Vertex]struct{}),
		chanIndex:     make(map[uint64]struct{}),
		nextHopMap:    make(map[channeldb.Vertex]*ChannelHop),
		prevHopMap:    make(map[channeldb.Vertex]*ChannelHop),
	}

	// We'll populate the next hop map for the _source_ node with the
	// information for the first hop so the mapping is sound.
	route.nextHopMap[sourceVertex] = pathEdges[0]

	pathLength := len(pathEdges)
	for i := pathLength - 1; i >= 0; i-- {
		edge := pathEdges[i]

		// First, we'll update both the node and channel index, to
		// indicate that this Vertex, and outgoing channel link are
		// present within this route.
		v := edge.Node
		route.nodeIndex[v] = struct{}{}
		route.chanIndex[edge.ChannelID] = struct{}{}

		// If this isn't a direct payment, and this isn't the last hop
		// in the route, then we'll also populate the nextHop map to
		// allow easy route traversal by callers.
		if len(pathEdges) > 1 && i != len(pathEdges)-1 {
			route.nextHopMap[v] = route.Hops[i+1].Channel
		}

		// Now we'll start to calculate the items within the per-hop
		// payload for this current hop.
		//
		// If this is the last hop, then we send the exact amount and
		// pay no fee, as we're paying directly to the receiver, and
		// there're no additional hops.
		amtToForward := amtToSend
		fee := lnwire.MilliSatoshi(0)

		// If this isn't the last hop, to add enough funds to pay for
		// transit over the next link.
		if i != len(pathEdges)-1 {
			// We'll grab the edge policy and per-hop payload of
			// the prior hop so we can calculate fees properly.
			nextHop := route.Hops[i+1]

			// The amount that this hop needs to forward is based
			// on how much the prior hop carried plus the fee
			// that needs to be paid to the prior hop
			amtToForward = nextHop.AmtToForward + nextHop.Fee

			// The fee that needs to be paid to this hop is based
			// on the amount that this hop needs to forward and
			// its policy for the outgoing channel
			fee = computeFee(amtToForward, nextHop.Channel.ChannelEdgePolicy)
		}

		// Now we create the hop struct for this point in the route.
		// The amount to forward is the running amount, and we compute
		// the required fee based on this amount.
		nextHop := &Hop{
			Channel:      edge,
			AmtToForward: amtToForward,
			Fee:          fee,
		}

		route.TotalFees += nextHop.Fee

		// Invalidate this route if its total fees exceed our fee limit.
		if route.TotalFees > feeLimit {
			err := fmt.Sprintf("total route fees exceeded fee "+
				"limit of %v", feeLimit)
			return nil, newErrf(ErrFeeLimitExceeded, err)
		}

		// As a sanity check, we ensure that the incoming channel has
		// enough capacity to carry the required amount which
		// includes the fee dictated at each hop.
		if nextHop.AmtToForward + fee > nextHop.Channel.Bandwidth {

			err := fmt.Sprintf("channel graph has insufficient "+
				"capacity for the payment: need %v, have %v",
				nextHop.AmtToForward + fee,
				nextHop.Channel.Bandwidth)

			return nil, newErrf(ErrInsufficientCapacity, err)
		}

		// If this is the last hop, then for verification purposes, the
		// value of the outgoing time-lock should be _exactly_ the
		// absolute time out they'd expect in the HTLC.
		if i == len(pathEdges)-1 {
			// As this is the last hop, we'll use the specified
			// final CLTV delta value instead of the value from the
			// last link in the route.
			route.TotalTimeLock += uint32(finalCLTVDelta)

			nextHop.OutgoingTimeLock = currentHeight + uint32(finalCLTVDelta)
		} else {
			// Next, increment the total timelock of the entire
			// route such that each hops time lock increases as we
			// walk backwards in the route, using the delta of the
			// previous hop.
			route.TotalTimeLock += uint32(edge.TimeLockDelta)

			// Otherwise, the value of the outgoing time-lock will
			// be the value of the time-lock for the _outgoing_
			// HTLC, so we factor in their specified grace period
			// (time lock delta).
			nextHop.OutgoingTimeLock = route.TotalTimeLock -
				uint32(edge.TimeLockDelta)
		}

		route.Hops[i] = nextHop
	}

	// We'll then make a second run through our route in order to set up
	// our prev hop mapping.
	for _, hop := range route.Hops {
		vertex := hop.Channel.Node
		route.prevHopMap[vertex] = hop.Channel
	}

	// The total amount required for this route will be the value
	// that the first hop needs to forward plus the fee that
	// the first hop charges for this.
	route.TotalAmount = route.Hops[0].AmtToForward + route.Hops[0].Fee

	return route, nil
}

// edgeWeight computes the weight of an edge. This value is used when searching
// for the shortest path within the channel graph between two nodes. Currently
// a component is just 1 + the cltv delta value required at this hop, this
// value should be tuned with experimental and empirical data. We'll also
// factor in the "pure fee" through this hop, using the square of this fee as
// part of the weighting. The goal here is to bias more heavily towards fee
// ranking, and fallback to a time-lock based value in the case of a fee tie.
//
// TODO(roasbeef): compute robust weight metric
func edgeWeight(fee lnwire.MilliSatoshi, timeLockDelta uint16) int64 {
	// We square the fee itself in order to more heavily weight our
	// edge selection to bias towards lower fees.

	// TODO(joostjager): I think this will go wrong in the following situation:
	// Two paths, both the same total time weight. Path 1, three hops, costs
	// 3+3 in fees (fee weight = 3*3+3*3 = 18). Path 2, two hops,
	// costs 5 in fees (fee weight = 5*5 = 25). Path 2 is better both in
	// time and fee, but is not selected by the current implementation.
	feeWeight := int64(fee * fee)

	// The final component is then 1 plus the timelock delta.
	timeWeight := int64(1 + timeLockDelta)

	// The final weighting is: fee^2 + time_lock_delta.
	return feeWeight + timeWeight
}

// findPath attempts to find a path from the source node within the
// ChannelGraph to the target node that's capable of supporting a payment of
// `amt` value. The current approach implemented is modified version of
// Dijkstra's algorithm to find a single shortest path between the source node
// and the destination. The distance metric used for edges is related to the
// time-lock+fee costs along a particular edge. If a path is found, this
// function returns a slice of ChannelHop structs which encoded the chosen path
// from the target to the source.
func findPath(tx *bolt.Tx, graph *channeldb.ChannelGraph,
	additionalEdges map[channeldb.Vertex][]*channeldb.ChannelEdgePolicy,
	sourceVertex channeldb.Vertex, target *btcec.PublicKey,
	ignoredNodes map[channeldb.Vertex]struct{}, ignoredEdges map[uint64]struct{},
	amt lnwire.MilliSatoshi, feeLimit lnwire.MilliSatoshi,
	bandwidthHints map[uint64]lnwire.MilliSatoshi) ([]*ChannelHop, error) {

	var err error
	if tx == nil {
		tx, err = graph.Database().Begin(false)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	// First we'll initialize an empty heap which'll help us to quickly
	// locate the next edge we should visit next during our graph
	// traversal.
	var nodeHeap distanceHeap

	// For each node in the graph, we create an entry in the distance
	// map for the node set with a distance of "infinity". graph.ForEachNode
	// also returns the source node, so there is no need to add the source
	// node explictly.
	distance := make(map[channeldb.Vertex]nodeWithDist)
	if err := graph.ForEachNode(tx, func(_ *bolt.Tx, node *channeldb.LightningNode) error {
		// TODO(roasbeef): with larger graph can just use disk seeks
		// with a visited map
		distance[channeldb.Vertex(node.PubKeyBytes)] = nodeWithDist{
			dist: infinity,
			node: node.PubKeyBytes,
		}
		return nil
	}); err != nil {
		return nil, err
	}

	additionalEdgesWithSrc := make(map[channeldb.Vertex][]*edgePolicyWithSource)
	for vertex, outgoingEdgePolicies := range additionalEdges {
		// We'll also include all the nodes found within the additional edges
		// that are not known to us yet in the distance map.
		distance[vertex] = nodeWithDist{
			dist: infinity,
			node: vertex,
		}

		// Build reverse lookup to find incoming edges. Needed
		// because search is taken place from target to source.
		for _, outgoingEdgePolicy := range outgoingEdgePolicies {
			toVertex := outgoingEdgePolicy.Node
			incomingEdgePolicy := &edgePolicyWithSource{
				sourceNode: vertex,
				edge:       outgoingEdgePolicy,
			}

			additionalEdgesWithSrc[toVertex] =
				append(additionalEdgesWithSrc[toVertex],
					incomingEdgePolicy)
		}
	}

	// We can't always assume that the end destination is publicly
	// advertised to the network and included in the graph.ForEachNode call
	// above, so we'll manually include the target node. The target
	// node charges no fee. Distance is set to 0, because this is the
	// starting point of the graph traversal. We are searching backwards to 
	// get the fees first time right and correctly match channel bandwidth.
	targetVertex := channeldb.NewVertex(target)
	distance[targetVertex] = nodeWithDist{
		dist:            0,
		node:            targetVertex,
		amountToReceive: amt,
		fee:             0,
	}

	// We'll use this map as a series of "next" hop pointers. So to get
	// from `Vertex` to the target node, we'll take the edge that it's
	// mapped to within `next`.
	next := make(map[channeldb.Vertex]*ChannelHop)

	// processEdge is a helper closure that will be used to make sure edges
	// satisfy our specific requirements.
	processEdge := func(fromVertex channeldb.Vertex,
		edge *channeldb.ChannelEdgePolicy,
		bandwidth lnwire.MilliSatoshi, toNode channeldb.Vertex) {

		// If the edge is currently disabled, then we'll stop here, as
		// we shouldn't attempt to route through it.
		edgeFlags := lnwire.ChanUpdateFlag(edge.Flags)
		if edgeFlags&lnwire.ChanUpdateDisabled != 0 {
			return
		}

		// If this vertex or edge has been black listed, then we'll skip
		// exploring this edge.
		if _, ok := ignoredNodes[fromVertex]; ok {
			return
		}
		if _, ok := ignoredEdges[edge.ChannelID]; ok {
			return
		}

		nodeInfo := distance[toNode]

		amountToSend := nodeInfo.amountToReceive

		// Compute fee that fromNode is charging. It is based on the 
		// amount that needs to be sent to the next node in the route.

		// Source node has no precedessor to pay a fee. Therefore set
		// fee to zero, because it should not be included in the
		// fee limit check and edge weight.
		var fee lnwire.MilliSatoshi
		if fromVertex != sourceVertex {
			fee = computeFee(amountToSend, edge)
		} 

		// Check if accumulated fees would exceed fee limit when
		// this node would be added to the path.
		totalFee := amountToSend + fee - amt
		if totalFee > feeLimit {
			return
		}

		// Compute the tentative distance to this new channel/edge which
		// is the distance from our toNode to the target node plus the 
		// weight of this edge.
		tempDist := nodeInfo.dist + edgeWeight(fee, edge.TimeLockDelta)


		// If this new tentative distance is not better than the current
		// best known distance to this node, return. 
		if tempDist >= distance[fromVertex].dist {
			return
		} 
		
		// If the estimated band width of the channel edge is not able
		// to carry the amount that needs to be send, return.
		if bandwidth < amountToSend {
			return
		}

		// If the amountToSend is less than the minimum required amount,
		// return.
		if amountToSend < edge.MinHTLC {
			return	
		} 
		
		// If the edge has no time lock delta, the payment will always
		// fail, so return.
		
		// TODO(joostjager): Is this really true? Can't it be that
		// nodes take this risk in exchange for a extraordinary high
		// fee?
		if edge.TimeLockDelta == 0 {
			return
		}

		// All conditions are met and this new tentative distance is 
		// better than the current best known distance to this node.
		// The new better distance is recorded, and also our 
		// "next hop" map is populated with this edge. 
		if tempDist < distance[fromVertex].dist && bandwidth >= amountToSend &&
			amountToSend >= edge.MinHTLC && edge.TimeLockDelta != 0 {

			// amountReceive is the amount that the node that
			// is added to the distance map needs to receive from
			// a (to be found) previous node in the route. That
			// previous node will need to pay the amount that this
			// node forwards plus the fee it charges.
			amountToReceive := amountToSend + fee

			distance[fromVertex] = nodeWithDist{
				dist:            tempDist,
				node:            fromVertex,
				amountToReceive: amountToReceive,
				fee:             fee,
			}

			next[fromVertex] = &ChannelHop{
				ChannelEdgePolicy: edge,
				Bandwidth:         bandwidth,
			}

			// Add this new node to our heap as we'd like to further
			// explore backwards through this edge.
			heap.Push(&nodeHeap, distance[fromVertex])
		}
	}

	// TODO(roasbeef): also add path caching
	//  * similar to route caching, but doesn't factor in the amount

	

	// To start, our target node will the sole item within our distance
	// heap.
	heap.Push(&nodeHeap, distance[targetVertex])

	for nodeHeap.Len() != 0 {
		// Fetch the node within the smallest distance from our source
		// from the heap.
		partialPath := heap.Pop(&nodeHeap).(nodeWithDist)
		bestNode := partialPath.node

		// If we've reached our source (or we don't have any incoming
		// edges), then we're done here and can exit the graph
		// traversal early.
		if bestNode == sourceVertex {
			break
		}

		// Now that we've found the next potential step to take we'll
		// examine all the incoming edges (channels) from this node to
		// further our graph traversal.
		pivot := bestNode
		err := bestNode.ForEachChannelOfVertex(tx, func(tx *bolt.Tx,
			edgeInfo *channeldb.ChannelEdgeInfo,
			outEdge, inEdge *channeldb.ChannelEdgePolicy) error {

			// If there is no edge policy for this candidate
			// node, skip.
			if inEdge == nil {
				return nil
			}

			// We'll query the lower layer to see if we can obtain
			// any more up to date information concerning the
			// bandwidth of this edge.
			edgeBandwidth, ok := bandwidthHints[edgeInfo.ChannelID]
			if !ok {
				// If we don't have a hint for this edge, then
				// we'll just use the known Capacity as the
				// available bandwidth.
				edgeBandwidth = lnwire.NewMSatFromSatoshis(
					edgeInfo.Capacity,
				)
			}

			channelSourceNode := edgeInfo.OtherNodeKeyBytes(pivot[:])

			processEdge(channelSourceNode, inEdge, edgeBandwidth, pivot)

			// TODO(roasbeef): return min HTLC as error in end?

			return nil
		})
		if err != nil {
			return nil, err
		}

		// Then, we'll examine all the additional edges from the node
		// we're currently visiting. Since we don't know the capacity
		// of the private channel, we'll assume it was selected as a
		// routing hint due to having enough capacity for the payment
		// and use the payment amount as its capacity.

		bandWidth := partialPath.amountToReceive
		for _, reverseEdge := range additionalEdgesWithSrc[bestNode] {
			processEdge(reverseEdge.sourceNode, reverseEdge.edge, bandWidth, pivot)
		}
	}

	// If the source node isn't found in the next hop map, then a path
	// doesn't exist, so we terminate in an error.
	if _, ok := next[sourceVertex]; !ok {
		return nil, newErrf(ErrNoPathFound, "unable to find a path to "+
			"destination")
	}

	// Use the nextHop map to unravel the forward path from source to target.
	pathEdges := make([]*ChannelHop, 0, len(next))
	currentNode := sourceVertex
	for currentNode != targetVertex { // TODO(roasbeef): assumes no cycles
		// Determine the next hop forward using the next map
		nextNode := next[currentNode]
		
		// Add the next hop to the list of path edges.
		pathEdges = append(pathEdges, nextNode)

		// Advance current node
		currentNode = nextNode.Node
	}

	// The route is invalid if it spans more than 20 hops. The current
	// Sphinx (onion routing) implementation can only encode up to 20 hops
	// as the entire packet is fixed size. If this route is more than 20
	// hops, then it's invalid.
	numEdges := len(pathEdges)
	if numEdges > HopLimit {
		return nil, newErr(ErrMaxHopsExceeded, "potential path has "+
			"too many hops")
	}

	return pathEdges, nil
}

// findPaths implements a k-shortest paths algorithm to find all the reachable
// paths between the passed source and target. The algorithm will continue to
// traverse the graph until all possible candidate paths have been depleted.
// This function implements a modified version of Yen's. To find each path
// itself, we utilize our modified version of Dijkstra's found above. When
// examining possible spur and root paths, rather than removing edges or
// Vertexes from the graph, we instead utilize a Vertex+edge black-list that
// will be ignored by our modified Dijkstra's algorithm. With this approach, we
// make our inner path finding algorithm aware of our k-shortest paths
// algorithm, rather than attempting to use an unmodified path finding
// algorithm in a block box manner.
func findPaths(tx *bolt.Tx, graph *channeldb.ChannelGraph,
	source channeldb.Vertex, target *btcec.PublicKey,
	amt lnwire.MilliSatoshi, feeLimit lnwire.MilliSatoshi, numPaths uint32,
	bandwidthHints map[uint64]lnwire.MilliSatoshi) ([][]*ChannelHop, error) {

	ignoredEdges := make(map[uint64]struct{})
	ignoredVertexes := make(map[channeldb.Vertex]struct{})

	// TODO(roasbeef): modifying ordering within heap to eliminate final
	// sorting step?
	var (
		shortestPaths  [][]*ChannelHop
		candidatePaths pathHeap
	)

	// First we'll find a single shortest path from the source (our
	// selfNode) to the target destination that's capable of carrying amt
	// satoshis along the path before fees are calculated.
	startingPath, err := findPath(
		tx, graph, nil, source, target, ignoredVertexes, ignoredEdges,
		amt, feeLimit, bandwidthHints,
	)
	if err != nil {
		log.Errorf("Unable to find path: %v", err)
		return nil, err
	}

	// Manually insert a "self" edge emanating from ourselves. This
	// self-edge is required in order for the path finding algorithm to
	// function properly.
	firstPath := make([]*ChannelHop, 0, len(startingPath)+1)
	firstPath = append(firstPath, &ChannelHop{
		ChannelEdgePolicy: &channeldb.ChannelEdgePolicy{
			Node: source,
		},
	})
	firstPath = append(firstPath, startingPath...)

	shortestPaths = append(shortestPaths, firstPath)

	// While we still have candidate paths to explore we'll keep exploring
	// the sub-graphs created to find the next k-th shortest path.
	for k := uint32(1); k < numPaths; k++ {
		prevShortest := shortestPaths[k-1]

		// We'll examine each edge in the previous iteration's shortest
		// path in order to find path deviations from each node in the
		// path.
		for i := 0; i < len(prevShortest)-1; i++ {
			// These two maps will mark the edges and Vertexes
			// we'll exclude from the next path finding attempt.
			// These are required to ensure the paths are unique
			// and loopless.
			ignoredEdges = make(map[uint64]struct{})
			ignoredVertexes = make(map[channeldb.Vertex]struct{})

			// Our spur node is the i-th node in the prior shortest
			// path, and our root path will be all nodes in the
			// path leading up to our spurNode.
			spurNode := prevShortest[i].Node
			rootPath := prevShortest[:i+1]

			// Before we kickoff our next path finding iteration,
			// we'll find all the edges we need to ignore in this
			// next round. This ensures that we create a new unique
			// path.
			for _, path := range shortestPaths {
				// If our current rootPath is a prefix of this
				// shortest path, then we'll remove the edge
				// directly _after_ our spur node from the
				// graph so we don't repeat paths.
				if len(path) > i+1 && isSamePath(rootPath, path[:i+1]) {
					ignoredEdges[path[i+1].ChannelID] = struct{}{}
				}
			}

			// Next we'll remove all entries in the root path that
			// aren't the current spur node from the graph. This
			// ensures we don't create a path with loops.
			for _, hop := range rootPath {
				node := hop.Node
				if node == spurNode {
					continue
				}

				ignoredVertexes[node] = struct{}{}
			}

			// With the edges that are part of our root path, and
			// the Vertexes (other than the spur path) within the
			// root path removed, we'll attempt to find another
			// shortest path from the spur node to the destination.
			spurPath, err := findPath(
				tx, graph, nil, spurNode, target,
				ignoredVertexes, ignoredEdges, amt, feeLimit,
				bandwidthHints,
			)

			// If we weren't able to find a path, we'll continue to
			// the next round.
			if IsError(err, ErrNoPathFound) {
				continue
			} else if err != nil {
				return nil, err
			}

			// Create the new combined path by concatenating the
			// rootPath to the spurPath.
			newPathLen := len(rootPath) + len(spurPath)
			newPath := path{
				hops: make([]*ChannelHop, 0, newPathLen),
				dist: newPathLen,
			}
			newPath.hops = append(newPath.hops, rootPath...)
			newPath.hops = append(newPath.hops, spurPath...)

			// TODO(roasbeef): add and consult path finger print

			// We'll now add this newPath to the heap of candidate
			// shortest paths.
			heap.Push(&candidatePaths, newPath)
		}

		// If our min-heap of candidate paths is empty, then we can
		// exit early.
		if candidatePaths.Len() == 0 {
			break
		}

		// To conclude this latest iteration, we'll take the shortest
		// path in our set of candidate paths and add it to our
		// shortestPaths list as the *next* shortest path.
		nextShortestPath := heap.Pop(&candidatePaths).(path).hops
		shortestPaths = append(shortestPaths, nextShortestPath)
	}

	return shortestPaths, nil
}
