package routing

import (
	"testing"

	"github.com/lightningnetwork/lnd/lnwire"
)

// TestProbabilityExtrapolation tests that probabilities for tried channels are
// extrapolated to untried channels. This is a way to improve pathfinding
// success by steering away from bad nodes.
func TestProbabilityExtrapolation(t *testing.T) {
	ctx := newIntegratedRoutingContext(t)

	// Create the following network of nodes:
	// source -> expensiveNode (charges routing fee) -> target
	// source -> intermediate1 (free routing) -> intermediate(1-10) (free routing) -> target
	g := ctx.graph

	expensiveNode := newMockNode()
	expensiveNode.baseFee = 10000
	g.addNode(expensiveNode)

	g.addChannel(ctx.source, expensiveNode, 100000)
	g.addChannel(ctx.target, expensiveNode, 100000)

	intermediate1 := newMockNode()
	g.addNode(intermediate1)
	g.addChannel(ctx.source, intermediate1, 100000)

	for i := 0; i < 10; i++ {
		imNode := newMockNode()
		g.addNode(imNode)
		g.addChannel(imNode, ctx.target, 100000)
		g.addChannel(imNode, intermediate1, 100000)

		// The channels from intermediate1 all have insufficient balance.
		g.nodes[intermediate1.pubkey].channels[imNode.pubkey].balance = 0
	}

	// It is expected that pathfinding will try to explore the routes via
	// intermediate1 first, because those are free. But as failures happen,
	// the node probability of intermediate1 will go down in favor of the
	// paid route via expensiveNode.
	//
	// The exact number of attempts required is dependent on mission control
	// config. For this test, it would have been enough to only assert that
	// we are not trying all routes via intermediate1. However, we do assert
	// a specific number of attempts to safe-guard against accidental
	// modifications anywhere in the chain of components that is involved in
	// this test.
	ctx.testPayment(5)

	// If we use a static value for the node probability (no extrapolation
	// of data from other channels), all ten bad channels will be tried
	// first before switching to the paid channel.
	ctx.mcCfg.AprioriWeight = 1
	ctx.testPayment(11)
}

// TestMppSend tests that a payment can be completed using multiple shards.
func TestMppSend(t *testing.T) {
	ctx := newIntegratedRoutingContext(t)

	// Create the following network of nodes:
	// source -> intermediate1 -> target
	// source -> intermediate2 -> target
	g := ctx.graph

	intermediate1 := newMockNode()
	g.addNode(intermediate1)

	intermediate2 := newMockNode()
	g.addNode(intermediate2)

	g.addChannel(ctx.source, intermediate1, 200000)
	g.addChannel(ctx.source, intermediate2, 200000)
	g.addChannel(ctx.target, intermediate1, 100000)
	g.addChannel(ctx.target, intermediate2, 100000)

	// It is expected that pathfinding will try first try to send the full
	// amount via the two available routes. When that fails, it will half
	// the amount to 35k sat and retry. That attempt reaches the target
	// successfully. Then the same route is tried again. Because the channel
	// only had 50k sat, it will fail. Finally the second route is tried for
	// 35k and it succeeds too. Mpp payment complete.
	ctx.amt = lnwire.NewMSatFromSatoshis(70000)

	ctx.testPayment(5)
}
