package routing

import (
	"fmt"
	"testing"
)

func TestMissionControl(t *testing.T) {
	// Routes defines all possible routes to a destination. A route is a
	// slice of ids that represent nodes.
	routes := [][]int{
		{0, 1, 100},
		{0, 2, 100},
	}

	mc := newTestMc()

	badNodePairList := []nodePair{
		newNodePair(1, 100),
		newNodePair(0, 1),
		newNodePair(0, 2),
		newNodePair(2, 100),
	}

	successes := 0
	attempts := 0
	badNodePair := badNodePairList[2]
	for successes < 5 {
		// "Path finding"
		var bestRoute []int
		var bestProb float64
		for _, route := range routes {
			var prob float64 = 1
			for i := 0; i < len(route)-1; i++ {
				pair := newNodePair(route[i], route[i+1])
				prob *= mc.getProbability(pair)
			}

			if prob >= bestProb {
				bestProb = prob
				bestRoute = route
			}
		}

		fmt.Printf("Attempting route: ")
		for _, p := range bestRoute {
			fmt.Printf("%v ", p)
		}
		fmt.Printf(" (prob: %v)\n", bestProb)

		// Execute payment attempt
		errorSourceIdx := -1
		for i := 0; i < len(bestRoute)-1; i++ {
			pair := newNodePair(bestRoute[i], bestRoute[i+1])

			if pair == badNodePair {
				errorSourceIdx = 0
				break
			}
		}

		// Report to mc
		if errorSourceIdx == -1 {
			mc.reportSuccess(bestRoute)
			successes++
		} else {
			mc.reportFailure(bestRoute, errorSourceIdx)
		}

		attempts++
		if attempts > 1000 {
			t.Fatal("too many attempts")
		}
	}
}

type nodePair struct {
	a, b int
}

func newNodePair(node1, node2 int) nodePair {
	if node1 < node2 {
		return nodePair{
			a: node1,
			b: node2,
		}
	}
	return nodePair{
		a: node2,
		b: node1,
	}
}

type testMc struct {
	badPairs map[nodePair]struct{}
}

func newTestMc() *testMc {
	return &testMc{
		badPairs: make(map[nodePair]struct{}),
	}
}

func (t *testMc) reportFailure(route []int, errorSourceIdx int) {
	t.badPairs[newNodePair(route[errorSourceIdx], route[errorSourceIdx+1])] = struct{}{}
}

func (t *testMc) reportSuccess(route []int) {

}

func (t *testMc) getProbability(pair nodePair) float64 {
	_, bad := t.badPairs[pair]
	if bad {
		return 0
	}
	return 1
}
