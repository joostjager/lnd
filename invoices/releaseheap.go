package invoices

import (
	"time"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lntypes"
)

type releaseEvent struct {
	hash        lntypes.Hash
	key         channeldb.CircuitKey
	releaseTime time.Time
}

// A releaseHeap is a min-heap of releaseEvent objects.
type releaseHeap []*releaseEvent

func (h releaseHeap) Len() int { return len(h) }

func (h releaseHeap) Less(i, j int) bool {
	return h[i].releaseTime.Before(h[j].releaseTime)
}

func (h releaseHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *releaseHeap) Push(x interface{}) {
	*h = append(*h, x.(*releaseEvent))
}

func (h *releaseHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
