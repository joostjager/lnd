package invoices

import (
	"container/heap"
	"time"

	"github.com/lightningnetwork/lnd/lntypes"
)

type hashReleaseEvent struct {
	hash        lntypes.Hash
	releaseTime time.Time
}

// A releaseHeap is a min-heap of hashReleaseEvent objects.
type releaseHeap struct {
	heap  []*hashReleaseEvent
	index map[lntypes.Hash]int
}

func newReleaseHeap() releaseHeap {
	h := releaseHeap{
		index: make(map[lntypes.Hash]int),
		heap:  make([]*hashReleaseEvent, 0),
	}

	heap.Init(&h)

	return h
}

func (h releaseHeap) Len() int { return len(h.heap) }

func (h releaseHeap) Less(i, j int) bool {
	return h.heap[i].releaseTime.Before(h.heap[j].releaseTime)
}

func (h releaseHeap) Swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
	h.index[h.heap[i].hash] = j
	h.index[h.heap[j].hash] = i
}

func (h *releaseHeap) Push(x interface{}) {
	r := x.(*hashReleaseEvent)
	h.heap = append(h.heap, r)
	h.index[r.hash] = len(h.heap) - 1
}

func (h *releaseHeap) Pop() interface{} {
	old := h.heap
	n := len(old)
	x := old[n-1]
	h.heap = old[0 : n-1]
	delete(h.index, x.hash)
	return x
}
func (h *releaseHeap) Schedule(x *hashReleaseEvent) {
	index, ok := h.index[x.hash]
	if !ok {
		heap.Push(h, x)
		return
	}

	// Only reschedule if given a later release time.
	if x.releaseTime.Before(h.heap[index].releaseTime) {
		return
	}

	// Change the value at the specified index.
	h.heap[index] = x

	// Call heap.Fix to reorder the heap.
	heap.Fix(h, index)
}
