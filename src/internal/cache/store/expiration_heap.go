package store


type expirationHeap []expirationHeapItem

type expirationHeapItem struct {
	timestamp int64
	sourceId  string
	queue     *envelopeQueue
}

func newExpirationHeapItem(queue *envelopeQueue) expirationHeapItem {
	return expirationHeapItem{
		timestamp: queue.getOldestTimestamp(),
		sourceId: queue.sourceId,
		queue: queue,
	}
}

func (h expirationHeap) Len() int           { return len(h) }
func (h expirationHeap) Less(i, j int) bool { return h[i].timestamp < h[j].timestamp }
func (h expirationHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *expirationHeap) Push(x interface{}) {
	*h = append(*h, x.(expirationHeapItem))
}

func (h *expirationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

func (h *expirationHeap) Peek() interface{} {
	return (*h)[0]
}
