package store

import (
	"sort"

	"code.cloudfoundry.org/go-loggregator/v9/rpc/loggregator_v2"

	"github.com/gammazero/deque"
)

type envelopeDeque = deque.Deque[loggregator_v2.Envelope]

type sortedDeque struct {
	timestampDeque deque.Deque[int64]
	envelopeDeque
}

func newSortedDeque(size ...int) *sortedDeque {
	return &sortedDeque{
		timestampDeque: *deque.New[int64](size...),
		envelopeDeque: *deque.New[loggregator_v2.Envelope](size...),
	}
}

func (sd *sortedDeque) PushFront(e loggregator_v2.Envelope) {
	sd.timestampDeque.PushFront(e.Timestamp)
	sd.envelopeDeque.PushFront(e)
}

func (sd *sortedDeque) PushBack(e loggregator_v2.Envelope) {
	sd.timestampDeque.PushBack(e.Timestamp)
	sd.envelopeDeque.PushBack(e)
}

func (sd *sortedDeque) PopFront() loggregator_v2.Envelope {
	sd.timestampDeque.PopFront()
	return sd.envelopeDeque.PopFront()
}

func (sd *sortedDeque) PopBack() loggregator_v2.Envelope {
	sd.timestampDeque.PopBack()
	return sd.envelopeDeque.PopBack()
}

func (sd *sortedDeque) Set(i int, e loggregator_v2.Envelope) {
	sd.timestampDeque.Set(i, e.Timestamp)
	sd.envelopeDeque.Set(i, e)
}

func (sd *sortedDeque) Clear() {
	sd.timestampDeque.Clear()
	sd.envelopeDeque.Clear()
}

func (sd *sortedDeque) Rotate(n int) {
	sd.timestampDeque.Rotate(n)
	sd.envelopeDeque.Rotate(n)
}

func (sd *sortedDeque) Insert(at int, e loggregator_v2.Envelope) {
	sd.timestampDeque.Insert(at, e.Timestamp)
	sd.envelopeDeque.Insert(at, e)
}

func (sd *sortedDeque) Remove(at int) loggregator_v2.Envelope {
	sd.timestampDeque.Remove(at)
	return sd.envelopeDeque.Remove(at)
}

func (sd *sortedDeque) SetMinCapacity(minCapacityExp uint) {
	sd.timestampDeque.SetMinCapacity(minCapacityExp)
	sd.envelopeDeque.SetMinCapacity(minCapacityExp)
}

func (sd *sortedDeque) InsortFront(e loggregator_v2.Envelope) {
	at := sort.Search(sd.timestampDeque.Len(), func(i int) bool {
		return sd.timestampDeque.At(i) <= e.Timestamp
	})

	sd.Insert(at, e)
}

func (sd *sortedDeque) SearchBack(ts int64) int {
	return sort.Search(sd.timestampDeque.Len(), func(i int) bool {
		return sd.timestampDeque.At(i) < ts
	})
}
