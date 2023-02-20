package store

import (
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/v9/rpc/loggregator_v2"
)

type envelopeQueue struct {
	sourceId string
	meta     logcache_v1.MetaInfo

	sync.RWMutex

	envDeque *sortedDeque
}

func newEnvelopeQueue(sourceId string) *envelopeQueue {
	return &envelopeQueue{
		sourceId: sourceId,
		envDeque: newSortedDeque(128, 128),
	}
}

func (queue *envelopeQueue) insertOrSwap(store *Store, e *loggregator_v2.Envelope) {
	queue.Lock()
	defer queue.Unlock()

	// If we're at our maximum capacity, remove an envelope before inserting
	if queue.envDeque.Len() >= store.maxPerSource {
		queue.envDeque.PopBack()
		queue.meta.Expired++
		store.metrics.expired.Add(1)
	} else {
		atomic.AddInt64(&store.count, 1)
		store.metrics.storeSize.Set(float64(atomic.LoadInt64(&store.count)))
	}

	queue.envDeque.InsortFront(*e)

	if e.Timestamp > queue.meta.NewestTimestamp {
		queue.meta.NewestTimestamp = e.Timestamp
	}

	oldestTimestamp := queue.envDeque.Back().Timestamp
	queue.meta.OldestTimestamp = oldestTimestamp
	storeOldestTimestamp := atomic.LoadInt64(&store.oldestTimestamp)
	// FIXME wat
	if oldestTimestamp < storeOldestTimestamp {
		atomic.StoreInt64(&store.oldestTimestamp, oldestTimestamp)
		storeOldestTimestamp = oldestTimestamp
	}

	cachePeriod := calculateCachePeriod(storeOldestTimestamp)
	store.metrics.cachePeriod.Set(float64(cachePeriod))
}

func (queue *envelopeQueue) removeOldestEnvelope(store *Store) (int64, bool) {
	queue.Lock()
	defer queue.Unlock()

	if queue.envDeque.Len() == 0 {
		return 0, false
	}

	atomic.AddInt64(&store.count, -1)
	store.metrics.expired.Add(1)

	queue.envDeque.PopBack()

	if queue.envDeque.Len() == 0 {
		return MIN_INT64, false
	}

	oldestTimestampAfterRemoval := queue.envDeque.Back().Timestamp

	queue.meta.Expired++
	queue.meta.OldestTimestamp = oldestTimestampAfterRemoval

	return oldestTimestampAfterRemoval, true
}

func (queue *envelopeQueue) getOldestTimestamp() int64 {
	queue.RLock()
	defer queue.RUnlock()

	if queue.envDeque.Len() == 0 {
		return MIN_INT64
	}

	return queue.envDeque.Back().Timestamp
}

func (queue *envelopeQueue) get(
	start time.Time,
	end time.Time,
	envelopeTypes []logcache_v1.EnvelopeType,
	nameFilter *regexp.Regexp,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	if start.After(end) {
		return nil
	}

	queue.RLock()
	defer queue.RUnlock()

	startIndex := queue.envDeque.SearchBack(start.UnixNano())
	endIndex := queue.envDeque.SearchBack(end.UnixNano())

	var step int
	if descending {
		step = 1
		startIndex, endIndex = endIndex, startIndex
	} else {
		step = -1
		// make startIndex inclusive and endIndex exclusive as
		// we're stepping "backwards"
		startIndex--
		endIndex--
	}

	var res []*loggregator_v2.Envelope
	for i := startIndex; i != endIndex && len(res) < limit; i += step {
		e_ := queue.envDeque.At(i)
		e := filterByName(&e_, nameFilter)
		if e == nil {
			continue
		}

		if validEnvelopeType(e, envelopeTypes) {
			res = append(res, e)
		}
	}

	return res
}

func filterByName(envelope *loggregator_v2.Envelope, nameFilter *regexp.Regexp) *loggregator_v2.Envelope {
	if nameFilter == nil {
		return envelope
	}

	switch envelope.Message.(type) {
	case *loggregator_v2.Envelope_Counter:
		if nameFilter.MatchString(envelope.GetCounter().GetName()) {
			return envelope
		}

	// TODO: refactor?
	case *loggregator_v2.Envelope_Gauge:
		filteredMetrics := make(map[string]*loggregator_v2.GaugeValue)
		envelopeMetrics := envelope.GetGauge().GetMetrics()
		for metricName, gaugeValue := range envelopeMetrics {
			if !nameFilter.MatchString(metricName) {
				continue
			}
			filteredMetrics[metricName] = gaugeValue
		}

		if len(filteredMetrics) > 0 {
			return &loggregator_v2.Envelope{
				Timestamp:      envelope.Timestamp,
				SourceId:       envelope.SourceId,
				InstanceId:     envelope.InstanceId,
				DeprecatedTags: envelope.DeprecatedTags,
				Tags:           envelope.Tags,
				Message: &loggregator_v2.Envelope_Gauge{
					Gauge: &loggregator_v2.Gauge{
						Metrics: filteredMetrics,
					},
				},
			}

		}

	case *loggregator_v2.Envelope_Timer:
		if nameFilter.MatchString(envelope.GetTimer().GetName()) {
			return envelope
		}
	}

	return nil
}

func validEnvelopeType(e *loggregator_v2.Envelope, types []logcache_v1.EnvelopeType) bool {
	if types == nil {
		return true
	}
	for _, t := range types {
		if checkEnvelopeType(e, t) {
			return true
		}
	}
	return false
}

func checkEnvelopeType(e *loggregator_v2.Envelope, t logcache_v1.EnvelopeType) bool {
	if t == logcache_v1.EnvelopeType_ANY {
		return true
	}

	switch t {
	case logcache_v1.EnvelopeType_LOG:
		return e.GetLog() != nil
	case logcache_v1.EnvelopeType_COUNTER:
		return e.GetCounter() != nil
	case logcache_v1.EnvelopeType_GAUGE:
		return e.GetGauge() != nil
	case logcache_v1.EnvelopeType_TIMER:
		return e.GetTimer() != nil
	case logcache_v1.EnvelopeType_EVENT:
		return e.GetEvent() != nil
	default:
		// This should never happen. This implies the store is being used
		// poorly.
		panic("unknown type")
	}
}
