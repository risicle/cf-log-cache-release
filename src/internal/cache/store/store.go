package store

import (
	"container/heap"
	"regexp"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	metrics "code.cloudfoundry.org/go-metric-registry"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/v9/rpc/loggregator_v2"

	"github.com/gammazero/deque"
)

type MetricsRegistry interface {
	NewCounter(name, helpText string, opts ...metrics.MetricOption) metrics.Counter
	NewGauge(name, helpText string, opts ...metrics.MetricOption) metrics.Gauge
}

// MemoryConsultant is used to determine if the store should prune.
type MemoryConsultant interface {
	// Prune returns the number of envelopes to prune.
	GetQuantityToPrune(int64) int
	// setMemoryReporter accepts a reporting function for Memory Utilization
	SetMemoryReporter(metrics.Gauge)
}

const MIN_INT64 = int64(^uint64(0) >> 1)

// Store is an in-memory data store for envelopes. It will store envelopes up
// to a per-source threshold and evict oldest data first, as instructed by the
// Pruner. All functions are thread safe.
type Store struct {
	storageIndex sync.Map

	initializationMutex sync.Mutex

	// count is incremented/decremented atomically during Put
	count           int64
	oldestTimestamp int64

	maxPerSource      int
	maxTimestampFudge int64

	metrics Metrics
	mc      MemoryConsultant

	truncationCompleted chan bool
	truncationInterval  time.Duration

	prunesPerGC int64

	consecutiveTruncation int64
}

type Metrics struct {
	expired            metrics.Counter
	cachePeriod        metrics.Gauge
	ingress            metrics.Counter
	egress             metrics.Counter
	storeSize          metrics.Gauge
	truncationDuration metrics.Gauge
	memoryUtilization  metrics.Gauge
}

func NewStore(maxPerSource int, truncationInterval time.Duration, prunesPerGC int64, mc MemoryConsultant, m MetricsRegistry) *Store {
	store := &Store{
		maxPerSource:      maxPerSource,
		maxTimestampFudge: 4000,
		oldestTimestamp:   MIN_INT64,

		metrics: registerMetrics(m),

		mc:                  mc,
		truncationCompleted: make(chan bool),

		truncationInterval: truncationInterval,
		prunesPerGC:        prunesPerGC,
	}

	store.mc.SetMemoryReporter(store.metrics.memoryUtilization)

	go store.truncationLoop(store.truncationInterval)

	return store
}

func registerMetrics(m MetricsRegistry) Metrics {
	return Metrics{
		expired: m.NewCounter(
			"log_cache_expired",
			"total_expired_envelopes",
		),
		cachePeriod: m.NewGauge(
			"log_cache_cache_period",
			"Cache period in milliseconds. Calculated as the difference between the oldest envelope timestamp and now.",
			metrics.WithMetricLabels(map[string]string{"unit": "milliseconds"}),
		),
		ingress: m.NewCounter(
			"log_cache_ingress",
			"Total envelopes ingressed.",
		),
		egress: m.NewCounter(
			"log_cache_egress",
			"Total envelopes retrieved from the store.",
		),
		storeSize: m.NewGauge(
			"log_cache_store_size",
			"Current number of envelopes in the store.",
			metrics.WithMetricLabels(map[string]string{"unit": "entries"}),
		),

		//TODO convert to histogram
		truncationDuration: m.NewGauge(
			"log_cache_truncation_duration",
			"Duration of last truncation in milliseconds.",
			metrics.WithMetricLabels(map[string]string{"unit": "milliseconds"}),
		),
		memoryUtilization: m.NewGauge(
			"log_cache_memory_utilization",
			"Percentage of system memory in use by log cache. Calculated as heap memory in use divided by system memory.",
			metrics.WithMetricLabels(map[string]string{"unit": "percentage"}),
		),
	}
}

func (store *Store) getOrInitializeStorage(sourceId string) (*storage, bool) {
	var newStorage bool

	store.initializationMutex.Lock()
	defer store.initializationMutex.Unlock()

	envelopeStorage, existingSourceId := store.storageIndex.Load(sourceId)

	if !existingSourceId {
		envelopeStorage = &storage{
			sourceId: sourceId,
			envDeque: deque.New[loggregator_v2.Envelope](128, 128),
		}
		store.storageIndex.Store(sourceId, envelopeStorage.(*storage))
		newStorage = true
	}

	return envelopeStorage.(*storage), newStorage
}

func (storage *storage) insertOrSwap(store *Store, e *loggregator_v2.Envelope) {
	storage.Lock()
	defer storage.Unlock()

	// If we're at our maximum capacity, remove an envelope before inserting
	if storage.Len() >= store.maxPerSource {
		storage.PopBack()
		storage.meta.Expired++
		store.metrics.expired.Add(1)
	} else {
		atomic.AddInt64(&store.count, 1)
		store.metrics.storeSize.Set(float64(atomic.LoadInt64(&store.count)))
	}

	insertionIndex := sort.Search(storage.Len(), func(i int) bool {
		return storage.At(i).Timestamp <= e.Timestamp
	})

	storage.Insert(insertionIndex, *e)

	if e.Timestamp > storage.meta.NewestTimestamp {
		storage.meta.NewestTimestamp = e.Timestamp
	}

	oldestTimestamp := storage.Back().Timestamp
	storage.meta.OldestTimestamp = oldestTimestamp
	storeOldestTimestamp := atomic.LoadInt64(&store.oldestTimestamp)
	// FIXME wat
	if oldestTimestamp < storeOldestTimestamp {
		atomic.StoreInt64(&store.oldestTimestamp, oldestTimestamp)
		storeOldestTimestamp = oldestTimestamp
	}

	cachePeriod := calculateCachePeriod(storeOldestTimestamp)
	store.metrics.cachePeriod.Set(float64(cachePeriod))
}

func (store *Store) WaitForTruncationToComplete() bool {
	return <-store.truncationCompleted
}

func (store *Store) sendTruncationCompleted(status bool) {
	select {
	case store.truncationCompleted <- status:
		// fmt.Println("Truncation ended with status", status)
	default:
		// Don't block if the channel has no receiver
	}
}

func (store *Store) truncationLoop(runInterval time.Duration) {

	t := time.NewTimer(runInterval)

	for {
		// Wait for our timer to go off
		<-t.C

		startTime := time.Now()
		store.truncate()
		t.Reset(runInterval)
		store.metrics.truncationDuration.Set(float64(time.Since(startTime) / time.Millisecond))
	}
}

func (store *Store) Put(envelope *loggregator_v2.Envelope, sourceId string) {
	store.metrics.ingress.Add(1)

	envelopeStorage, _ := store.getOrInitializeStorage(sourceId)
	envelopeStorage.insertOrSwap(store, envelope)
}

func (store *Store) BuildExpirationHeap() *ExpirationHeap {
	expirationHeap := &ExpirationHeap{}
	heap.Init(expirationHeap)

	store.storageIndex.Range(func(sourceId interface{}, queue interface{}) bool {
		queue.(*storage).RLock()
		oldestTimestamp := queue.(*storage).Back().Timestamp
		heap.Push(expirationHeap, storageExpiration{
			timestamp: oldestTimestamp,
			sourceId: sourceId.(string),
			queue: queue.(*storage),
		})
		queue.(*storage).RUnlock()

		return true
	})

	return expirationHeap
}

// truncate removes the n oldest envelopes across all queues
func (store *Store) truncate() {
	storeCount := atomic.LoadInt64(&store.count)

	numberToPrune := store.mc.GetQuantityToPrune(storeCount)

	// Just make sure we don't try to prune more entries than we have
	// FIXME why?
	if numberToPrune > int(storeCount) {
		numberToPrune = int(storeCount)
	}

	if numberToPrune == 0 {
		store.sendTruncationCompleted(false)
		// FIXME wat
		atomic.CompareAndSwapInt64(&store.consecutiveTruncation, store.consecutiveTruncation, 0)
		return
	}

	expirationHeap := store.BuildExpirationHeap()

	// Remove envelopes one at a time, popping state from the expirationHeap
	for i := 0; i < numberToPrune && expirationHeap.Len() > 0; i++ {
		oldest := heap.Pop(expirationHeap)
		newOldestTimestamp, valid := store.removeOldestEnvelope(
			oldest.(storageExpiration).queue,
			oldest.(storageExpiration).sourceId,
		)
		if valid {
			heap.Push(
				expirationHeap,
				storageExpiration{
					timestamp: newOldestTimestamp,
					sourceId: oldest.(storageExpiration).sourceId,
					queue: oldest.(storageExpiration).queue,
				},
			)
		}
	}

	// Always update our store size metric and close out the channel when we return
	defer func() {
		store.metrics.storeSize.Set(float64(atomic.LoadInt64(&store.count)))
		store.sendTruncationCompleted(true)
	}()

	// If there's nothing left on the heap, our store is empty, so we can
	// reset everything to default values and bail out
	if expirationHeap.Len() == 0 {
		atomic.StoreInt64(&store.oldestTimestamp, MIN_INT64)
		store.metrics.cachePeriod.Set(0)
		return
	}

	// Otherwise, grab the next oldest timestamp and use it to update the cache period
	if oldest := expirationHeap.Pop(); oldest.(storageExpiration).queue != nil {
		atomic.StoreInt64(&store.oldestTimestamp, oldest.(storageExpiration).timestamp)
		cachePeriod := calculateCachePeriod(oldest.(storageExpiration).timestamp)
		store.metrics.cachePeriod.Set(float64(cachePeriod))
	}

	atomic.AddInt64(&store.consecutiveTruncation, 1)

	if store.consecutiveTruncation >= store.prunesPerGC {
		runtime.GC()
		// FIXME wat
		atomic.CompareAndSwapInt64(&store.consecutiveTruncation, store.consecutiveTruncation, 0)
	}
}

func (store *Store) removeOldestEnvelope(queueToPrune *storage, sourceId string) (int64, bool) {
	queueToPrune.Lock()
	defer queueToPrune.Unlock()

	if queueToPrune.Len() == 0 {
		return 0, false
	}

	atomic.AddInt64(&store.count, -1)
	store.metrics.expired.Add(1)

	queueToPrune.PopBack()

	if queueToPrune.Len() == 0 {
		store.storageIndex.Delete(sourceId)
		return 0, false
	}

	oldestTimestampAfterRemoval := queueToPrune.Back().Timestamp

	queueToPrune.meta.Expired++
	queueToPrune.meta.OldestTimestamp = oldestTimestampAfterRemoval

	return oldestTimestampAfterRemoval, true
}

// Get fetches envelopes from the store based on the source ID, start and end
// time. Start is inclusive while end is not: [start..end).
func (store *Store) Get(
	index string,
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

	queueAny, ok := store.storageIndex.Load(index)
	if !ok {
		return nil
	}
	queue := queueAny.(*storage)

	queue.RLock()
	defer queue.RUnlock()


	startIndex := sort.Search(queue.Len(), func(i int) bool {
		return queue.At(i).Timestamp < start.UnixNano()
	})
	endIndex := sort.Search(queue.Len(), func(i int) bool {
		return queue.At(i).Timestamp < end.UnixNano()
	})

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
		e_ := queue.At(i)
		e := store.filterByName(&e_, nameFilter)
		if e == nil {
			continue
		}

		if store.validEnvelopeType(e, envelopeTypes) {
			res = append(res, e)
		}
	}

	store.metrics.egress.Add(float64(len(res)))
	return res
}

func (store *Store) filterByName(envelope *loggregator_v2.Envelope, nameFilter *regexp.Regexp) *loggregator_v2.Envelope {
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

func (s *Store) validEnvelopeType(e *loggregator_v2.Envelope, types []logcache_v1.EnvelopeType) bool {
	if types == nil {
		return true
	}
	for _, t := range types {
		if s.checkEnvelopeType(e, t) {
			return true
		}
	}
	return false
}

func (s *Store) checkEnvelopeType(e *loggregator_v2.Envelope, t logcache_v1.EnvelopeType) bool {
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

// Meta returns each source ID tracked in the store.
func (store *Store) Meta() map[string]logcache_v1.MetaInfo {
	metaReport := make(map[string]logcache_v1.MetaInfo)

	store.storageIndex.Range(func(sourceId interface{}, queue interface{}) bool {
		queue.(*storage).RLock()
		metaReport[sourceId.(string)] = logcache_v1.MetaInfo{
			Count:           int64(queue.(*storage).meta.GetCount()),
			Expired:         queue.(*storage).meta.GetExpired(),
			OldestTimestamp: queue.(*storage).meta.GetOldestTimestamp(),
			NewestTimestamp: queue.(*storage).meta.GetNewestTimestamp(),
		}
		queue.(*storage).RUnlock()

		return true
	})

	// Range over our local copy of metaReport
	// TODO - shouldn't we just maintain Count on metaReport..?!
	for sourceId := range metaReport {
		queue, _ := store.storageIndex.Load(sourceId)

		queue.(*storage).RLock()
		metaReport[sourceId] = logcache_v1.MetaInfo{
			Count:           int64(queue.(*storage).Len()),
			Expired:         queue.(*storage).meta.GetExpired(),
			OldestTimestamp: queue.(*storage).meta.GetOldestTimestamp(),
			NewestTimestamp: queue.(*storage).meta.GetNewestTimestamp(),
		}
		queue.(*storage).RUnlock()
	}
	return metaReport
}

type envDeque = deque.Deque[loggregator_v2.Envelope]

type storage struct {
	sourceId string
	meta     logcache_v1.MetaInfo

	*envDeque
	sync.RWMutex
}

type ExpirationHeap []storageExpiration

type storageExpiration struct {
	timestamp int64
	sourceId  string
	queue     *storage
}

func (h ExpirationHeap) Len() int           { return len(h) }
func (h ExpirationHeap) Less(i, j int) bool { return h[i].timestamp < h[j].timestamp }
func (h ExpirationHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *ExpirationHeap) Push(x interface{}) {
	*h = append(*h, x.(storageExpiration))
}

func (h *ExpirationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

func calculateCachePeriod(oldestTimestamp int64) int64 {
	return (time.Now().UnixNano() - oldestTimestamp) / int64(time.Millisecond)
}

// Used in tests
func (store *Store) GetConsecutiveTruncations() int64 {
	return atomic.LoadInt64(&store.consecutiveTruncation)
}
