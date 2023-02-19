package store

import (
	"container/heap"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	metrics "code.cloudfoundry.org/go-metric-registry"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/v9/rpc/loggregator_v2"
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

func (store *Store) getOrInitializeQueue(sourceId string) (*envelopeQueue, bool) {
	var newStorage bool

	store.initializationMutex.Lock()
	defer store.initializationMutex.Unlock()

	queue, existingSourceId := store.storageIndex.Load(sourceId)

	if !existingSourceId {
		queue = newEnvelopeQueue(sourceId)
		store.storageIndex.Store(sourceId, queue)
		newStorage = true
	}

	return queue.(*envelopeQueue), newStorage
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

	queue, _ := store.getOrInitializeQueue(sourceId)
	queue.insertOrSwap(store, envelope)
}

func (store *Store) buildExpirationHeap() *expirationHeap {
	expHeap := &expirationHeap{}
	heap.Init(expHeap)

	store.storageIndex.Range(func(sourceId interface{}, queue interface{}) bool {
		heap.Push(expHeap, newExpirationHeapItem(queue.(*envelopeQueue)))

		return true
	})

	return expHeap
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

	expHeap := store.buildExpirationHeap()

	// Remove envelopes one at a time, popping state from the expirationHeap
	for i := 0; i < numberToPrune && expHeap.Len() > 0; i++ {
		oldest_ := heap.Pop(expHeap)
		oldest := oldest_.(expirationHeapItem)
		newOldestTimestamp, valid := oldest.queue.removeOldestEnvelope(store)
		if valid {
			// re-insert with new timestamp
			oldest.timestamp = newOldestTimestamp
			heap.Push(expHeap, oldest)
		} else {
			store.storageIndex.Delete(oldest.sourceId)
		}
	}

	// Always update our store size metric and close out the channel when we return
	defer func() {
		store.metrics.storeSize.Set(float64(atomic.LoadInt64(&store.count)))
		store.sendTruncationCompleted(true)
	}()

	// If there's nothing left on the heap, our store is empty, so we can
	// reset everything to default values and bail out
	if expHeap.Len() == 0 {
		atomic.StoreInt64(&store.oldestTimestamp, MIN_INT64)
		store.metrics.cachePeriod.Set(0)
		return
	}

	// Otherwise, grab the next oldest timestamp and use it to update the cache period
	if oldest := expHeap.Peek(); oldest.(expirationHeapItem).queue != nil {
		atomic.StoreInt64(&store.oldestTimestamp, oldest.(expirationHeapItem).timestamp)
		cachePeriod := calculateCachePeriod(oldest.(expirationHeapItem).timestamp)
		store.metrics.cachePeriod.Set(float64(cachePeriod))
	}

	atomic.AddInt64(&store.consecutiveTruncation, 1)

	if store.consecutiveTruncation >= store.prunesPerGC {
		runtime.GC()
		// FIXME wat
		atomic.CompareAndSwapInt64(&store.consecutiveTruncation, store.consecutiveTruncation, 0)
	}
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
	queue_, ok := store.storageIndex.Load(index)
	if !ok {
		return nil
	}
	queue := queue_.(*envelopeQueue)

	res := queue.get(
		start,
		end,
		envelopeTypes,
		nameFilter,
		limit,
		descending,
	)

	if res != nil {
		store.metrics.egress.Add(float64(len(res)))
	}
	return res
}

// Meta returns each source ID tracked in the store.
// FIXME wat
func (store *Store) Meta() map[string]logcache_v1.MetaInfo {
	metaReport := make(map[string]logcache_v1.MetaInfo)

	store.storageIndex.Range(func(sourceId interface{}, queue interface{}) bool {
		queue.(*envelopeQueue).RLock()
		metaReport[sourceId.(string)] = logcache_v1.MetaInfo{
			Count:           int64(queue.(*envelopeQueue).meta.GetCount()),
			Expired:         queue.(*envelopeQueue).meta.GetExpired(),
			OldestTimestamp: queue.(*envelopeQueue).meta.GetOldestTimestamp(),
			NewestTimestamp: queue.(*envelopeQueue).meta.GetNewestTimestamp(),
		}
		queue.(*envelopeQueue).RUnlock()

		return true
	})

	// Range over our local copy of metaReport
	// TODO - shouldn't we just maintain Count on metaReport..?!
	for sourceId := range metaReport {
		queue, _ := store.storageIndex.Load(sourceId)

		queue.(*envelopeQueue).RLock()
		metaReport[sourceId] = logcache_v1.MetaInfo{
			Count:           int64(queue.(*envelopeQueue).envDeque.Len()),
			Expired:         queue.(*envelopeQueue).meta.GetExpired(),
			OldestTimestamp: queue.(*envelopeQueue).meta.GetOldestTimestamp(),
			NewestTimestamp: queue.(*envelopeQueue).meta.GetNewestTimestamp(),
		}
		queue.(*envelopeQueue).RUnlock()
	}
	return metaReport
}

func calculateCachePeriod(oldestTimestamp int64) int64 {
	return (time.Now().UnixNano() - oldestTimestamp) / int64(time.Millisecond)
}

// Used in tests
func (store *Store) GetConsecutiveTruncations() int64 {
	return atomic.LoadInt64(&store.consecutiveTruncation)
}
