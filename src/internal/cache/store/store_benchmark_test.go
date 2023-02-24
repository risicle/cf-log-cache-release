package store_test

import (
	"bytes"
	"math/bits"
	"math/rand"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metrics "code.cloudfoundry.org/go-metric-registry"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/v9/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache/store"

	"google.golang.org/protobuf/proto"
)

const (
	MaxPerSource       = 1000000
	TruncationInterval = 500 * time.Millisecond
	PrunesPerGC        = int64(3)
	LogStepDuration    = 1 * time.Millisecond
)

var (
	MinTime     = time.Unix(0, 0)
	MaxTime     = time.Unix(0, 9223372036854775807)
	results     []*loggregator_v2.Envelope
	metaResults map[string]logcache_v1.MetaInfo
	genGet, genReset, genGetSync, genResetSync = randEnvGen(true)
)

func BenchmarkStoreWrite(b *testing.B) {
	benchmarkStoreWrite(b, 0)
}

func BenchmarkStoreWritePruning(b *testing.B) {
	benchmarkStoreWrite(b, 1000)
}

func benchmarkStoreWrite(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := genGet()
		s.Put(e, e.GetSourceId())
	}
}

func BenchmarkStoreTruncationOnWrite(b *testing.B) {
	benchmarkStoreTruncationOnWrite(b, 0)
}

func benchmarkStoreTruncationOnWrite(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(100, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := genGet()
		s.Put(e, e.GetSourceId())
	}
}

func BenchmarkStoreWriteParallel(b *testing.B) {
	benchmarkStoreWriteParallel(b, 0)
}

func BenchmarkStoreWriteParallelPruning(b *testing.B) {
	benchmarkStoreWriteParallel(b, 1000)
}

func benchmarkStoreWriteParallel(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genResetSync()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e := genGetSync()
			s.Put(e, e.GetSourceId())
		}
	})
}

func BenchmarkStoreGetTime5MinRange(b *testing.B) {
	benchmarkStoreGetTime5MinRange(b, 0)
}

func BenchmarkStoreGetTime5MinRangePruning(b *testing.B) {
	benchmarkStoreGetTime5MinRange(b, 100)
}

func benchmarkStoreGetTime5MinRange(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	oldestTimestamp := MaxTime.UnixNano()
	newestTimestamp := MinTime.UnixNano()
	for i := 0; i < MaxPerSource/2; i++ {
		e := genGet()
		if e.Timestamp < oldestTimestamp {
			oldestTimestamp = e.Timestamp
		}
		if e.Timestamp > newestTimestamp {
			newestTimestamp = e.Timestamp
		}
		s.Put(e, e.GetSourceId())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startTimestamp := time.Unix(
			0,
			rand.Int63n(1 + newestTimestamp - oldestTimestamp) + oldestTimestamp,
		)
		results = s.Get(
			getRandomSourceId(),
			startTimestamp,
			startTimestamp.Add(5 * time.Minute),
			nil,
			nil,
			1000,
			false,
		)
	}
}

func BenchmarkStoreGetTime5MinRangeParallel(b *testing.B) {
	benchmarkStoreGetTime5MinRangeParallel(b, 0)
}

func BenchmarkStoreGetTime5MinRangeParallelPruning(b *testing.B) {
	benchmarkStoreGetTime5MinRangeParallel(b, 100)
}

func benchmarkStoreGetTime5MinRangeParallel(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	oldestTimestamp := MaxTime.UnixNano()
	newestTimestamp := MinTime.UnixNano()
	for i := 0; i < MaxPerSource/2; i++ {
		e := genGet()
		if e.Timestamp < oldestTimestamp {
			oldestTimestamp = e.Timestamp
		}
		if e.Timestamp > newestTimestamp {
			newestTimestamp = e.Timestamp
		}
		s.Put(e, e.GetSourceId())
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			startTimestamp := time.Unix(
				0,
				rand.Int63n(1 + newestTimestamp - oldestTimestamp) + oldestTimestamp,
			)
			s.Get(
				getRandomSourceId(),
				startTimestamp,
				startTimestamp.Add(5 * time.Minute),
				nil,
				nil,
				1000,
				false,
			)
		}
	})
}

func BenchmarkStoreGetLogType(b *testing.B) {
	benchmarkStoreGetLogType(b, 0)
}

func BenchmarkStoreGetLogTypePruning(b *testing.B) {
	benchmarkStoreGetLogType(b, 100)
}

func benchmarkStoreGetLogType(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	for i := 0; i < MaxPerSource/2; i++ {
		e := genGet()
		s.Put(e, e.GetSourceId())
	}

	logType := []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_LOG}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = s.Get(
			getRandomSourceId(),
			MinTime,
			MaxTime,
			logType,
			nil,
			1000,
			false,
		)
	}
}

func BenchmarkStoreGetTime5MinRangeWhileWriting(b *testing.B) {
	benchmarkStoreGetTime5MinRangeWhileWriting(b, 0)
}

func BenchmarkStoreGetTime5MinRangeWhileWritingPruning(b *testing.B) {
	benchmarkStoreGetTime5MinRangeWhileWriting(b, 1000)
}

func benchmarkStoreGetTime5MinRangeWhileWriting(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	oldestTimestamp := genGet().Timestamp
	newestTimestamp := oldestTimestamp
	var newestTimestampAtomic atomic.Int64
	newestTimestampAtomic.Store(newestTimestamp)

	wReady := make(chan struct{}, 1)
	go func() {
		close(wReady)
		for i := 0; i < b.N; i++ {
			e := genGet()
			// avoid reading newestTimestampAtomic if we don't need to
			if e.Timestamp > newestTimestamp {
				newestTimestamp = e.Timestamp
				newestTimestampAtomic.Store(newestTimestamp)
			}
			s.Put(e, e.GetSourceId())
		}
	}()
	<-wReady

	b.ResetTimer()

	done := make(chan struct{}, 1)
	go func() {
		for i := 0; i < b.N; i++ {
			startTimestamp := time.Unix(
				0,
				rand.Int63n(1 + newestTimestampAtomic.Load() - oldestTimestamp) + oldestTimestamp,
			)
			results = s.Get(
				getRandomSourceId(),
				startTimestamp,
				startTimestamp.Add(5 * time.Minute),
				nil,
				nil,
				1000,
				false,
			)
		}
		close(done)
	}()
	<-done
}

func BenchmarkMeta(b *testing.B) {
	benchmarkMeta(b, 0)
}

func BenchmarkMetaPruning(b *testing.B) {
	benchmarkMeta(b, 100)
}

func benchmarkMeta(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	for i := 0; i < MaxPerSource/2; i++ {
		e := genGet()
		s.Put(e, e.GetSourceId())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metaResults = s.Meta()
	}
}

func BenchmarkMetaWhileWriting(b *testing.B) {
	benchmarkMetaWhileWriting(b, 0)
}

func BenchmarkMetaWhileWritingPruning(b *testing.B) {
	benchmarkMetaWhileWriting(b, 1000)
}

func benchmarkMetaWhileWriting(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	ready := make(chan struct{}, 1)
	go func() {
		close(ready)
		for i := 0; i < b.N; i++ {
			e := genGet()
			s.Put(e, e.GetSourceId())
		}
	}()
	<-ready

	b.ResetTimer()
	for i := 0; i < b.N * 5; i++ {
		metaResults = s.Meta()
	}
}

func BenchmarkMetaWhileReading(b *testing.B) {
	benchmarkMetaWhileReading(b, 0)
}

func BenchmarkMetaWhileReadingPruning(b *testing.B) {
	benchmarkMetaWhileReading(b, 1000)
}

func benchmarkMetaWhileReading(b *testing.B, prunerMax int) {
	runtime.GC()
	s := store.NewStore(MaxPerSource, TruncationInterval, PrunesPerGC, &staticPruner{prunerMax}, nopMetrics{})

	genReset()
	oldestTimestamp := MaxTime.UnixNano()
	newestTimestamp := MinTime.UnixNano()
	for i := 0; i < b.N; i++ {
		e := genGet()
		if e.Timestamp < oldestTimestamp {
			oldestTimestamp = e.Timestamp
		}
		if e.Timestamp > newestTimestamp {
			newestTimestamp = e.Timestamp
		}
		s.Put(e, e.GetSourceId())
	}

	ready := make(chan struct{}, 1)
	go func() {
		close(ready)
		for i := 0; i < b.N; i++ {
			startTimestamp := time.Unix(
				0,
				rand.Int63n(1 + newestTimestamp - oldestTimestamp) + oldestTimestamp,
			)
			results = s.Get(
				getRandomSourceId(),
				startTimestamp,
				startTimestamp.Add(5 * time.Minute),
				nil,
				nil,
				b.N,
				false,
			)
		}
	}()
	<-ready

	b.ResetTimer()
	for i := 0; i < b.N * 5; i++ {
		metaResults = s.Meta()
	}
}

func deepCopyEnvelope(srcEnv *loggregator_v2.Envelope) *loggregator_v2.Envelope {
	ser, err := proto.Marshal(srcEnv)
	if err != nil {
		panic(err)
	}
	newEnv := &loggregator_v2.Envelope{}
	err = proto.Unmarshal(ser, newEnv)
	if err != nil {
		panic(err)
	}

	return newEnv
}

func randEnvGen(deepCopyEnvelopes bool) (
	func() *loggregator_v2.Envelope,
	func(),
	func() *loggregator_v2.Envelope,
	func(),
) {
	var s []*loggregator_v2.Envelope
	startTimestamp := time.Now()
	for i := 0; i < 10000; i++ {
		s = append(s, benchBuildLog(
			startTimestamp.Add(time.Duration(i) * LogStepDuration),
		))
	}

	var sMutex sync.RWMutex
	var i int
	var iAtomic atomic.Int64
	reset := func() {
		tsDelta := s[len(s)-1].Timestamp - s[0].Timestamp
		// if we don't bump the timestamps all repeated envelopes
		// will go straight to the back of the buffer immediately
		for j := range s {
			s[j].Timestamp = s[j].Timestamp + tsDelta
		}

		i = 0
	}
	resetSync := func() {
		sMutex.Lock()
		defer sMutex.Unlock()

		tsDelta := s[len(s)-1].Timestamp - s[0].Timestamp
		// if we don't bump the timestamps all repeated envelopes
		// will go straight to the back of the buffer immediately
		for j := range s {
			s[j].Timestamp = s[j].Timestamp + tsDelta
		}

		iAtomic.Store(0)
	}
	get := func() *loggregator_v2.Envelope {
		i++
		if i >= len(s) {
			reset()
		}
		if deepCopyEnvelopes {
			return deepCopyEnvelope(s[i])
		}
		e := &loggregator_v2.Envelope{}
		*e = *s[i]
		return e
	}
	getSync := func() *loggregator_v2.Envelope {
		iNew := iAtomic.Add(1)
		if iNew >= int64(len(s)) {
			resetSync()
			iNew = 0
		}
		sMutex.RLock()
		defer sMutex.RUnlock()
		if deepCopyEnvelopes {
			return deepCopyEnvelope(s[i])
		}
		e := &loggregator_v2.Envelope{}
		*e = *s[i]
		return e
	}
	return get, reset, getSync, resetSync
}

func benchBuildLog(baseTimestamp time.Time) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		// cheap approx exponential distribution of SourceIds & InstanceIds
		SourceId: getRandomSourceId(),
		InstanceId: getRandomSourceId(),
		// cheap approx exponential distribution of jitter for timestamp
		Timestamp: baseTimestamp.Add(
			time.Duration((rand.Intn(256)-128) * (rand.Intn(256)-128)) * time.Millisecond,
		).UnixNano(),
		Tags: map[string]string {
			"foo": "bar",
			"baz": "321",
		},
		Message: &loggregator_v2.Envelope_Log{
			Log: &loggregator_v2.Log{
				Payload: bytes.Repeat([]byte("blah "), rand.Intn(64)),
			},
		},
	}
}

func getRandomSourceId() string {
	// cheap approx exponential distribution of smallish number of ids,
	// padded to approx uuid size
	return fmt.Sprintf("%16x", bits.LeadingZeros32(rand.Uint32()))
}

type nopMetrics struct{}

type nopCounter struct{}

func (nc *nopCounter) Add(float64) {}

type nopGauge struct{}

func (ng *nopGauge) Add(float64) {}
func (ng *nopGauge) Set(float64) {}

func (n nopMetrics) NewCounter(name, helpText string, opts ...metrics.MetricOption) metrics.Counter {
	return &nopCounter{}
}

func (n nopMetrics) NewGauge(name, helpText string, opts ...metrics.MetricOption) metrics.Gauge {
	return &nopGauge{}
}

type staticPruner struct {
	maximum int
}

func (s *staticPruner) GetQuantityToPrune(int64) int {
	if s.maximum > 0 {
		return rand.Intn(s.maximum)
	}
	return 0
}

func (s *staticPruner) SetMemoryReporter(metrics.Gauge) {}
