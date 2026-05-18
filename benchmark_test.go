package go_concurrency

import (
	"sync"
	"testing"

	"github.com/itelman/go-concurrency/chanpool"
	"github.com/itelman/go-concurrency/mutexpool"
)

type BenchObj struct {
	Data [1024]byte // 1KB payload
}

// poolInterface ensures we can test all implementations with the same benchmark logic.
type poolInterface interface {
	Put(any)
	Get() any
}

// runMPSC is the shared benchmark logic for Multi-Producer, Single-Consumer workloads.
// totalItems is fixed at 120 across all sub-benchmarks; only producer count varies.
// This keeps total work constant while sweeping from low-contention (2 producers)
// to high-contention (120 producers, each putting exactly 1 item).
func runMPSC(b *testing.B, p poolInterface, producers, itemsPerProducer int) {
	b.Helper()
	totalItems := producers * itemsPerProducer
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		// Multiple producers — each pushes itemsPerProducer items concurrently.
		for w := 0; w < producers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < itemsPerProducer; j++ {
					p.Put(new(BenchObj))
				}
			}()
		}

		// Single consumer — one goroutine drains the pool.
		// Contract: only this goroutine calls Get() at any point in time.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < totalItems; j++ {
				_ = p.Get()
			}
		}()

		wg.Wait()
	}
}

func runBenchmarks(b *testing.B, p poolInterface) {
	b.Helper()
	b.Run("VLight_2_Producers", func(b *testing.B) { runMPSC(b, p, 2, 60) })
	b.Run("Light_4_Producers", func(b *testing.B) { runMPSC(b, p, 4, 30) })
	b.Run("Medium_12_Producers", func(b *testing.B) { runMPSC(b, p, 12, 10) })
	b.Run("Heavy_30_Producers", func(b *testing.B) { runMPSC(b, p, 30, 4) })
	b.Run("VHeavy_60_Producers", func(b *testing.B) { runMPSC(b, p, 60, 2) })
	b.Run("SHeavy_120_Producers", func(b *testing.B) { runMPSC(b, p, 120, 1) })
}

// =============================================================================
// sync.Pool Benchmarks
// =============================================================================

func BenchmarkSyncPool(b *testing.B) {
	p := &sync.Pool{
		New: func() any { return new(BenchObj) },
	}
	runBenchmarks(b, p)
}

// =============================================================================
// Mutex-Based Custom Pool Benchmarks
// =============================================================================

func BenchmarkMutexPool(b *testing.B) {
	p := &mutexpool.Pool{
		New: func() any { return new(BenchObj) },
	}
	runBenchmarks(b, p)
}

// =============================================================================
// Channel-Based Pool Benchmarks
// =============================================================================

func BenchmarkChannelPool(b *testing.B) {
	// Capacity 120 = max total items (SHeavy: 120 producers × 1 item each).
	// Prevents channel-block stalls on the heaviest sub-benchmark.
	p := chanpool.NewPool(120, func() any { return new(BenchObj) })
	runBenchmarks(b, p)
}
