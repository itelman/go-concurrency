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
func runMPSC(b *testing.B, p poolInterface, producers, itemsPerProducer int) {
	totalItems := producers * itemsPerProducer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		// Producers (Multiple goroutines pushing concurrently)
		for w := 0; w < producers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < itemsPerProducer; j++ {
					p.Put(new(BenchObj))
				}
			}()
		}

		// Single Consumer (One goroutine draining the pool)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < totalItems; j++ {
				_ = p.Get() // Drain the pool
			}
		}()

		wg.Wait() // Wait for this iteration to finish
	}
}

func runBenchmarks(b *testing.B, p poolInterface) {
	b.Run("VLight_2_Producer", func(b *testing.B) { runMPSC(b, p, 2, 60) })
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
	// Size is set to 100 to comfortably accommodate the maximum totalItems (10x10)
	p := chanpool.NewPool(120, func() any { return new(BenchObj) })

	runBenchmarks(b, p)
}
