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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		totalItems := producers * itemsPerProducer

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
				_ = p.Get()
			}
		}()

		wg.Wait()
	}
}

// =============================================================================
// sync.Pool Benchmarks
// =============================================================================

func BenchmarkSyncPool(b *testing.B) {
	p := &sync.Pool{
		New: func() any { return new(BenchObj) },
	}

	b.Run("Light_1_Producer", func(b *testing.B) { runMPSC(b, p, 1, 100) })
	b.Run("Medium_4_Producers", func(b *testing.B) { runMPSC(b, p, 4, 25) })
	b.Run("Heavy_10_Producers", func(b *testing.B) { runMPSC(b, p, 10, 10) })
}

// =============================================================================
// Mutex-Based Custom Pool Benchmarks
// =============================================================================

func BenchmarkMutexPool(b *testing.B) {
	p := &mutexpool.Pool{
		New: func() any { return new(BenchObj) },
	}

	b.Run("Light_1_Producer", func(b *testing.B) { runMPSC(b, p, 1, 100) })
	b.Run("Medium_4_Producers", func(b *testing.B) { runMPSC(b, p, 4, 25) })
	b.Run("Heavy_10_Producers", func(b *testing.B) { runMPSC(b, p, 10, 10) })
}

// =============================================================================
// Channel-Based Pool Benchmarks
// =============================================================================

func BenchmarkChannelPool(b *testing.B) {
	// Size is set to 100 to comfortably accommodate the maximum totalItems (10x10)
	p := chanpool.NewPool(100, func() any { return new(BenchObj) })

	b.Run("Light_1_Producer", func(b *testing.B) { runMPSC(b, p, 1, 100) })
	b.Run("Medium_4_Producers", func(b *testing.B) { runMPSC(b, p, 4, 25) })
	b.Run("Heavy_10_Producers", func(b *testing.B) { runMPSC(b, p, 10, 10) })
}
