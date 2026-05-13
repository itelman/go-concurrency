package go_concurrency

import (
	"sync"
	"testing"

	"github.com/itelman/go-concurrency/mutexpool"
)

func BenchmarkSyncPool(b *testing.B) {
	var p sync.Pool
	var wg sync.WaitGroup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() { // Multiple Producers
			p.Put(1)
			wg.Done()
		}()

		// Single Consumer (simulated interleaving)
		_ = p.Get()
	}
	wg.Wait()
}

func BenchmarkCustomPool(b *testing.B) {
	var p mutexpool.Pool
	var wg sync.WaitGroup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() { // Multiple Producers
			p.Put(1)
			wg.Done()
		}()

		// Single Consumer (simulated interleaving)
		_ = p.Get()
	}
	wg.Wait()
}
