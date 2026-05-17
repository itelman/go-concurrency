package pool

import (
	"sync"
	"testing"
)

type BenchObj struct {
	Data [1024]byte // 1KB payload
}

func BenchmarkSyncPool(b *testing.B) {
	var sp sync.Pool
	sp.New = func() any { return new(BenchObj) }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		ch := make(chan any, 100)

		// Producers (multiple goroutines calling Put)
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 25; j++ {
					sp.Put(new(BenchObj))
				}
			}()
		}

		// Single consumer (one goroutine calling Get)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				obj := sp.Get()
				ch <- obj
			}
		}()

		wg.Wait()
	}
}

func BenchmarkPool(b *testing.B) {
	op := NewPool(100, func() any { return new(BenchObj) })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		ch := make(chan any, 100)

		// Producers (multiple goroutines calling Put)
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 25; j++ {
					op.Put(new(BenchObj))
				}
			}()
		}

		// Single consumer (one goroutine calling Get)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				obj := op.Get()
				ch <- obj
			}
		}()

		wg.Wait()
	}
}
