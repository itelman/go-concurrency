package main

import (
	"sync"

	"github.com/itelman/go-concurrency/pool"
)

type NetworkBuffer struct {
	Data []byte
}

func main() {
	// Initialize pool with a capacity of 1024 reusable elements
	objectPool := pool.NewPool(1024, func() any {
		return &NetworkBuffer{Data: make([]byte, 4096)} // Allocates 4KB blocks
	})

	var wg sync.WaitGroup

	// MULTI-PRODUCER: Spawning multiple worker goroutines calling Put
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				// Simulating payload work
				buf := &NetworkBuffer{Data: make([]byte, 4096)}
				objectPool.Put(buf)
			}
		}(i)
	}

	// SINGLE-CONSUMER: One dedicated goroutine calling Get
	go func() {
		for {
			// Fast O(1) path without work-stealing overhead
			item := objectPool.Get().(*NetworkBuffer)
			_ = item // Process your data stream here
		}
	}()

	wg.Wait()
}
