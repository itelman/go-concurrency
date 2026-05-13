package mutexpool

import (
	"sync"
	_ "unsafe" // for go:linkname, not used for unsafe.*
)

// Hook into runtime's pool cleanup, invoked during GC Stop-The-World (STW)
//
//go:linkname registerPoolCleanup sync.runtime_registerPoolCleanup
func registerPoolCleanup(cleanup func())

var (
	allPoolsMu sync.Mutex
	allPools   []*Pool
)

// executed by runtime automatically
func init() {
	registerPoolCleanup(cleanupPools)
}

// cleanupPools drops all pooled references during Garbage Collection.
func cleanupPools() {
	// This runs during GC STW (all goroutines are paused) so
	// no need to acquire instance-level locks.
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock()
	for _, p := range allPools {
		p.local = nil
		p.shared = nil
	}
}

type Pool struct {
	New func() any

	initOnce sync.Once // to register once for GC cleanup
	local    []any     // accessed by the single consumer

	mu     sync.Mutex // protect shared slice
	shared []any      // appended to by multiple producers
}

func (p *Pool) register() {
	p.initOnce.Do(func() {
		allPoolsMu.Lock()
		allPools = append(allPools, p)
		allPoolsMu.Unlock()
	})
}

// Get Retrieves an item from the pool.
// Must ONLY be called by a single goroutine.
func (p *Pool) Get() any {
	p.register()

	// pop from local slice (no need for locking)
	if n := len(p.local); n > 0 {
		item := p.local[n-1]
		p.local[n-1] = nil // Avoid memory leaks
		p.local = p.local[:n-1]
		return item
	}

	// if local is empty, grab everything from shared
	p.mu.Lock()
	p.local, p.shared = p.shared, p.local
	p.mu.Unlock()

	// try popping again after the swap
	if n := len(p.local); n > 0 {
		item := p.local[n-1]
		p.local[n-1] = nil
		p.local = p.local[:n-1]
		return item
	}

	// empty case handling
	if p.New != nil {
		return p.New()
	}
	return nil
}

// Put adds x to the pool.
// Can be called concurrently by multiple goroutines.
func (p *Pool) Put(x any) {
	if x == nil {
		return
	}
	p.register()

	p.mu.Lock()
	p.shared = append(p.shared, x)
	p.mu.Unlock()
}
