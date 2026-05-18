package mutexpool

import (
	"sync"
	_ "unsafe" // Required for go:linkname
)

// Hook into the Go runtime's internal pool cleanup.
// Invoked automatically during the GC Stop-The-World (STW) phase.
//
//go:linkname registerPoolCleanup sync.runtime_registerPoolCleanup
func registerPoolCleanup(cleanup func())

var (
	allPoolsMu sync.Mutex
	allPools   []*Pool
)

func init() {
	registerPoolCleanup(cleanupPools)
}

// cleanupPools implements the two-cycle GC eviction (Victim Cache).
func cleanupPools() {
	// Runs during GC STW (all goroutines are physically paused).
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock()
	for _, p := range allPools {
		// 1. Demote current primaries to victims (overwriting/deleting old victims).
		// Single-word pointer assignments are naturally atomic and prevent torn headers.
		p.victimLocal = p.local
		p.victimShared = p.shared

		// 2. Clear primary caches
		p.local = nil
		p.shared = nil
	}
}

// sliceWrapper protects slice headers from tearing during GC STW overwrites.
type sliceWrapper struct {
	items []any
}

// Pool is a custom MPSC (Multi-Producer, Single-Consumer) object pool.
type Pool struct {
	New func() any

	initOnce sync.Once // Ensures the pool registers for GC cleanup exactly once

	// Primary caches (Cleared/Demoted on every GC cycle)
	local  *sliceWrapper // Fast-path: Accessed exclusively by the single consumer
	mu     sync.Mutex    // Protects the shared wrapper
	shared *sliceWrapper // Slow-path: Appended to by multiple producers

	// Victim caches (Demoted from primaries by the GC)
	// Read-only for the consumer. Producers never write here.
	victimLocal  *sliceWrapper
	victimShared *sliceWrapper
}

func (p *Pool) register() {
	p.initOnce.Do(func() {
		allPoolsMu.Lock()
		allPools = append(allPools, p)
		allPoolsMu.Unlock()
	})
}

// Get retrieves an item from the pool.
// Must ONLY be called by a single goroutine.
func (p *Pool) Get() any {
	p.register()

	for {
		// Primary Local Cache
		l := p.local
		if l != nil {
			if n := len(l.items); n > 0 {
				item := l.items[n-1]
				l.items[n-1] = nil // Avoid memory leaks
				l.items = l.items[:n-1]
				if item != nil {
					return item
				}
				continue // Defensive check for STW overlaps
			}
		}

		// Primary Shared Cache (Requires Lock)
		p.mu.Lock()
		s := p.shared

		// Optimization: Recycle the empty local wrapper's capacity back to the producers
		p.shared = l
		if p.shared != nil {
			p.shared.items = p.shared.items[:0]
		}

		// Steal the shared wrapper
		p.local = s
		p.mu.Unlock()

		if s != nil {
			if n := len(s.items); n > 0 {
				item := s.items[n-1]
				s.items[n-1] = nil
				s.items = s.items[:n-1]
				if item != nil {
					return item
				}
				continue
			}
		}

		// Victim Local Cache (No lock needed)
		vl := p.victimLocal
		if vl != nil {
			if n := len(vl.items); n > 0 {
				item := vl.items[n-1]
				vl.items[n-1] = nil
				vl.items = vl.items[:n-1]

				// Revive the remaining victims BACK to the primary local cache!
				p.local = vl
				p.victimLocal = nil

				if item != nil {
					return item
				}
				continue
			}
		}

		// Victim Shared Cache (No lock needed, producers don't write there)
		vs := p.victimShared
		if vs != nil {
			if n := len(vs.items); n > 0 {
				item := vs.items[n-1]
				vs.items[n-1] = nil
				vs.items = vs.items[:n-1]

				// Revive the remaining victims BACK to the primary local cache!
				p.local = vs
				p.victimShared = nil

				if item != nil {
					return item
				}
				continue
			}
		}

		// 5. Pool is completely empty
		if p.New != nil {
			return p.New()
		}
		return nil
	}
}

// Put adds x to the pool.
// Contract: Can be called concurrently by multiple goroutines.
func (p *Pool) Put(x any) {
	if x == nil {
		return
	}
	p.register()

	p.mu.Lock()
	if p.shared == nil {
		p.shared = &sliceWrapper{} // Allocates only once per GC cycle
	}
	p.shared.items = append(p.shared.items, x)
	p.mu.Unlock()
}
