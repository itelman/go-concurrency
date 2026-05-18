// Package mutexpool provides a MPSC (Multi-Producer, Single-Consumer) object
// pool with the same interface as sync.Pool.
//
// # Design
//
// The pool maintains two tiers of storage — local and shared — plus a
// two-generation victim cache that mirrors sync.Pool's GC behavior:
//
//	local  – the consumer's private slice. Get() drains it without taking any
//	         lock, because only one goroutine ever calls Get().
//
//	shared – producers append here under a mutex. When local is exhausted,
//	         Get() swaps the two wrappers under the lock (O(1) pointer swap,
//	         not an O(n) copy) and drains the promoted slice lock-free.
//
// On every GC cycle the runtime fires cleanupPools (registered via go:linkname
// into sync.runtime_registerPoolCleanup). The cleanup atomically
// moves primary→victim and clears primary, giving items a second chance to be
// retrieved before they are dropped — identical to sync.Pool's two-generation
// scheme.
//
// # Contract
//
//   - Get MUST be called from a single goroutine at a time (MPSC contract).
//     No runtime guard is added; violating this causes data races.
//   - Put MAY be called from any number of goroutines concurrently.
//   - The pool MUST NOT be copied after first use (go vet catches this via
//     the embedded sync.Once and sync.Mutex).
//
// # GC safety
//
// cleanupPools is invoked during a sensitive phase of the GC. Only
// pointer-width atomic operations are safe there — no mutex locking, no
// allocation, no calls that can park a goroutine. All four slice-wrapper
// pointers are therefore atomic.Pointer[sliceWrapper], allowing the cleanup
// to swap them with a single atomic instruction each.
package mutexpool

import (
	"sync"
	"sync/atomic"
	_ "unsafe" // required for go:linkname
)

// ---------------------------------------------------------------------------
// Runtime GC hook
// ---------------------------------------------------------------------------

// registerPoolCleanup wires cleanupPools into the same STW callback used by
// sync.Pool. It fires once per GC cycle, before the sweep phase begins.
//
//go:linkname registerPoolCleanup sync.runtime_registerPoolCleanup
func registerPoolCleanup(cleanup func())

// allPoolsMu guards allPools. It is held only during pool registration and
// during the GC cleanup sweep — never during normal Put/Get operation.
var (
	allPoolsMu sync.Mutex
	allPools   []*Pool
)

func init() {
	registerPoolCleanup(cleanupPools)
}

// cleanupPools is called by the runtime on every GC cycle.
//
// IMPORTANT: this function runs during a sensitive scheduler state. It MUST
// NOT acquire any mutex, allocate memory, or call any function that can park
// a goroutine (e.g. sync.Mutex.Lock, fmt.*, etc.). Doing so corrupts the
// scheduler P-state and causes a fatal runtime crash.
//
// Only pointer-width atomic operations are permitted here.
func cleanupPools() {
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock()
	for _, p := range allPools {
		// Rotate primary → victim in one atomic swap each.
		//
		// GC cycle N:   primary → victim  (items still reachable via Get)
		// GC cycle N+1: old victim dropped (items collected by GC)
		//
		// Swap returns the old primary; Store overwrites the old victim.
		// Both are single atomic pointer operations — safe during GC cleanup.
		p.victimLocal.Store(p.local.Swap(nil))
		p.victimShared.Store(p.shared.Swap(nil))
	}
}

// ---------------------------------------------------------------------------
// sliceWrapper
// ---------------------------------------------------------------------------

// sliceWrapper boxes a []any so that atomic.Pointer can track a single heap
// object rather than a fat slice header (pointer + len + cap).
// This also lets Get() recycle the backing array by resetting items[:0]
// instead of allocating a new slice when the wrapper is reused as shared.
type sliceWrapper struct {
	items []any
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

// Pool is a concurrency-safe object pool optimized for a single consumer
// calling Get() and multiple producers calling Put() concurrently.
//
// The zero value is ready to use. Set New before the first call if you want
// automatic object creation on cache miss.
type Pool struct {
	// New is called by Get when all cache tiers are empty.
	// May be nil; in that case Get returns nil on a cache miss.
	// Must not be changed concurrently with Get calls.
	New func() any

	// initOnce ensures the pool is registered with the GC cleanup exactly once.
	initOnce sync.Once

	// mu protects shared and victimShared.
	// local and victimLocal are only ever written by the single consumer,
	// so they don't need the mutex for consumer-side reads/writes.
	// Exception: cleanupPools swaps all four via atomic ops (no mutex needed
	// there — see the GC safety note above).
	mu sync.Mutex

	// local is the consumer's private cache. Get() reads and writes it
	// without acquiring mu because only one goroutine calls Get().
	// Stored as atomic.Pointer so cleanupPools can swap it safely.
	local atomic.Pointer[sliceWrapper]

	// shared is the producers' cache. Put() appends under mu.
	// Get() promotes it to local by swapping the two pointers under mu.
	shared atomic.Pointer[sliceWrapper]

	// victimLocal and victimShared hold items that survived one GC cycle.
	// They are drained by Get() before calling New, giving items a second
	// chance to be reused rather than garbage-collected.
	victimLocal  atomic.Pointer[sliceWrapper]
	victimShared atomic.Pointer[sliceWrapper]
}

// register adds this pool to the global list so cleanupPools visits it.
// Called lazily on first Put or Get; safe for concurrent callers via Once.
func (p *Pool) register() {
	p.initOnce.Do(func() {
		allPoolsMu.Lock()
		allPools = append(allPools, p)
		allPoolsMu.Unlock()
	})
}

// Get returns an item from the pool.
//
// The lookup order is:
//  1. local  — consumer's private cache (no lock)
//  2. shared — producers' cache, promoted to local via O(1) pointer swap
//  3. victimLocal  — survived one GC cycle (no lock)
//  4. victimShared — survived one GC cycle (lock required; Put may still
//     be appending to it if GC fired between Put's lock release and the
//     victim rotation)
//  5. p.New() — allocate a fresh object, or return nil if New is unset
//
// MUST be called from a single goroutine at a time.
func (p *Pool) Get() any {
	p.register()

	for {
		// ----------------------------------------------------------------
		// Tier 1 — local (consumer-private, no lock needed)
		// ----------------------------------------------------------------
		if l := p.local.Load(); l != nil {
			if n := len(l.items); n > 0 {
				item := l.items[n-1]
				l.items[n-1] = nil // clear slot to avoid memory leak
				l.items = l.items[:n-1]
				if item != nil {
					return item
				}
				// item was nil (shouldn't happen in normal use); retry
				continue
			}
		}

		// ----------------------------------------------------------------
		// Tier 2 — promote shared → local under lock (O(1) pointer swap)
		//
		// We also recycle local's now-empty backing array: reset its length
		// to zero and hand it back as the new shared wrapper so producers
		// can reuse the already-allocated slice capacity.
		// ----------------------------------------------------------------
		p.mu.Lock()
		l := p.local.Load()
		s := p.shared.Load()
		if l != nil {
			l.items = l.items[:0] // recycle capacity; keep the backing array
		}
		p.shared.Store(l) // empty local becomes new shared
		p.local.Store(s)  // full shared becomes new local
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

		// ----------------------------------------------------------------
		// Tier 3 — victimLocal (consumer-private victim; no lock needed)
		//
		// Promote the victim wrapper to local so subsequent Get() calls
		// drain it via the fast Tier-1 path.
		// ----------------------------------------------------------------
		if vl := p.victimLocal.Load(); vl != nil {
			if n := len(vl.items); n > 0 {
				item := vl.items[n-1]
				vl.items[n-1] = nil
				vl.items = vl.items[:n-1]
				p.local.Store(vl)        // promote: victim becomes new local
				p.victimLocal.Store(nil) // clear victim slot
				if item != nil {
					return item
				}
				continue
			}
		}

		// ----------------------------------------------------------------
		// Tier 4 — victimShared (lock required)
		//
		// Put() always appends to shared under mu. If GC fired after Put()
		// released mu but before cleanupPools moved shared → victimShared,
		// a concurrent Put() on the next cycle might still hold mu and be
		// appending to what is now victimShared. The lock prevents that race.
		// ----------------------------------------------------------------
		p.mu.Lock()
		vs := p.victimShared.Load()
		if vs != nil {
			if n := len(vs.items); n > 0 {
				item := vs.items[n-1]
				vs.items[n-1] = nil
				vs.items = vs.items[:n-1]
				p.local.Store(vs)         // promote victim to local
				p.victimShared.Store(nil) // clear victim slot
				p.mu.Unlock()
				if item != nil {
					return item
				}
				continue
			}
		}
		p.mu.Unlock()

		// ----------------------------------------------------------------
		// Tier 5 — all caches empty; allocate or return nil
		// ----------------------------------------------------------------
		if p.New != nil {
			return p.New()
		}
		return nil
	}
}

// Put adds x to the pool for future reuse.
//
// Put is safe to call from any number of goroutines concurrently.
// Calling Put(nil) is a no-op, matching sync.Pool behavior.
func (p *Pool) Put(x any) {
	if x == nil {
		return
	}
	p.register()

	p.mu.Lock()
	s := p.shared.Load()
	if s == nil {
		// First Put after pool creation or after GC cleared shared.
		// Allocate a fresh wrapper; its slice grows on demand.
		s = &sliceWrapper{}
		p.shared.Store(s)
	}
	s.items = append(s.items, x)
	p.mu.Unlock()
}
