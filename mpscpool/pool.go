// Package mpscpool implements a high-performance, wait-free, CAS-free
// Multi-Producer Single-Consumer object pool leveraging runtime processor pinning.
package mpscpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
	_ "unsafe" // Required for go:linkname compiler directives
)

// Link into Go's internal runtime scheduler to pin the execution context
// to the current OS thread/P. This allows wait-free, single-writer access
// to a shard without lock or CAS overhead.
//
//go:linkname runtime_procPin sync.runtime_procPin
func runtime_procPin() int

//go:linkname runtime_procUnpin sync.runtime_procUnpin
func runtime_procUnpin()

// Link into the Go runtime's global pool cleanup hook. This guarantees that
// objects held inside the pool will be automatically dropped during STW GC cycles,
// preventing long-term memory bloat.
//
//go:linkname registerPoolCleanup sync.runtime_registerPoolCleanup
func registerPoolCleanup(cleanup func())

func init() {
	registerPoolCleanup(poolCleanup)
}

const (
	// queueBits dictates the size of each per-core shard ring buffer.
	// 12 bits = 4096 entries per CPU shard.
	queueBits = 12
	queueMask = (1 << queueBits) - 1
)

// spscQueue is a Single-Producer Single-Consumer ring buffer.
// It uses strict cache line padding (64 bytes) to eliminate false sharing
// between different CPU cores executing concurrent Put and Get operations.
type spscQueue struct {
	head uint32   // Accessed and advanced exclusively by a producer on this core
	_    [60]byte // 64-byte alignment padding (64 - sizeof(uint32))
	tail uint32   // Accessed and advanced exclusively by the single consumer
	_    [60]byte // 64-byte alignment padding (64 - sizeof(uint32))
	vals [1 << queueBits]any
}

// noCopy is a structural check mechanism embedded into the pool.
// It alerts static analysis tools (like go vet) if a Pool is mistakenly
// passed by value, which would break the internal pointer tracking.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Pool is a highly-optimized object pool designed specifically for
// Multi-Producer, Single-Consumer (MPSC) pipelines.
//
// Concurrency Contract:
//   - Put(x): Fully thread-safe. Can be called concurrently by infinite goroutines.
//   - Get():  Strictly thread-unsafe. Must ONLY be called by a single consumer goroutine.
type Pool struct {
	noCopy     noCopy
	New        func() any     // Optional factory function to allocate items when pool is empty
	local      unsafe.Pointer // Dynamic slice of per-core queues: *[]spscQueue
	lastIdx    uint32         // Cached index to track the consumer's last successful drain location
	registered bool           // Guard variable preventing multiple registrations in the cleanup registry
}

var (
	allPoolsMu sync.Mutex
	allPools   []*Pool
)

// poolCleanup is called globally by the Go runtime during standard garbage collection.
// It clears local shards, dropping references to cached elements so they can be GC'd.
func poolCleanup() {
	// CRITICAL: DO NOT ACQUIRE LOCKS HERE.
	// This function is invoked with the world stopped (STW) at the start of a GC cycle.
	// Because the world is stopped, no user goroutine can concurrently read or mutate
	// allPools or p.local. Attempting to call sync.Mutex.Lock() will panic or cause
	// severe runtime state corruption (such as "releasep: invalid p state").
	for _, p := range allPools {
		atomic.StorePointer(&p.local, nil)
	}
}

// initLocal dynamically scales the shard slice to comfortably accommodate
// the system's GOMAXPROCS capacity.
func (p *Pool) initLocal() {
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock()

	// Double-check pattern under the mutex
	if atomic.LoadPointer(&p.local) == nil {
		size := runtime.GOMAXPROCS(0)
		if size < 256 {
			size = 256 // Ensure safety margin for high thread-churn/preemption envs
		}
		qs := make([]spscQueue, size)
		atomic.StorePointer(&p.local, unsafe.Pointer(&qs))

		if !p.registered {
			allPools = append(allPools, p)
			p.registered = true
		}
	}
}

// Put adds an item into the pool. It is wait-free and fully concurrent-safe
// for multiple producer goroutines. Nil inputs are safely ignored.
func (p *Pool) Put(x any) {
	if x == nil {
		return
	}

	// Step 1: Lock execution to the current processor (P) core.
	// This ensures that no other goroutine on this thread can interrupt this logic.
	pid := runtime_procPin()

	l := atomic.LoadPointer(&p.local)
	if l == nil {
		runtime_procUnpin()
		p.initLocal()
		pid = runtime_procPin()
		l = atomic.LoadPointer(&p.local)
	}

	qs := (*[]spscQueue)(l)
	if pid >= len(*qs) {
		runtime_procUnpin()
		return // Guard against radical out-of-bounds P scaling changes mid-flight
	}

	q := &(*qs)[pid]

	// Step 2: Sample tracking offsets.
	// We use atomic loads to guarantee memory visibility cross-core and perfectly
	// satisfy the Go Race Detector (-race), ensuring zero runtime warnings.
	h := atomic.LoadUint32(&q.head)
	t := atomic.LoadUint32(&q.tail)

	// Step 3: Check boundary capacity. If ring buffer is full, drop item gracefully.
	if h-t < (1 << queueBits) {
		q.vals[h&queueMask] = x

		// Memory release barrier: publishes the element to the consumer thread.
		atomic.StoreUint32(&q.head, h+1)
	}

	runtime_procUnpin()
}

// Get pulls an item out of the pool. If the pool is empty, it returns nil
// (or the result of p.New() if specified).
//
// CRITICAL CONTRACT: Must ONLY be invoked by a single dedicated consumer goroutine.
func (p *Pool) Get() any {
	l := atomic.LoadPointer(&p.local)
	if l != nil {
		qs := (*[]spscQueue)(l)
		n := uint32(len(*qs))

		// Resume scanning from the shard index where we last successfully pulled an object.
		// This minimizes empty-shard scanning thrashing in heavily populated environments.
		start := p.lastIdx

		for i := uint32(0); i < n; i++ {
			idx := start + i
			if idx >= n {
				idx -= n
			}
			q := &(*qs)[idx]

			t := atomic.LoadUint32(&q.tail)
			h := atomic.LoadUint32(&q.head)

			// Is there data in this shard?
			if t != h {
				x := q.vals[t&queueMask]
				q.vals[t&queueMask] = nil // Crucial: Clear reference to prevent memory leaks/hoarding

				// Memory release barrier: returns ownership of the slot back to the producers.
				atomic.StoreUint32(&q.tail, t+1)

				p.lastIdx = idx
				return x
			}
		}
	}

	// Fallback to factory if the pool is entirely drained
	if p.New != nil {
		return p.New()
	}
	return nil
}
