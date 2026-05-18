package mutexpool

import (
	"sync"
	"sync/atomic"
	_ "unsafe" // Required for go:linkname
)

//go:linkname registerPoolCleanup sync.runtime_registerPoolCleanup
func registerPoolCleanup(cleanup func())

var (
	allPoolsMu sync.Mutex
	allPools   []*Pool
)

func init() {
	registerPoolCleanup(cleanupPools)
}

func cleanupPools() {
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock()
	for _, p := range allPools {
		// Atomically move primary → victim, clear primary.
		// No mutex — only pointer-width atomic ops, safe during GC cleanup.
		p.victimLocal.Store(p.local.Swap(nil))
		p.victimShared.Store(p.shared.Swap(nil))
	}
}

type sliceWrapper struct {
	items []any
}

// Pool is a custom MPSC (Multi-Producer, Single-Consumer) object pool.
type Pool struct {
	New func() any

	initOnce sync.Once
	mu       sync.Mutex

	local  atomic.Pointer[sliceWrapper] // was *sliceWrapper
	shared atomic.Pointer[sliceWrapper] // was *sliceWrapper

	victimLocal  atomic.Pointer[sliceWrapper]
	victimShared atomic.Pointer[sliceWrapper]
}

func (p *Pool) register() {
	p.initOnce.Do(func() {
		allPoolsMu.Lock()
		allPools = append(allPools, p)
		allPoolsMu.Unlock()
	})
}

func (p *Pool) Get() any {
	p.register()

	for {
		// 1. Primary local (single consumer — no lock needed)
		if l := p.local.Load(); l != nil {
			if n := len(l.items); n > 0 {
				item := l.items[n-1]
				l.items[n-1] = nil
				l.items = l.items[:n-1]
				if item != nil {
					return item
				}
				continue
			}
		}

		// 2. Swap local ↔ shared under lock
		p.mu.Lock()
		l := p.local.Load()
		s := p.shared.Load()
		if l != nil {
			l.items = l.items[:0]
		}
		p.shared.Store(l)
		p.local.Store(s)
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

		// 3. Victim local
		if vl := p.victimLocal.Load(); vl != nil {
			if n := len(vl.items); n > 0 {
				item := vl.items[n-1]
				vl.items[n-1] = nil
				vl.items = vl.items[:n-1]
				p.local.Store(vl)
				p.victimLocal.Store(nil)
				if item != nil {
					return item
				}
				continue
			}
		}

		// 4. Victim shared (still needs lock — Put() may be appending to it)
		p.mu.Lock()
		vs := p.victimShared.Load()
		if vs != nil {
			if n := len(vs.items); n > 0 {
				item := vs.items[n-1]
				vs.items[n-1] = nil
				vs.items = vs.items[:n-1]
				p.local.Store(vs)
				p.victimShared.Store(nil)
				p.mu.Unlock()
				if item != nil {
					return item
				}
				continue
			}
		}
		p.mu.Unlock()

		// 5. Empty
		if p.New != nil {
			return p.New()
		}
		return nil
	}
}

func (p *Pool) Put(x any) {
	if x == nil {
		return
	}
	p.register()

	p.mu.Lock()
	s := p.shared.Load()
	if s == nil {
		s = &sliceWrapper{}
		p.shared.Store(s)
	}
	s.items = append(s.items, x)
	p.mu.Unlock()
}
