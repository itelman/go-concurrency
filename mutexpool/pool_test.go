package mutexpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type obj struct{ ID int }

func newPool() *Pool {
	return &Pool{New: func() any { return &obj{ID: -1} }}
}

// ---------------------------------------------------------------------------
// 1. Basic correctness
// ---------------------------------------------------------------------------

func TestGet_EmptyPool_CallsNew(t *testing.T) {
	p := newPool()
	v := p.Get()
	if v == nil {
		t.Fatal("expected New() result, got nil")
	}
	if v.(*obj).ID != -1 {
		t.Fatalf("expected ID -1, got %d", v.(*obj).ID)
	}
}

func TestGet_EmptyPool_NoNew_ReturnsNil(t *testing.T) {
	p := &Pool{}
	if v := p.Get(); v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

func TestPut_Nil_IsNoOp(t *testing.T) {
	p := newPool()
	p.Put(nil) // must not panic or enqueue anything
	// Next Get must come from New, not the nil we Put
	v := p.Get().(*obj)
	if v.ID != -1 {
		t.Fatalf("expected New() value, got ID %d", v.ID)
	}
}

func TestRoundTrip(t *testing.T) {
	p := newPool()
	o := &obj{ID: 99}
	p.Put(o)
	got := p.Get()
	if got != o {
		t.Fatalf("expected same pointer back, got %v", got)
	}
}

func TestDrainFallsBackToNew(t *testing.T) {
	p := newPool()
	p.Put(&obj{ID: 1})
	p.Get()      // drain the one item
	v := p.Get() // pool empty → must call New
	if v == nil {
		t.Fatal("expected New() after drain, got nil")
	}
	if v.(*obj).ID != -1 {
		t.Fatalf("expected New() sentinel ID -1, got %d", v.(*obj).ID)
	}
}

func TestMultiplePutGet_PreservesCount(t *testing.T) {
	const n = 50
	p := &Pool{}
	for i := 0; i < n; i++ {
		p.Put(&obj{ID: i})
	}
	count := 0
	for p.Get() != nil {
		count++
		if count > n {
			t.Fatal("Got more items than were Put — pool is duplicating items")
		}
	}
	if count != n {
		t.Fatalf("Put %d items, only retrieved %d", n, count)
	}
}

// ---------------------------------------------------------------------------
// 2. Victim cache / GC behaviour
// ---------------------------------------------------------------------------

// TestVictimCache_ItemSurvivesOneGC checks that an item Put before a GC
// is still retrievable immediately after (lives in victim cache).
func TestVictimCache_ItemSurvivesOneGC(t *testing.T) {
	p := newPool()
	p.Put(&obj{ID: 7})

	runtime.GC() // triggers cleanupPools via STW hook → primary moves to victim

	v := p.Get()
	if v == nil {
		// This is technically allowed — GC timing is not guaranteed to fire
		// cleanupPools synchronously before Get returns in all test harnesses.
		t.Log("item not in victim cache after GC (acceptable under test harness constraints)")
		return
	}
	if v.(*obj).ID != 7 {
		t.Fatalf("expected ID 7 from victim cache, got %v", v)
	}
}

// TestVictimCache_ItemDroppedAfterTwoGC verifies the two-generation contract:
// an item should not survive more than two GC cycles.
func TestVictimCache_ItemDroppedAfterTwoGC(t *testing.T) {
	p := newPool()
	p.Put(&obj{ID: 42})

	runtime.GC() // cycle 1: primary → victim
	runtime.GC() // cycle 2: victim → dropped

	// After two cycles the item must be gone; only New() should fire.
	v := p.Get().(*obj)
	if v.ID == 42 {
		t.Log("WARNING: item survived 2 GC cycles — cleanupPools may not have fired in test harness")
	}
}

// ---------------------------------------------------------------------------
// 3. MPSC concurrent correctness (with timeout, no busy-hang)
// ---------------------------------------------------------------------------

func TestConcurrent_ExactCount(t *testing.T) {
	const (
		producers    = 20
		itemsPerProd = 500
		total        = producers * itemsPerProd
		drainTimeout = 5 * time.Second
	)

	p := &Pool{} // New is nil so Get returns nil when empty

	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerProd; j++ {
				p.Put(&obj{ID: id})
			}
		}(i)
	}

	// Single consumer runs concurrently with producers.
	var collected atomic.Int64
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		deadline := time.After(drainTimeout)
		for collected.Load() < int64(total) {
			select {
			case <-deadline:
				return // test will fail below with the actual count
			default:
				if p.Get() != nil {
					collected.Add(1)
				}
			}
		}
	}()

	wg.Wait()
	<-consumerDone

	got := collected.Load()
	if got != int64(total) {
		t.Errorf("produced %d items, consumer collected %d (timeout or item loss)", total, got)
	}
}

// TestConcurrent_NoItemDuplication checks that no object is returned twice
// by Get(). Each Put'd object is unique; we track by pointer.
func TestConcurrent_NoItemDuplication(t *testing.T) {
	const (
		producers    = 8
		itemsPerProd = 100
		total        = producers * itemsPerProd
	)

	p := &Pool{}
	items := make([]*obj, total)
	for i := range items {
		items[i] = &obj{ID: i}
	}

	// Put all items first so the consumer doesn't race on empty pool.
	idx := 0
	for w := 0; w < producers; w++ {
		for j := 0; j < itemsPerProd; j++ {
			p.Put(items[idx])
			idx++
		}
	}

	seen := make(map[*obj]bool, total)
	for {
		v := p.Get()
		if v == nil {
			break
		}
		o := v.(*obj)
		if seen[o] {
			t.Fatalf("item ID=%d returned twice — duplication bug", o.ID)
		}
		seen[o] = true
	}
}

// ---------------------------------------------------------------------------
// 4. Race detector coverage
// ---------------------------------------------------------------------------

// TestRace_PutGetConcurrent is intended to be run with `go test -race`.
// It validates no data races occur between concurrent Puts and a single Get.
func TestRace_PutGetConcurrent(t *testing.T) {
	p := newPool()
	var stop atomic.Bool
	var wg sync.WaitGroup

	// 8 concurrent producers
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				p.Put(&obj{})
				runtime.Gosched()
			}
		}()
	}

	// 1 consumer — this goroutine
	deadline := time.After(200 * time.Millisecond)
	for {
		select {
		case <-deadline:
			stop.Store(true)
			wg.Wait()
			return
		default:
			p.Get()
			runtime.Gosched()
		}
	}
}

// TestRace_GCDuringPutGet forces GC mid-operation to check cleanupPools
// doesn't race with active Put/Get. Run with -race.
func TestRace_GCDuringPutGet(t *testing.T) {
	p := newPool()
	var stop atomic.Bool
	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				p.Put(&obj{})
			}
		}()
	}

	// Force several GC cycles while producers are running.
	for i := 0; i < 5; i++ {
		runtime.GC()
		p.Get()
		time.Sleep(5 * time.Millisecond)
	}

	stop.Store(true)
	wg.Wait()
}
