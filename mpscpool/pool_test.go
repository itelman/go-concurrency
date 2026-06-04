package mpscpool

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// TestItem is a dummy struct used to track data integrity across threads.
type TestItem struct {
	ProducerID int
	SequenceID int
}

// 1. Basic Functionality Test
func TestPool_BasicPutGet(t *testing.T) {
	p := &Pool{}

	// Ensure empty pool returns nil if New is not provided
	if val := p.Get(); val != nil {
		t.Fatalf("Expected nil from empty pool, got %v", val)
	}

	// Put an item and retrieve it
	expected := &TestItem{ProducerID: 1, SequenceID: 100}
	p.Put(expected)

	val := p.Get()
	if val == nil {
		t.Fatal("Expected an item, got nil")
	}

	actual, ok := val.(*TestItem)
	if !ok {
		t.Fatalf("Expected type *TestItem, got %T", val)
	}

	if actual.ProducerID != expected.ProducerID || actual.SequenceID != expected.SequenceID {
		t.Errorf("Data corruption! Expected %+v, got %+v", expected, actual)
	}

	// Ensure pool is empty again
	if val := p.Get(); val != nil {
		t.Fatalf("Expected pool to be empty after draining, got %v", val)
	}
}

// 2. Fallback Factory Function Test
func TestPool_NewFallback(t *testing.T) {
	p := &Pool{
		New: func() any {
			return &TestItem{ProducerID: -1, SequenceID: -1}
		},
	}

	// Should trigger New() on empty pool
	val := p.Get()
	item, ok := val.(*TestItem)
	if !ok || item.ProducerID != -1 {
		t.Fatalf("New() factory fallback failed, got: %v", val)
	}

	// Put a real item
	p.Put(&TestItem{ProducerID: 5, SequenceID: 5})

	// Should return the real item first
	val2 := p.Get()
	item2 := val2.(*TestItem)
	if item2.ProducerID != 5 {
		t.Errorf("Expected real item, got factory item")
	}

	// Next Get should fall back to New() again
	val3 := p.Get()
	item3 := val3.(*TestItem)
	if item3.ProducerID != -1 {
		t.Errorf("Expected fallback item on second drain, got: %v", val3)
	}
}

// 3. Nil Put Protection Test
func TestPool_PutNil(t *testing.T) {
	p := &Pool{}
	p.Put(nil) // Should gracefully ignore

	if val := p.Get(); val != nil {
		t.Errorf("Pool should ignore nil inputs, but returned: %v", val)
	}
}

// 4. MPSC Concurrency Stress Test
// This ensures that multiple concurrent producers can push data safely
// and a single dedicated consumer can drain it with zero data loss or duplicates.
func TestPool_MPSC_Correctness(t *testing.T) {
	p := &Pool{} // Leave New as nil to cleanly capture empty states

	numProducers := 16
	itemsPerProducer := 200 // Well within the 4096 ring buffer safety limit
	totalExpectedItems := numProducers * itemsPerProducer

	var wg sync.WaitGroup

	// Spin up multiple concurrent producers
	for w := 0; w < numProducers; w++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				p.Put(&TestItem{
					ProducerID: producerID,
					SequenceID: i,
				})
			}
		}(w)
	}

	// Single Dedicated Consumer tracking results
	// Contract Validation: Because there is only ONE consumer, we can safely
	// write to a standard, non-thread-safe Go map without any mutexes!
	consumedTracker := make(map[string]bool)
	consumerDone := make(chan struct{})
	duplicateFound := false
	totalConsumedCount := 0

	go func() {
		defer close(consumerDone)
		for totalConsumedCount < totalExpectedItems {
			val := p.Get()
			if val != nil {
				item := val.(*TestItem)
				key := fmt.Sprintf("p%d-s%d", item.ProducerID, item.SequenceID)

				if consumedTracker[key] {
					t.Errorf("Critical Error: Duplicate item delivery detected for %s", key)
					duplicateFound = true
				}
				consumedTracker[key] = true
				totalConsumedCount++
			} else {
				// The queue is lock-free and non-blocking. If it's temporarily empty
				// because producers are context-switched out, yield the CPU.
				runtime.Gosched()
			}
		}
	}()

	// Wait for all producers to finish executing their Puts
	wg.Wait()

	// Wait for the single consumer thread to completely drain the shards
	<-consumerDone

	if duplicateFound {
		t.FailNow()
	}

	if totalConsumedCount != totalExpectedItems {
		t.Errorf("Data loss! Expected to consume %d items, but only processed %d",
			totalExpectedItems, totalConsumedCount)
	}

	if len(consumedTracker) != totalExpectedItems {
		t.Errorf("Unique item mismatch. Expected %d unique items, tracked %d",
			totalExpectedItems, len(consumedTracker))
	}
}

// 5. Runtime GC Cleanup Test
// Validates that when a standard Garbage Collection cycle occurs, the pool's
// local sharded structures are cleared just like standard library's sync.Pool.
func TestPool_GarbageCollectionCleanup(t *testing.T) {
	p := &Pool{}

	// Seed the pool with an item
	p.Put(&TestItem{ProducerID: 99, SequenceID: 99})

	// Force a full synchronous STW Garbage Collection cycle.
	// This invokes the registered poolCleanup() hook via runtime_registerPoolCleanup.
	runtime.GC()

	// The local shard reference must now be nil, and the item dropped.
	val := p.Get()
	if val != nil {
		t.Errorf("Expected pool to be flushed entirely by the GC cycle, but received: %v", val)
	}
}
