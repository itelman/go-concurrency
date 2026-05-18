package chanpool

import (
	"sync"
	"testing"
)

type TestObject struct {
	ID int
}

func TestPool_Basic(t *testing.T) {
	p := NewPool(10, func() any {
		return &TestObject{ID: -1}
	})

	// 1. Initial Get should trigger allocation via New()
	obj1 := p.Get().(*TestObject)
	if obj1.ID != -1 {
		t.Fatalf("Expected ID -1, got %d", obj1.ID)
	}

	// 2. Modify and Put back
	obj1.ID = 42
	p.Put(obj1)

	// 3. Retrieve again, should get the recycled object
	obj2 := p.Get().(*TestObject)
	if obj2.ID != 42 {
		t.Fatalf("Expected recycled object with ID 42, got %d", obj2.ID)
	}
}

func TestPool_Concurrency(t *testing.T) {
	const workers = 20
	const itemsPerWorker = 1000
	const totalExpected = workers * itemsPerWorker

	// We return nil for New() so the consumer knows when the pool is empty
	// without artificially generating new items during the drain phase.
	p := NewPool(totalExpected, func() any { return nil })

	var wg sync.WaitGroup

	// Multiple Producers (calling Put concurrently)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				p.Put(&TestObject{ID: workerID})
			}
		}(i)
	}

	// Single Consumer (calling Get concurrently with Puts)
	collectedCount := 0
	doneConsumer := make(chan struct{})

	go func() {
		// Spin until we've collected exactly what the producers pushed.
		// This simulates true MPSC concurrency.
		for collectedCount < totalExpected {
			if obj := p.Get(); obj != nil {
				collectedCount++
			}
		}
		close(doneConsumer)
	}()

	wg.Wait()      // Wait for all producers to finish
	<-doneConsumer // Wait for the consumer to finish draining

	if collectedCount != totalExpected {
		t.Errorf("Expected to collect %d items, but got %d", totalExpected, collectedCount)
	}
}
