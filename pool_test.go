package pool

import (
	"sync"
	"testing"
)

type TestObject struct {
	ID int
}

func TestPool_BasicGetPut(t *testing.T) {
	p := NewPool(10, func() any {
		return &TestObject{ID: -1}
	})

	// Initial Get should trigger allocation
	obj1 := p.Get().(*TestObject)
	if obj1.ID != -1 {
		t.Errorf("Expected ID -1, got %d", obj1.ID)
	}

	// Modify and Put back
	obj1.ID = 42
	p.Put(obj1)

	// Retrieve again
	obj2 := p.Get().(*TestObject)
	if obj2.ID != 42 {
		t.Errorf("Expected recycled object with ID 42, got %d", obj2.ID)
	}
}

func TestPool_ConcurrencyAndRace(t *testing.T) {
	const workers = 20
	const itemsPerWorker = 1000

	p := NewPool(workers*itemsPerWorker, func() any {
		return &TestObject{ID: 0}
	})

	var wg sync.WaitGroup

	// Simulating multiple producer goroutines calling Put
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				p.Put(&TestObject{ID: workerID})
			}
		}(i)
	}

	// Single consumer goroutine calling Get
	collectedCount := 0
	doneConsumer := make(chan bool)

	go func() {
		totalExpected := workers * itemsPerWorker
		for collectedCount < totalExpected {
			obj := p.Get()
			if obj != nil {
				collectedCount++
			}
		}
		doneConsumer <- true
	}()

	wg.Wait() // Wait for all Puts to finish
	<-doneConsumer

	if collectedCount != workers*itemsPerWorker {
		t.Errorf("Expected to collect %d items, but got %d", workers*itemsPerWorker, collectedCount)
	}
}
