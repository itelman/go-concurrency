package mutexpool

import (
	"sync"
	"testing"
)

func TestPool_Basic(t *testing.T) {
	p := &Pool{
		New: func() any { return "item" },
	}

	// New() test
	if val := p.Get(); val != "item" {
		t.Fatalf("expected `item`, got `%s`", val)
	}

	// Put() test
	p.Put("another")
	if val := p.Get(); val != "another" {
		t.Fatalf("expected `another`, got `%s`", val)
	}
}

func TestPool_Concurrency(t *testing.T) {
	p := &Pool{}
	var wg sync.WaitGroup

	N := 100 // multiple Put goroutines
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			p.Put(val)
		}(i)
	}
	wg.Wait()

	count := 0 // single Get goroutine
	for p.Get() != nil {
		count++
	}
	if count != N {
		t.Fatalf("Expected %d items, got %d", N, count)
	}
}
