package pool

// Pool is a custom object pool tailored for single-consumer (Get)
// and multi-producer (Put) execution patterns.
type Pool struct {
	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	New func() any

	// queue acts as a concurrent lock-free buffer between producers and the consumer.
	queue chan any
}

// NewPool initializes the pool with a fixed maximum capacity for recycled items.
func NewPool(capacity int, allocFunc func() any) *Pool {
	return &Pool{
		New:   allocFunc,
		queue: make(chan any, capacity),
	}
}

// Get selects an item from the Pool.
// This MUST be called from a single, dedicated goroutine to maintain optimal speed.
func (p *Pool) Get() any {
	select {
	case item := <-p.queue:
		return item
	default:
		// If the queue is empty, allocate a new object fallback
		if p.New != nil {
			return p.New()
		}
		return nil
	}
}

// Put adds an item to the pool. It safely allows concurrent calls from multiple goroutines.
func (p *Pool) Put(x any) {
	if x == nil {
		return
	}

	select {
	case p.queue <- x:
		// Successfully recycled
	default:
		// Buffer is full; discard the object to avoid blocking producer goroutines and let GC handle it
	}
}
