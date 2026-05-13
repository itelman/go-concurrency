## Our Implementation of Go/Concurrency Task by 7G

The main idea of `sync.Pool` is to solve the Heap Reallocation problem: too many short-lived
allocations create garbage collector (GC) pressure in high‑throughput Go programs.
Instead of allocating a new object every time and letting the GC clean it up,
you can reuse objects from a pool of temporaries.

For this Task:
- `Get()` is called from one goroutine: no need for sync or atomic operations here
- `Put()` is called from several goroutines: need write sync by mutex

Standard `sync.Pool` might not be ideal since it assumes `Get()` is called concurrently across multiple threads.
To achieve this, `sync.Pool` creates a local lock-free ring buffer for each Processor (P).
If a goroutine on one P tries to `Get()` an item but its local buffer is empty,
it uses a complex "work-stealing" algorithm to lock and steal objects from the buffers of other Ps.

In our Multi-Producer, Single-Consumer (MPSC) scenario, this built-in per-P architecture and work-stealing overhead
is entirely unnecessary and suboptimal. Because `Get()` is strictly called by a single goroutine, we can completely
bypass atomic operations and work-stealing during retrieval.

We can maintain nonsync local slice for the consumer, and a mutex-protected shared slice for the producers.
When the local slice is empty, the single consumer simply acquires the mutex and performs an $O(1)$ swap of the local
and shared slices, instantly refilling its supply with near-zero overhead.

### Project Structure and Execution


`go test -v`: Test inside the mutexpool directory

`go test -bench=. -benchmem`: Do benchmarking in the root directory