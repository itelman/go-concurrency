# Our Implementation of Go/Concurrency Task by 7G

The main idea of `sync.Pool` is to solve the Heap Reallocation problem: too many short‑lived allocations create garbage collector (GC) pressure in high‑throughput Go programs.
Instead of allocating a new object every time and letting the GC clean it up, you can reuse objects from a pool of temporaries.

