# Custom `sync.Pool` Implementation

A highly optimized, thread-safe object pool implementation tailored specifically for asymmetric concurrency patterns.
This project presents two implementation alternatives to `sync.Pool` under specific thread-allocation constraints.

## Overview

In the Go standard library, `sync.Pool` is the go-to utility for reusing allocated memory and reducing Garbage
Collection (GC) pressure.
However, because `sync.Pool` is built to be a one-size-fits-all generic solution, it introduces synchronization
compromises to accommodate arbitrary multithreaded usage.

This repository provides `Pool` - a specialized object pool optimized explicitly for scenarios where resource
retrieval (`Get`) and resource recycling (`Put`) are entirely asymmetric.

---

## The Asymmetric Scenario

This library is designed for architectures that match the following execution flow:
* **Single-Consumer (`Get`):** All calls to `.Get()` happen sequentially within **one unique goroutine**
(e.g., a central event loop, a single network reader, or a main worker router).
* **Multi-Producer (`Put`):** Calls to `.Put()` are executed concurrently from **multiple independent background
goroutines** (e.g., worker threads returning processed buffers or network packets back to the pool).

---

## Why `sync.Pool` is Suboptimal Here

The standard `sync.Pool` maintains a two-tiered caching layer for every CPU core (`P`): a `private` slot and a `shared` lockless deque.

When your architecture forces a single goroutine to do all the `Get` requests:
1. The consumer goroutine quickly exhausts its own thread-local `private` cache.
2. It is then forced into a "work-stealing" routine—performing expensive atomic Compare-And-Swap (CAS) operations to
steal objects recycled by other goroutines on different CPU cores.
3. If stealing fails, it drops into a slow-path mutex lock or allocates entirely new objects, which triggers runtime
GC overhead.

---

## Architecture & Optimization Strategy

`Pool` drops the overhead of work-stealing, cross-thread cache thrashing, and runtime-hook cleanups by utilizing a highly tuned ring-buffer channel.

* **$O(1)$ Lockless Get:** Because a single goroutine is pulling items sequentially, reading from the buffered channel takes a direct, lock-free execution path.
* **Non-blocking Puts:** Multiple background goroutines can rapidly push items back into the queue concurrently.
If the pool capacity is saturated, extra items are safely discarded to avoid blocking performance-critical worker
threads, leaving them to be cleaned up naturally by the GC.
* **Zero-Allocation Lifecycles:** Unlike `sync.Pool`, which automatically drops all cached items during every global
Garbage Collection cycle, `Pool` retains its underlying hot-cache elements across GC intervals, drastically cutting allocation spikes.

---

## Getting Started

### Installation

Clone this repository directly into your project structure:

```bash
git clone https://github.com/itelman/go-concurrency.git
```

## Running Benchmarks & Tests

This repository enforces strict thread-safety validation and comprehensive performance tracking.

### Run Unit & Stress Tests

To confirm correct pool behavior and guarantee no race conditions under high concurrent stress,
run the tests in each implementation directory with:

```bash
go test -v -race ./mutexpool/
go test -v -race ./chanpool/
```

### Run Performance Benchmarks

To compare the throughput, execution speed, and allocation metrics of our implementations against `sync.Pool`,
run the common benchmark file from the project root directory:

```bash
go test -bench=. -benchmem
```

---