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
`mpscpool` is an ultra-high performance, lock-free, CAS-free object pool for Go. It is optimized specifically for **Multi-Producer, Single-Consumer (MPSC)** workflows.

By leveraging Go runtime processor pinning (`runtime_procPin`) and a sharded architecture, it avoids global locks, central rings, and Compare-And-Swap (CAS) retry loops completely on the producer path. This provides deterministic, wait-free execution even under extreme thread contention.

## 🚀 Performance Characteristics
- **Producers (`Put`):** Completely **Wait-Free**. Under high contention, it out-performs standard library `sync.Pool` and atomic-CAS ring buffers because producers never loop, never block, and write exclusively to separate, CPU-sharded memory segments.
- **Consumer (`Get`):** Highly localized, cache-friendly array scans with no central mutex tracking.
- **False Sharing Protection:** Struct fields and array segments are protected by strict 64-byte cache-line padding boundaries.
- **GC Integration:** Integrates directly with Go's internal Garbage Collector hooks (`sync.runtime_registerPoolCleanup`). The pool automatically flushes cached entries during Stop-The-World (STW) GC cycles to prevent memory bloat, matching `sync.Pool` behavior.

## ⚠️ The Architectural Contract (Crucial)

To achieve its lock-free speeds, `mpscpool` enforces a strict architectural invariant:

1. **`Put(any)`:** Can be called concurrently by **unlimited, arbitrary numbers of goroutines/producers**.
2. **`Get() any`:** Must **ONLY be called by ONE goroutine at a time**.

> **Warning:** Calling `Get()` from multiple concurrent consumer goroutines without outer synchronization will violate memory guarantees, cause data corruption, or result in dropped items.
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