# Distributed-task-engine

A production-grade worker pool engine written in Go — with dynamic autoscaling, backpressure, per-job timeouts, graceful shutdown, and retry with exponential backoff.

Built from scratch using only the Go standard library. No external dependencies.

---

## Why this exists

Most worker pool examples give you a fixed goroutine count and a `Submit(func())`. Real production systems need more:

- **Backpressure** — when the system is overloaded, the caller should know, not have work silently dropped
- **Dynamic scaling** — worker count should grow under load and shrink when idle
- **Per-job timeouts** — one hung job must never freeze a worker forever
- **Retry with backoff** — transient failures should retry automatically, with jitter to prevent thundering herd
- **Graceful shutdown** — in-flight jobs must complete before the pool exits, with no goroutine leaks and no panics

This project implements all of the above from scratch, with the reasoning documented for each design decision.

---

## Architecture

```
                          ┌──────────────────────────────────────────┐
                          │           distributed-task-engine        │
                          │                                          │
  Caller                  │    ┌─────────────────────────────┐       │
  ───────                 │    │   Job Channel (buffered)    │       │
  Submit(job) ──────────► │    │   provides backpressure     │       │
  TrySubmit(job) ───────► │    └──────────────┬──────────────┘       │
                          │                   │                      │
                          │      ┌────────────▼────────────┐         │
                          │      │       Worker Pool       │         │
                          │      │  ┌─────┐ ┌─────┐        │         │
                          │      │  │ W1  │ │ W2  │  ...   │         │
                          │      │  └─────┘ └─────┘        │         │
                          │      │  Min ←→ Max workers     │         │
                          │      └────────────┬────────────┘         │
                          │                   │                      │
        ┌─────────────┐   │    ┌──────────────▼──────────────┐       │
        │ Autoscaler  │───┼──► │   Results Channel           │ ────► Results()
        │ (background)│   │    │   (non-blocking writes)     │       │
        └─────────────┘   │    └─────────────────────────────┘       │
                          │                                          │
                          │    ┌─────────────────────────────┐       │
                          │    │  retry.Wrap() middleware    │       │
                          │    │  exponential backoff+jitter │       │
                          │    └─────────────────────────────┘       │
                          └──────────────────────────────────────────┘
```

---

## Features

### ✅ Worker Pool (`worker-pool/`)

- **Fixed + dynamic worker count** — starts with `MinWorkers`, autoscales up to `MaxWorkers` based on queue pressure
- **Buffered job queue with backpressure** — `Submit()` blocks when full; `TrySubmit()` returns `ErrPoolFull` immediately for load-shedding
- **Per-job timeout** — each job runs against a `context.WithTimeout`; pool-level default with per-job override
- **Idle worker retirement** — workers above the minimum retire themselves after `IdleTimeout` of no work
- **Graceful shutdown** — `Shutdown()` drains all in-flight jobs, waits via `sync.WaitGroup`, then closes channels in panic-safe order
- **Race-safe state** — `atomic.Bool` for the stopped flag with `CompareAndSwap` for idempotent shutdown, `atomic.Int64` for the hot-path worker counter, `recover()` guards against the close-channel race in submit paths

### ✅ Retry Middleware (`retry/`)

- **Pure function wrapper** — `retry.Wrap(task, policy)` returns a new task with retry built in; the pool never knows retrying is happening
- **Exponential backoff** — delay grows by `Multiplier` each attempt, capped at `MaxDelay`
- **Jitter** — ±N% randomness on every delay to prevent thundering herd when many jobs retry simultaneously
- **Context-aware** — never retries a cancelled or deadline-exceeded context; cancellation during a backoff wait exits immediately

---

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/swag2716/distributed-task-engine/retry"
    pool "github.com/swag2716/distributed-task-engine/worker-pool"
)

func main() {
    p := pool.NewPool(pool.Config{
        MinWorkers:    3,
        MaxWorkers:    10,
        QueueSize:     100,
        JobTimeout:    5 * time.Second,
        ScaleInterval: 2 * time.Second,
        IdleTimeout:   30 * time.Second,
    })

    // drain results concurrently
    done := make(chan struct{})
    go func() {
        defer close(done)
        for res := range p.Results() {
            if res.Err != nil {
                fmt.Printf("[FAIL] %s: %v\n", res.JobId, res.Err)
            } else {
                fmt.Printf("[OK]   %s: %v (%v)\n", res.JobId, res.Output, res.Latency)
            }
        }
    }()

    // a task with retry: 3 attempts, 100ms → 200ms backoff with ±20% jitter
    task := retry.Wrap(
        func(ctx context.Context, payload any) (any, error) {
            // your real work here — HTTP call, DB write, etc.
            return fmt.Sprintf("processed %v", payload), nil
        },
        retry.Policy{
            MaxAttempts: 3,
            BaseDelay:   100 * time.Millisecond,
            MaxDelay:    2 * time.Second,
            Multiplier:  2.0,
            Jitter:      0.2,
        },
    )

    p.Submit(pool.Job{Id: "job-1", Payload: "user-42", Task: task})

    p.Shutdown() // waits for all in-flight jobs
    <-done       // waits for result reader
}
```

---

## API overview

| Method | Behaviour |
|---|---|
| `NewPool(cfg)` | Creates pool, spawns `MinWorkers` goroutines, starts autoscaler |
| `Submit(job)` | Enqueues a job; **blocks** if queue is full (backpressure) |
| `TrySubmit(job)` | Enqueues without blocking; returns `ErrPoolFull` if at capacity |
| `Results()` | Read-only channel of completed `Result`s |
| `Shutdown()` | Rejects new jobs, drains in-flight work, closes channels safely; idempotent |
| `retry.Wrap(task, policy)` | Returns a task with exponential-backoff retry built in |

---

## Design decisions

| Decision | Why |
|---|---|
| `chan Job` over mutex-protected queue | Channels give blocking semantics, `select` composability, and goroutine-safe access for free |
| `atomic` counters on the hot path | ~5ns vs ~25ns for a mutex; the worker counter is read on every autoscaler tick and written on every spawn/retire |
| `CompareAndSwap` for shutdown guard | A `Load`-then-`Store` has a race window where two goroutines both pass the check and double-close the channel (panic) |
| Non-blocking result writes | If the caller isn't draining results, workers should keep processing rather than stall — throughput over delivery guarantee |
| Counter increments **before** goroutine launch | Incrementing inside the goroutine creates a window where the autoscaler sees a stale count and over-spawns |
| Timer drain before `Reset` | A timer that fires while the worker is mid-job leaves a stale value in `.C`; without draining, the worker incorrectly retires the moment it returns to `select` |
| Retry as middleware, not pool logic | `Wrap` keeps the pool ignorant of retry entirely — separation of concerns, and any task function can opt in independently |
| Jitter on backoff delays | 1000 jobs failing together would otherwise retry at the exact same instant and hammer the recovering service again (thundering herd) |
| `recover()` in submit paths | There is an unavoidable race between the stopped-check and the channel send during shutdown; recover converts the panic into a clean `ErrPoolStopped` |

---

## Running tests

```bash
# all tests
go test ./... -v

# with race detector (always run this — it catches concurrency bugs)
go test ./... -v -race
```

Tests cover: basic submit/result flow, graceful shutdown draining all in-flight jobs, per-job timeout enforcement, backpressure (`ErrPoolFull`), and post-shutdown rejection (`ErrPoolStopped`).

---

## Roadmap

Planned next — in order:

- [ ] **Token bucket rate limiter** (`ratelimiter/`) — throttle job submission rate (e.g. max 100 jobs/sec with burst capacity), using lazy token refill with no background goroutine
- [ ] **Non-blocking requeue retry** — instead of a worker sleeping through backoff, failed jobs re-enter the queue after their delay via `time.AfterFunc`, freeing the worker immediately (the Sidekiq/Kafka retry-topic model); requires a second WaitGroup and reordered shutdown to handle jobs in limbo between queue and worker
- [ ] **Metrics collector** (`metrics/`) — atomic counters and bucketed latency histograms (p50/p95/p99) exposed via a Prometheus-compatible `/metrics` HTTP endpoint, built from scratch with no client library
- [ ] Benchmarks — throughput at varying worker counts (`go test -bench`), documenting the fixed-vs-dynamic overhead tradeoff

---

## Project structure

```
distributed-task-engine/
├── worker-pool/
│   ├── pool.go          # Pool, Job, Result, Config — core engine
│   └── pool_test.go     # unit tests
├── retry/
│   └── retry.go         # Wrap() middleware + Policy
├── main.go              # runnable demo
├── go.mod
└── README.md
```

---

## License

MIT