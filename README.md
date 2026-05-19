# digital.vasic.eventbus

A generic, reusable Go module for publish/subscribe event-driven communication with typed events, glob/prefix/metadata filtering, and pluggable middleware. Round-245 deep-doc + paired-mutation challenge enrichment.

## Installation

```bash
go get digital.vasic.eventbus
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "digital.vasic.eventbus/pkg/bus"
    "digital.vasic.eventbus/pkg/event"
    "digital.vasic.eventbus/pkg/filter"
    "digital.vasic.eventbus/pkg/middleware"
)

func main() {
    b := bus.New(nil) // uses default config
    defer b.Close()

    b.Use(middleware.Enrich("service", "my-app"))

    sub := b.Subscribe("user.created")
    defer sub.Cancel()

    b.Publish(event.New("user.created", "auth-service", map[string]string{
        "userID": "123",
    }))

    select {
    case e := <-sub.Channel:
        fmt.Printf("Received: %s from %s\n", e.Type, e.Source)
    case <-time.After(time.Second):
        fmt.Println("Timeout")
    }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    go b.Publish(event.New("order.completed", "order-service", nil))
    e, _ := b.Wait(ctx, "order.completed")
    fmt.Printf("Order completed: %s\n", e.ID)
}
```

## Features

- **Typed events** with dot-notation topics (e.g., `provider.health.changed`)
- **Flexible filtering**: by type, source, glob pattern, prefix, metadata
- **Filter combinators**: `And`, `Or`, `Not` for composite filter logic
- **Middleware chain**: logging, metrics, enrichment, rate limiting
- **Multiple subscription modes**: single type, multiple types, all events
- **Async publishing** with `PublishAsync`
- **Wait / WaitMultiple** for blocking event consumption
- **Metrics tracking**: published, delivered, dropped, subscriber counts
- **Thread-safe** concurrent publish/subscribe (race-tested)
- **Graceful shutdown** with `Close()`
- **Bounded subscriber buffers** with configurable overflow policy (drop after `PublishTimeout`)

## Packages

| Package | Description |
|---------|-------------|
| `pkg/event` | Core `Event`, `Type`, `Subscription`, `Handler` |
| `pkg/bus` | `EventBus` implementation, `Config`, `Metrics` |
| `pkg/filter` | `Filter` predicate type + `BySource`, `ByGlob`, `ByPrefix`, `ByMetadata`, `And`, `Or`, `Not` |
| `pkg/middleware` | Middleware chain: `LoggingFunc`, `MetricsCounter`, `Enrich`, `RateLimit`, `Chain` |

## Filtering

```go
sub := b.SubscribeWithFilter("user.created", filter.BySource("auth-service"))
sub := b.SubscribeAllWithFilter(filter.ByGlob("provider.*"))
sub := b.SubscribeAllWithFilter(filter.ByPrefix("cache."))

f := filter.And(
    filter.ByPrefix("provider."),
    filter.Not(filter.BySource("internal")),
)
sub := b.SubscribeAllWithFilter(f)
```

## Middleware

```go
b.Use(middleware.LoggingFunc(log.Printf))

mc := middleware.NewMetricsCounter()
b.Use(mc.Middleware())
fmt.Println(mc.GetTotal())

b.Use(middleware.Enrich("env", "production"))
b.Use(middleware.RateLimit(1000)) // max 1000 events/second

b.Use(middleware.Chain(
    middleware.LoggingFunc(log.Printf),
    middleware.Enrich("version", "v1"),
))
```

## Configuration

```go
cfg := &bus.Config{
    BufferSize:      1000,                  // per-subscriber channel capacity
    PublishTimeout:  10 * time.Millisecond, // drop event if subscriber full
    CleanupInterval: 30 * time.Second,      // dead-subscriber GC interval
    MaxSubscribers:  100,                   // soft cap per event type
}
b := bus.New(cfg)
```

`bus.DefaultConfig()` returns the above defaults. `bus.New(nil)` is equivalent.

## Metrics

```go
m := b.Metrics()
// m.EventsPublished, m.EventsDelivered, m.EventsDropped,
// m.SubscribersActive, m.SubscribersTotal
```

`Dropped` increments when a subscriber's channel is full and the `PublishTimeout` elapses — non-blocking-publish guarantee. Use this counter as the back-pressure signal in your monitoring.

## Anti-bluff guarantees (round-245)

- `make test` runs every package's unit + integration + e2e + security + stress suite with `-race -p 1` and ZERO `t.Skip()` without a `SKIP-OK: #<ticket>` marker.
- `challenges/scripts/eventbus_describe_challenge.sh` is paired-mutation aware (`--anti-bluff-mutate` exits 99 to prove the gate actually detects a planted violation).
- Bilingual fixtures (`tests/fixtures/i18n/`) exercise non-ASCII event payloads + metadata to prove `Type` and `Metadata` round-trip through UTF-8 without corruption.
- `docs/test-coverage.md` enumerates every public symbol with its test sources and per-symbol coverage status — drift between the file and `go test -cover` is treated as a CONST-035 / Article XI §11.9 bluff.

## Documentation

- `docs/API_REFERENCE.md` — every exported symbol, package-by-package
- `docs/ARCHITECTURE.md` — internal design, locking model, fan-out
- `docs/USER_GUIDE.md` — task-oriented recipes (filters, middleware composition)
- `docs/test-coverage.md` — round-245 coverage ledger (symbol → test source)
- `docs/CHANGELOG.md` — release history

## Build & Test

```bash
make build          # go build ./...
make test           # -race -p 1 across all packages
make test-coverage  # generates coverage.out + coverage.html
make fmt vet lint   # formatter, vet, golangci-lint
```

## Constitutional anchors

EventBus inherits Article XI §11.9 (anti-bluff), CONST-035 (zero-bluff), CONST-047 (recursive submodule application), CONST-048 (full-automation-coverage), CONST-050 (no-fakes-beyond-unit-tests + 100%-test-type-coverage), CONST-051 (submodules-as-equal-codebase + decoupling) from the constitution submodule. See `CONSTITUTION.md`, `CLAUDE.md`, `AGENTS.md` for the verbatim mandates.

## License

See the parent project for licensing terms.
