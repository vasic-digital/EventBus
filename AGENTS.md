# AGENTS.md - EventBus Module

## Module Overview

`digital.vasic.eventbus` is a generic, reusable Go module providing publish/subscribe event-driven communication. It is designed for in-process event routing with typed events, topic-based filtering (including glob patterns), composable middleware chains, and built-in metrics tracking. The module has zero external runtime dependencies beyond `github.com/google/uuid`.

**Module path**: `digital.vasic.eventbus`
**Go version**: 1.24+
**Dependencies**: `github.com/google/uuid` (runtime), `github.com/stretchr/testify` (test only)

## Package Responsibilities

| Package | Path | Responsibility |
|---------|------|----------------|
| `event` | `pkg/event/` | Core types: `Event` struct, `Type` alias, `Handler` function type, `Subscription` with channel-based delivery and cancellation. This is the foundational package with no internal dependencies. |
| `bus` | `pkg/bus/` | `EventBus` implementation: publish/subscribe orchestration, subscriber lifecycle management, middleware execution pipeline, metrics collection, periodic cleanup of dead subscribers, graceful shutdown. Depends on `event`, `filter`, `middleware`. |
| `filter` | `pkg/filter/` | Stateless filter functions: `ByType`, `BySource`, `ByGlob`, `ByPrefix`, `ByMetadata`, `HasMetadata`, plus combinators `And`, `Or`, `Not`, `All`, `None`. Depends only on `event`. |
| `middleware` | `pkg/middleware/` | Event transformation pipeline: `Logging`, `LoggingFunc`, `MetricsCounter`, `Enrich`, `Timestamp`, `Recover`, `RateLimit`, `Chain`. Depends only on `event`. |

## Dependency Graph

```
bus  --->  event
bus  --->  filter  --->  event
bus  --->  middleware  --->  event
```

`event` is the leaf package. `filter` and `middleware` depend only on `event`. `bus` depends on all three.

## Key Files

| File | Purpose |
|------|---------|
| `pkg/event/event.go` | Event, Type, Handler, Subscription types; New() constructor; builder methods |
| `pkg/bus/bus.go` | EventBus struct, Config, Metrics, all pub/sub methods, lifecycle management |
| `pkg/filter/filter.go` | All filter constructors and combinators |
| `pkg/middleware/middleware.go` | All middleware constructors, MetricsCounter, Chain |
| `pkg/event/event_test.go` | Event package unit tests |
| `pkg/bus/bus_test.go` | Bus package unit tests (concurrent publish, close semantics, middleware integration) |
| `pkg/filter/filter_test.go` | Filter package unit tests |
| `pkg/middleware/middleware_test.go` | Middleware package unit tests |
| `go.mod` | Module definition and dependencies |
| `CLAUDE.md` | AI coding assistant instructions |
| `README.md` | User-facing documentation with quick start |

## Agent Coordination Guide

### Division of Work

When multiple agents work on this module simultaneously, divide work by package boundary:

1. **Event Agent** -- Owns `pkg/event/`. Changes to core types affect all other packages. Must coordinate with all other agents before modifying the `Event` struct or `Subscription` interface.
2. **Bus Agent** -- Owns `pkg/bus/`. This is the integration layer. Changes here rarely affect other packages but require testing against all feature combinations.
3. **Filter Agent** -- Owns `pkg/filter/`. New filters can be added independently. Only constraint: filter signature must remain `func(*event.Event) bool`.
4. **Middleware Agent** -- Owns `pkg/middleware/`. New middleware can be added independently. Only constraint: middleware signature must remain `func(*event.Event) *event.Event`.

### Coordination Rules

- **Event struct changes** require all agents to update. The `Event` struct is the shared data contract.
- **Filter and middleware** packages are independent of each other and can be modified in parallel without conflict.
- **Bus package** integrates all packages. Any interface change in `filter` or `middleware` requires a corresponding bus update.
- **Test isolation**: Each package has its own `_test.go` file. Bus tests import `filter` and `middleware` for integration scenarios.
- **No circular dependencies**: The dependency graph is strictly acyclic. Never import `bus` from `event`, `filter`, or `middleware`.

### Safe Parallel Changes

These changes can be made simultaneously without coordination:
- Adding a new filter function to `pkg/filter/`
- Adding a new middleware to `pkg/middleware/`
- Adding new tests to any package
- Updating documentation

### Changes Requiring Coordination

- Modifying the `Event` struct fields
- Changing `Filter` or `Middleware` type signatures
- Modifying `EventBus` publish/subscribe flow
- Changing `Config` defaults or adding new config fields

## Build and Test Commands

```bash
# Build all packages
go build ./...

# Run all tests with race detection
go test ./... -count=1 -race

# Run unit tests only (short mode)
go test ./... -short

# Run integration tests
go test -tags=integration ./...

# Run benchmarks
go test -bench=. ./tests/benchmark/

# Run a specific test
go test -v -run TestEventBus_Publish ./pkg/bus/

# Format code
gofmt -w .

# Vet code
go vet ./...
```

## Commit Conventions

Follow Conventional Commits with package scope:

```
feat(event): add Priority field to Event struct
feat(filter): add ByPayloadType filter
feat(middleware): add deduplication middleware
fix(bus): prevent race condition in cleanup loop
test(bus): add concurrent unsubscribe test
docs(eventbus): update API reference
refactor(middleware): extract rate limiter to separate struct
```

## Thread Safety Notes

- `EventBus` is fully thread-safe. All public methods use `sync.RWMutex` or `atomic` operations.
- `subscriber.trySend` uses `RLock` for the closed check and a timeout-based channel send.
- `MetricsCounter` uses `atomic.AddInt64` / `atomic.LoadInt64` for lock-free counting.
- `RateLimit` middleware uses atomic operations for the rolling window counter.
- Filters and middleware functions must be safe for concurrent invocation (they are called from multiple goroutines via `Publish`).
