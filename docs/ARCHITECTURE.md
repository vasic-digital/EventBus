# EventBus Architecture

## Design Goals

1. **Simplicity** -- Provide a minimal, easy-to-understand pub/sub API that covers common event-driven use cases without over-engineering.
2. **Decoupling** -- Publishers and subscribers know nothing about each other. Communication happens through typed event topics.
3. **Composability** -- Filters and middleware are plain functions that compose naturally with standard functional patterns (And/Or/Not, Chain).
4. **Thread Safety** -- All operations are safe for concurrent use from multiple goroutines.
5. **Zero External Runtime Dependencies** -- The only runtime dependency is `github.com/google/uuid` for event ID generation.
6. **Reusability** -- The module is generic with no application-specific types, making it usable in any Go project.

## Package Dependency Diagram

```
+-----------+
|    bus    |
+-----------+
  |    |    |
  |    |    +-------------------+
  |    |                        |
  v    v                        v
+--------+    +----------+    +-----------+
| filter |    |middleware |    |   event   |
+--------+    +----------+    +-----------+
  |                |                ^
  |                |                |
  +----------------+----------------+
                   |
                   v
            (github.com/google/uuid)
```

- `event` is the foundational package. It defines the shared data types (`Event`, `Type`, `Handler`, `Subscription`) used by all other packages.
- `filter` depends only on `event`. It provides stateless predicate functions.
- `middleware` depends only on `event`. It provides stateless transformation functions.
- `bus` depends on all three packages. It is the orchestration layer that ties everything together.

There are no circular dependencies. The dependency graph forms a strict DAG (directed acyclic graph).

## Design Patterns

### Observer Pattern

The core pub/sub mechanism implements the Observer pattern. Subscribers register interest in event types, and the bus notifies all matching subscribers when an event is published.

**Why**: The Observer pattern is the natural fit for event-driven systems. It provides loose coupling between event producers and consumers while supporting one-to-many notification.

**Implementation**: Subscribers are stored in two collections:
- `subscribers map[event.Type][]*subscriber` -- Type-specific subscribers, looked up by exact event type.
- `allSubs []*subscriber` -- Global subscribers that receive every event regardless of type.

When an event is published, the bus iterates both the type-specific slice and the global slice, delivering to each matching subscriber.

### Mediator Pattern

The `EventBus` struct acts as a Mediator, centralizing communication between components. No component talks to another directly; all communication flows through the bus.

**Why**: In systems with many interacting components, direct communication creates tight coupling and an N-to-N dependency graph. The Mediator centralizes this into a star topology where each component depends only on the bus.

**Implementation**: The `EventBus` struct holds all subscriber registrations and is the single point through which events flow. Publishers call `Publish()` on the bus; subscribers receive events from channels obtained via `Subscribe()`.

### Middleware Chain Pattern

The middleware system implements a sequential processing pipeline. Each middleware receives an event, optionally transforms it, and passes it forward. Returning `nil` short-circuits the chain and drops the event.

**Why**: Middleware chains provide a clean separation of cross-cutting concerns (logging, metrics, enrichment, rate limiting) from core event routing logic. New behaviors can be added without modifying existing code.

**Implementation**: Middleware are stored as a slice of `func(*event.Event) *event.Event`. During `Publish()`, the bus iterates the slice in order, passing the event through each middleware. If any middleware returns `nil`, the event is dropped and no subscribers are notified.

The `Chain()` combinator composes multiple middleware into a single function, which can itself be passed to `Use()`.

### Builder Pattern

The `Event` struct uses the Builder pattern with fluent `With*` methods for optional fields.

**Why**: Events have required fields (Type, Source, Payload) set via the `New()` constructor and optional fields (TraceID, Metadata) that vary per use case. The builder pattern avoids constructors with many parameters.

**Implementation**: `WithTraceID()` and `WithMetadata()` return `*Event`, enabling method chaining:
```go
event.New("user.created", "auth", data).
    WithTraceID("trace-123").
    WithMetadata("region", "us-east-1")
```

### Functional Options (Filters)

Filters use the functional pattern where each filter constructor returns a `func(*event.Event) bool`. Combinators (`And`, `Or`, `Not`) compose these functions into more complex predicates.

**Why**: Functional composition is more flexible than interface-based approaches for predicate logic. Users can combine built-in filters or supply custom functions with the same signature.

## Concurrency Model

### Thread Safety Mechanisms

| Component | Mechanism | Purpose |
|-----------|-----------|---------|
| `EventBus.subscribers` | `sync.RWMutex` | Protects subscriber map during subscribe/unsubscribe/publish |
| `EventBus.metrics` | `sync/atomic` | Lock-free metric counter updates |
| `subscriber.closed` | `sync.RWMutex` | Protects against send-on-closed-channel |
| `MetricsCounter.Total` | `sync/atomic` | Lock-free middleware counter |
| `RateLimit` counter | `sync/atomic` | Lock-free rolling window counter |

### Publish Flow

1. Acquire read lock on `EventBus.mu`.
2. Check if bus is closed; if so, return immediately.
3. Copy middleware slice and subscriber slices (to release lock quickly).
4. Release read lock.
5. Apply middleware chain sequentially; if any returns nil, stop.
6. Increment `EventsPublished` atomically.
7. For each matching subscriber, call `trySend()` which attempts a buffered channel send with a timeout.

The copy-on-read approach minimizes lock contention. The read lock is held only long enough to snapshot the subscriber lists. Actual event delivery happens outside the lock.

### Subscriber Channel Semantics

- Subscriber channels are buffered (default 1000 elements).
- `trySend()` uses a timer-based timeout (default 10ms) to avoid blocking indefinitely on full channels.
- If the send times out, the event is counted as dropped.
- This design prevents slow consumers from blocking the entire bus.

### Cleanup Loop

A background goroutine runs periodically (default every 30 seconds) to remove closed subscribers from the internal slices. This prevents memory leaks from cancelled subscriptions that have not been garbage collected.

The cleanup loop is stopped when `Close()` is called via context cancellation.

## Event Lifecycle

```
Publisher                     EventBus                        Subscriber
   |                             |                                |
   |-- Publish(event) ---------> |                                |
   |                             |-- Apply middleware[0] -------> |
   |                             |-- Apply middleware[1] -------> |
   |                             |   (if nil, drop event)         |
   |                             |-- Apply middleware[N] -------> |
   |                             |                                |
   |                             |-- Lookup type subscribers      |
   |                             |-- Lookup global subscribers    |
   |                             |                                |
   |                             |-- Apply subscriber filter ---> |
   |                             |   (if false, skip)             |
   |                             |                                |
   |                             |-- trySend(event, timeout) ---> |
   |                             |                         event --> Channel
   |                             |                                |
```

## Error Handling Strategy

The EventBus follows a "fail silently, track metrics" approach:

- **Nil events**: `Publish(nil)` is a no-op.
- **Closed bus**: Publishing after close is a no-op. Subscribing after close returns a closed channel.
- **Full channels**: Events are dropped after timeout; counted in `EventsDropped`.
- **Middleware panics**: The `Recover` middleware catches panics; without it, panics propagate.
- **Rate limiting**: Excess events are dropped (middleware returns nil).
- **Double close**: `Close()` is idempotent; second call returns nil.

This design prioritizes system stability over event delivery guarantees. For use cases requiring delivery guarantees, consumers should monitor `EventsDropped` metrics and increase buffer sizes or reduce publish rates accordingly.

## Configuration Design

Configuration uses a simple struct (`bus.Config`) with a `DefaultConfig()` factory function. Passing `nil` to `New()` applies defaults. This avoids the complexity of functional options for a small, stable set of configuration parameters.

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `BufferSize` | 1000 | Channel buffer size per subscriber |
| `PublishTimeout` | 10ms | Max time to wait when sending to a subscriber channel |
| `CleanupInterval` | 30s | How often the cleanup goroutine runs |
| `MaxSubscribers` | 100 | Maximum subscribers per event type |

## Why Not Interfaces for Filter and Middleware?

Filters and middleware are defined as function types (`func(*event.Event) bool` and `func(*event.Event) *event.Event`) rather than interfaces. This was a deliberate choice:

1. **Simplicity**: Function types require less boilerplate than interfaces. No need to define a struct, implement a method, and instantiate.
2. **Composability**: Functions compose naturally with higher-order functions (`And`, `Or`, `Not`, `Chain`). Interface composition requires wrapper types.
3. **Inline definitions**: Users can define ad-hoc filters and middleware as closures without creating new types.
4. **Go idiom**: The standard library uses function types extensively for similar purposes (`http.HandlerFunc`, `sort.Slice`).

## Why Channel-Based Delivery?

Subscriptions deliver events through Go channels rather than callback functions. Reasons:

1. **Backpressure**: Channels naturally provide backpressure through buffering and blocking.
2. **Select integration**: Subscribers can use `select` to multiplex event reception with timeouts, cancellation, and other channels.
3. **Go idiom**: Channels are the standard Go mechanism for communicating between goroutines.
4. **Consumer control**: The consumer decides when and how to process events, rather than being called back at arbitrary times.

The tradeoff is that slow consumers can cause event drops when the channel buffer fills. The `EventsDropped` metric and configurable `BufferSize` address this.
