# Architecture -- EventBus

## Purpose

Generic, reusable Go module for publish/subscribe event-driven communication with typed events, dot-notation topics, flexible filtering (type, source, glob, prefix, metadata), composable middleware chains, and metrics tracking.

## Structure

```
pkg/
  event/       Core types: Event, Type, Handler, Subscription with Channel and Cancel
  bus/         EventBus implementation with pub/sub, middleware, and metrics
  filter/      Event filtering: ByType, BySource, ByGlob, ByPrefix, And, Or, Not combinators
  middleware/   Event middleware: Logging, Metrics, Enrichment, RateLimit, Chain
```

## Key Components

- **`event.Event`** -- Event struct with ID, Type, Source, Payload, Timestamp, TraceID, Metadata
- **`bus.EventBus`** -- Central pub/sub manager: Subscribe, SubscribeMultiple, SubscribeAll, Publish, PublishAsync, Wait, WaitMultiple, Close
- **`event.Subscription`** -- Active subscription with a Channel for receiving events and Cancel for cleanup
- **`filter.Filter`** -- `func(*event.Event) bool` for filtering; composable with And/Or/Not
- **`middleware.Middleware`** -- `func(*event.Event) *event.Event` for transformation; chainable

## Data Flow

```
Publisher: bus.Publish(event) -> middleware chain -> for each matching subscription:
    filter.Match(event)? -> subscription.Channel <- event

Subscriber: sub := bus.Subscribe("user.created")
    for event := range sub.Channel { ... }
    defer sub.Cancel()

Middleware: bus.Use(middleware.Chain(
    middleware.LoggingFunc(log.Printf),
    middleware.Enrich("env", "production"),
    middleware.RateLimit(1000),
))
```

## Dependencies

- `github.com/google/uuid` -- UUID generation for event IDs
- `github.com/stretchr/testify` -- Test assertions

## Testing Strategy

Table-driven tests with `testify` and race detection. Tests cover single and multi-type subscriptions, glob and prefix filtering, filter combinators, middleware chaining, async publishing, Wait/WaitMultiple blocking, metrics tracking, and graceful shutdown.
