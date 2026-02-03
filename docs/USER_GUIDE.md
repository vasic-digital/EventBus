# EventBus User Guide

## Installation

```bash
go get digital.vasic.eventbus
```

**Requirements**: Go 1.24 or later.

## Creating an Event Bus

The event bus is the central hub for all event communication. Create one with default configuration or provide custom settings.

### Default Configuration

```go
package main

import "digital.vasic.eventbus/pkg/bus"

func main() {
    b := bus.New(nil) // Uses default config
    defer b.Close()
}
```

Default values: buffer size 1000, publish timeout 10ms, cleanup interval 30s, max 100 subscribers per type.

### Custom Configuration

```go
package main

import (
    "time"

    "digital.vasic.eventbus/pkg/bus"
)

func main() {
    config := &bus.Config{
        BufferSize:      5000,              // Larger buffer for high-throughput
        PublishTimeout:  50 * time.Millisecond, // More time for slow consumers
        CleanupInterval: 10 * time.Second,  // More frequent dead subscriber cleanup
        MaxSubscribers:  200,               // Allow more subscribers per type
    }

    b := bus.New(config)
    defer b.Close()
}
```

## Creating and Publishing Events

Events are the fundamental unit of communication. Each event has a type (dot-notation topic), a source identifier, and an arbitrary payload.

### Basic Event Creation

```go
package main

import (
    "digital.vasic.eventbus/pkg/bus"
    "digital.vasic.eventbus/pkg/event"
)

func main() {
    b := bus.New(nil)
    defer b.Close()

    // Create and publish an event
    e := event.New("user.created", "auth-service", map[string]interface{}{
        "userID":   "usr_123",
        "email":    "user@example.com",
    })

    b.Publish(e)
}
```

### Event Builder Pattern

Events support fluent builder methods for setting trace IDs and metadata:

```go
e := event.New("order.completed", "order-service", orderData).
    WithTraceID("trace-abc-123").
    WithMetadata("region", "us-east-1").
    WithMetadata("priority", "high")
```

### Asynchronous Publishing

`PublishAsync` dispatches the event in a new goroutine, returning immediately to the caller:

```go
b.PublishAsync(event.New("analytics.pageview", "web-frontend", pageData))
```

This is useful when the publisher does not need to wait for delivery confirmation and wants to avoid blocking on slow subscribers.

## Subscribing to Events

### Subscribe to a Single Event Type

```go
package main

import (
    "fmt"
    "time"

    "digital.vasic.eventbus/pkg/bus"
    "digital.vasic.eventbus/pkg/event"
)

func main() {
    b := bus.New(nil)
    defer b.Close()

    sub := b.Subscribe("user.created")
    defer sub.Cancel()

    // Publish from another goroutine
    go b.Publish(event.New("user.created", "auth", nil))

    // Receive events from the channel
    select {
    case e := <-sub.Channel:
        fmt.Printf("User created: id=%s source=%s\n", e.ID, e.Source)
    case <-time.After(5 * time.Second):
        fmt.Println("No event received within timeout")
    }
}
```

### Subscribe to Multiple Event Types

```go
sub := b.SubscribeMultiple("order.created", "order.updated", "order.cancelled")
defer sub.Cancel()

for e := range sub.Channel {
    switch e.Type {
    case "order.created":
        fmt.Println("New order:", e.Payload)
    case "order.updated":
        fmt.Println("Order updated:", e.Payload)
    case "order.cancelled":
        fmt.Println("Order cancelled:", e.Payload)
    }
}
```

### Subscribe to All Events

```go
sub := b.SubscribeAll()
defer sub.Cancel()

go func() {
    for e := range sub.Channel {
        fmt.Printf("[%s] %s from %s\n", e.Timestamp.Format(time.RFC3339), e.Type, e.Source)
    }
}()
```

### Blocking Wait for an Event

`Wait` blocks until an event of the specified type arrives or the context is cancelled:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

e, err := b.Wait(ctx, "system.ready")
if err != nil {
    log.Fatalf("System did not become ready: %v", err)
}
fmt.Printf("System ready at %s\n", e.Timestamp)
```

### Wait for Any of Multiple Types

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

e, err := b.WaitMultiple(ctx, "provider.healthy", "provider.degraded", "provider.down")
if err != nil {
    log.Fatalf("No provider status received: %v", err)
}
fmt.Printf("Provider status: %s\n", e.Type)
```

### Cancelling Subscriptions

Always cancel subscriptions when they are no longer needed to free resources:

```go
sub := b.Subscribe("cache.invalidated")

// Option 1: Cancel via the subscription
sub.Cancel()

// Option 2: Cancel by channel reference
b.UnsubscribeByChannel(sub.Channel)
```

## Filtering Events

Filters control which events reach a subscriber. A filter is a function `func(*event.Event) bool` that returns `true` to allow delivery.

### Filter by Source

```go
import "digital.vasic.eventbus/pkg/filter"

// Only receive events from the auth service
sub := b.SubscribeWithFilter("user.login", filter.BySource("auth-service"))
defer sub.Cancel()
```

### Filter by Event Type

```go
// Useful with SubscribeAll to select specific types
sub := b.SubscribeAllWithFilter(filter.ByType("system.error"))
```

### Filter by Multiple Types

```go
sub := b.SubscribeAllWithFilter(filter.ByTypes("cache.hit", "cache.miss", "cache.evict"))
```

### Glob Pattern Matching

Uses `path.Match` syntax where `*` matches any sequence of non-dot characters and `?` matches a single character:

```go
// Match all provider events at one level: provider.registered, provider.removed
sub := b.SubscribeAllWithFilter(filter.ByGlob("provider.*"))
```

### Prefix Matching

Unlike glob, prefix matching works across dot boundaries:

```go
// Match provider.registered, provider.health.changed, provider.config.updated
sub := b.SubscribeAllWithFilter(filter.ByPrefix("provider."))
```

### Metadata Filters

```go
// Only events with a specific metadata value
sub := b.SubscribeAllWithFilter(filter.ByMetadata("env", "production"))

// Only events that have a specific metadata key (any value)
sub := b.SubscribeAllWithFilter(filter.HasMetadata("correlationID"))
```

### Combining Filters

Filters can be composed with logical combinators:

```go
// AND: all conditions must match
f := filter.And(
    filter.ByPrefix("provider."),
    filter.BySource("health-monitor"),
    filter.HasMetadata("severity"),
)
sub := b.SubscribeAllWithFilter(f)

// OR: any condition matches
f := filter.Or(
    filter.ByType("system.error"),
    filter.ByType("system.critical"),
)
sub := b.SubscribeAllWithFilter(f)

// NOT: negate a filter
f := filter.And(
    filter.ByPrefix("provider."),
    filter.Not(filter.BySource("internal-monitor")),
)
sub := b.SubscribeAllWithFilter(f)
```

### Special Filters

```go
// Always passes (no filtering)
sub := b.SubscribeAllWithFilter(filter.All())

// Never passes (blocks everything -- useful for testing)
sub := b.SubscribeAllWithFilter(filter.None())
```

## Middleware

Middleware intercepts and transforms events before they reach subscribers. A middleware function receives an event and returns a (possibly modified) event. Returning `nil` drops the event entirely.

### Adding Middleware

```go
b := bus.New(nil)
b.Use(middleware.Enrich("service", "my-app"))
```

Middleware are applied in the order they are added.

### Logging Middleware

```go
import "log"

// Using a standard logger
logger := log.New(os.Stdout, "", log.LstdFlags)
b.Use(middleware.Logging(logger))

// Using a custom log function (e.g., log.Printf, fmt.Printf)
b.Use(middleware.LoggingFunc(log.Printf))
```

Output format: `[event] id=<id> type=<type> source=<source> trace=<traceID>`

### Metrics Middleware

```go
mc := middleware.NewMetricsCounter()
b.Use(mc.Middleware())

// Later, query metrics
total := mc.GetTotal()
fmt.Printf("Total events processed: %d\n", total)
```

### Enrichment Middleware

Automatically add metadata to every event:

```go
b.Use(middleware.Enrich("env", "production"))
b.Use(middleware.Enrich("version", "v2.3.1"))
b.Use(middleware.Enrich("region", "us-east-1"))
```

### Timestamp Middleware

Replace the event timestamp with the current time at middleware execution:

```go
b.Use(middleware.Timestamp())
```

### Recovery Middleware

Prevent panics in downstream processing from crashing the bus:

```go
b.Use(middleware.Recover(func(r interface{}) {
    log.Printf("Recovered from panic: %v", r)
}))
```

### Rate Limiting Middleware

Drop events that exceed a per-second threshold:

```go
// Allow max 1000 events per second; excess events are dropped (return nil)
b.Use(middleware.RateLimit(1000))
```

When the rate limit is exceeded, the middleware returns `nil`, which causes the bus to silently drop the event. The event will not be counted as published and no subscribers will receive it.

### Chaining Middleware

Compose multiple middleware into a single unit:

```go
pipeline := middleware.Chain(
    middleware.LoggingFunc(log.Printf),
    middleware.Enrich("service", "order-processor"),
    middleware.Enrich("env", "production"),
    middleware.RateLimit(5000),
)
b.Use(pipeline)
```

### Custom Middleware

Write your own middleware by implementing the `func(*event.Event) *event.Event` signature:

```go
// Middleware that adds a processing timestamp
func ProcessingTimestamp() middleware.Middleware {
    return func(e *event.Event) *event.Event {
        e.WithMetadata("processed_at", time.Now().UTC().Format(time.RFC3339Nano))
        return e
    }
}

// Middleware that drops events from a specific source
func DropSource(source string) middleware.Middleware {
    return func(e *event.Event) *event.Event {
        if e.Source == source {
            return nil // Drop the event
        }
        return e
    }
}

b.Use(ProcessingTimestamp())
b.Use(DropSource("debug-emitter"))
```

## Monitoring with Metrics

The event bus tracks operational metrics:

```go
b := bus.New(nil)
defer b.Close()

// ... publish and subscribe ...

metrics := b.Metrics()
fmt.Printf("Published:  %d\n", metrics.EventsPublished)
fmt.Printf("Delivered:  %d\n", metrics.EventsDelivered)
fmt.Printf("Dropped:    %d\n", metrics.EventsDropped)
fmt.Printf("Active:     %d\n", metrics.SubscribersActive)
fmt.Printf("Total:      %d\n", metrics.SubscribersTotal)

// Subscriber count for a specific type
count := b.SubscriberCount("user.created")
fmt.Printf("user.created subscribers: %d\n", count)

// Total active subscribers across all types
total := b.TotalSubscribers()
fmt.Printf("Total active subscribers: %d\n", total)
```

**Metrics fields**:
- `EventsPublished` -- Events that passed through middleware and were dispatched to subscribers.
- `EventsDelivered` -- Events successfully sent to subscriber channels.
- `EventsDropped` -- Events that could not be delivered (subscriber channel full or timed out).
- `SubscribersActive` -- Currently active subscriptions.
- `SubscribersTotal` -- Lifetime total subscriptions created.

## Graceful Shutdown

`Close()` stops the cleanup goroutine, closes all subscriber channels, and prevents further publishes:

```go
b := bus.New(nil)

sub := b.Subscribe("user.created")

// Shutdown
err := b.Close()
if err != nil {
    log.Printf("Error closing bus: %v", err)
}

// After close, the subscription channel is closed
_, ok := <-sub.Channel
// ok == false

// Publishing after close is a no-op (does not panic)
b.Publish(event.New("user.created", "test", nil))

// Subscribing after close returns an immediately-closed channel
sub2 := b.Subscribe("user.created")
_, ok = <-sub2.Channel
// ok == false
```

Calling `Close()` multiple times is safe and returns `nil` on subsequent calls.

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "digital.vasic.eventbus/pkg/bus"
    "digital.vasic.eventbus/pkg/event"
    "digital.vasic.eventbus/pkg/filter"
    "digital.vasic.eventbus/pkg/middleware"
)

func main() {
    // 1. Create bus with custom config
    b := bus.New(&bus.Config{
        BufferSize:      2000,
        PublishTimeout:  25 * time.Millisecond,
        CleanupInterval: 15 * time.Second,
        MaxSubscribers:  50,
    })
    defer b.Close()

    // 2. Add middleware pipeline
    logger := log.New(os.Stdout, "", log.LstdFlags)
    mc := middleware.NewMetricsCounter()

    b.Use(middleware.Chain(
        middleware.Logging(logger),
        mc.Middleware(),
        middleware.Enrich("service", "order-processor"),
        middleware.Enrich("env", "production"),
    ))

    // 3. Subscribe to order events with a filter
    orderSub := b.SubscribeAllWithFilter(
        filter.And(
            filter.ByPrefix("order."),
            filter.Not(filter.BySource("test-harness")),
        ),
    )
    defer orderSub.Cancel()

    // 4. Process events in background
    go func() {
        for e := range orderSub.Channel {
            fmt.Printf("Processing %s: %v\n", e.Type, e.Payload)
        }
    }()

    // 5. Publish events
    b.Publish(event.New("order.created", "api-gateway", map[string]string{
        "orderID": "ord_456",
        "total":   "99.99",
    }).WithTraceID("trace-xyz").WithMetadata("priority", "normal"))

    b.Publish(event.New("order.shipped", "fulfillment", map[string]string{
        "orderID":    "ord_456",
        "trackingNo": "1Z999AA10123456784",
    }))

    // 6. Wait for a specific event
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    go func() {
        time.Sleep(100 * time.Millisecond)
        b.Publish(event.New("order.delivered", "logistics", nil))
    }()

    delivered, err := b.Wait(ctx, "order.delivered")
    if err != nil {
        log.Fatalf("Delivery event not received: %v", err)
    }
    fmt.Printf("Order delivered at %s\n", delivered.Timestamp.Format(time.RFC3339))

    // 7. Check metrics
    fmt.Printf("Total events processed by middleware: %d\n", mc.GetTotal())
    metrics := b.Metrics()
    fmt.Printf("Bus stats: published=%d delivered=%d dropped=%d\n",
        metrics.EventsPublished, metrics.EventsDelivered, metrics.EventsDropped)
}
```
