# digital.vasic.eventbus

A generic, reusable Go module for publish/subscribe event-driven communication with typed events, filtering, and middleware.

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
    // Create event bus
    b := bus.New(nil) // uses default config
    defer b.Close()

    // Add middleware
    b.Use(middleware.Enrich("service", "my-app"))

    // Subscribe to events
    sub := b.Subscribe("user.created")
    defer sub.Cancel()

    // Publish an event
    b.Publish(event.New("user.created", "auth-service", map[string]string{
        "userID": "123",
    }))

    // Receive the event
    select {
    case e := <-sub.Channel:
        fmt.Printf("Received: %s from %s\n", e.Type, e.Source)
    case <-time.After(time.Second):
        fmt.Println("Timeout")
    }

    // Wait for specific event
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
- **Filter combinators**: `And`, `Or`, `Not` for complex filter logic
- **Middleware chain**: logging, metrics, enrichment, rate limiting
- **Multiple subscription modes**: single type, multiple types, all events
- **Async publishing** with `PublishAsync`
- **Wait/WaitMultiple** for blocking event consumption
- **Metrics tracking**: published, delivered, dropped counts
- **Thread-safe** concurrent publish/subscribe
- **Graceful shutdown** with `Close()`

## Packages

| Package | Description |
|---------|-------------|
| `pkg/event` | Core event types and subscription |
| `pkg/bus` | EventBus implementation |
| `pkg/filter` | Event filtering functions |
| `pkg/middleware` | Event middleware (logging, metrics, etc.) |

## Filtering

```go
// Filter by source
sub := b.SubscribeWithFilter("user.created", filter.BySource("auth-service"))

// Glob pattern matching
sub := b.SubscribeAllWithFilter(filter.ByGlob("provider.*"))

// Prefix matching
sub := b.SubscribeAllWithFilter(filter.ByPrefix("cache."))

// Combine filters
f := filter.And(
    filter.ByPrefix("provider."),
    filter.Not(filter.BySource("internal")),
)
sub := b.SubscribeAllWithFilter(f)
```

## Middleware

```go
// Logging
b.Use(middleware.LoggingFunc(log.Printf))

// Metrics
mc := middleware.NewMetricsCounter()
b.Use(mc.Middleware())
fmt.Println(mc.GetTotal()) // events processed

// Enrichment
b.Use(middleware.Enrich("env", "production"))

// Rate limiting
b.Use(middleware.RateLimit(1000)) // max 1000 events/second

// Chain multiple
b.Use(middleware.Chain(
    middleware.LoggingFunc(log.Printf),
    middleware.Enrich("version", "v1"),
))
```
