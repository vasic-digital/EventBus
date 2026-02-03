# EventBus API Reference

## Package `event`

**Import**: `digital.vasic.eventbus/pkg/event`

The `event` package defines core types for the EventBus system. Events are the fundamental unit of communication in the pub/sub model.

---

### Type `Type`

```go
type Type string
```

Represents the type of event using dot-notation topics. Examples: `"provider.registered"`, `"cache.hit"`, `"system.startup"`.

---

### Struct `Event`

```go
type Event struct {
    ID        string
    Type      Type
    Source    string
    Payload   interface{}
    Timestamp time.Time
    TraceID   string
    Metadata  map[string]string
}
```

Represents a system event with typed payload.

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `ID` | `string` | Unique event identifier (UUID v4, auto-generated) |
| `Type` | `Type` | Dot-notation event type (e.g., `"user.created"`) |
| `Source` | `string` | Identifier of the component that produced the event |
| `Payload` | `interface{}` | Arbitrary event data |
| `Timestamp` | `time.Time` | When the event was created (set to `time.Now()` by `New`) |
| `TraceID` | `string` | Distributed tracing identifier (UUID v4, auto-generated) |
| `Metadata` | `map[string]string` | Key-value metadata pairs |

---

### Function `New`

```go
func New(eventType Type, source string, payload interface{}) *Event
```

Creates a new event with auto-generated ID, TraceID, current timestamp, and empty metadata map.

**Parameters**:
- `eventType` -- The event type (dot-notation topic string).
- `source` -- Identifier of the producing component.
- `payload` -- Arbitrary event data (can be `nil`).

**Returns**: A pointer to the new `Event`.

---

### Method `Event.WithTraceID`

```go
func (e *Event) WithTraceID(traceID string) *Event
```

Sets the trace ID on the event. Returns the event for method chaining.

---

### Method `Event.WithMetadata`

```go
func (e *Event) WithMetadata(key, value string) *Event
```

Adds a metadata key-value pair to the event. Initializes the metadata map if nil. Returns the event for method chaining.

---

### Type `Handler`

```go
type Handler func(*Event)
```

A function type that processes events.

---

### Struct `Subscription`

```go
type Subscription struct {
    ID      string
    Types   []Type
    Channel <-chan *Event
}
```

Represents an active subscription to events.

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `ID` | `string` | Unique subscription identifier (UUID v4) |
| `Types` | `[]Type` | Event types this subscription is registered for (nil for all-event subscriptions) |
| `Channel` | `<-chan *Event` | Read-only channel delivering events to the subscriber |

---

### Function `NewSubscription`

```go
func NewSubscription(id string, types []Type, ch <-chan *Event, cancel func()) *Subscription
```

Creates a new `Subscription`. Typically called internally by the bus; users receive subscriptions from `Subscribe*` methods.

---

### Method `Subscription.Cancel`

```go
func (s *Subscription) Cancel()
```

Cancels the subscription, removing it from the bus and closing the delivery channel. Safe to call multiple times. After cancellation, the channel will be closed and further reads will return the zero value.

---

## Package `bus`

**Import**: `digital.vasic.eventbus/pkg/bus`

The `bus` package provides the core EventBus implementation for pub/sub event-driven communication.

---

### Struct `Config`

```go
type Config struct {
    BufferSize      int
    PublishTimeout  time.Duration
    CleanupInterval time.Duration
    MaxSubscribers  int
}
```

Holds configuration for the event bus.

**Fields**:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `BufferSize` | `int` | `1000` | Buffer size for subscriber channels |
| `PublishTimeout` | `time.Duration` | `10ms` | Timeout for publishing to each subscriber |
| `CleanupInterval` | `time.Duration` | `30s` | Interval for the dead-subscriber cleanup goroutine |
| `MaxSubscribers` | `int` | `100` | Maximum number of subscribers per event type |

---

### Function `DefaultConfig`

```go
func DefaultConfig() *Config
```

Returns a `Config` with default values: BufferSize=1000, PublishTimeout=10ms, CleanupInterval=30s, MaxSubscribers=100.

---

### Struct `Metrics`

```go
type Metrics struct {
    EventsPublished   int64
    EventsDelivered   int64
    EventsDropped     int64
    SubscribersActive int64
    SubscribersTotal  int64
}
```

Tracks event bus statistics. All fields are safe to read concurrently when obtained from `EventBus.Metrics()`.

**Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `EventsPublished` | `int64` | Events that passed middleware and were dispatched |
| `EventsDelivered` | `int64` | Events successfully sent to subscriber channels |
| `EventsDropped` | `int64` | Events that failed to deliver (channel full/timeout) |
| `SubscribersActive` | `int64` | Currently active subscriptions |
| `SubscribersTotal` | `int64` | Lifetime total subscriptions created |

---

### Struct `EventBus`

```go
type EventBus struct {
    // unexported fields
}
```

Provides publish/subscribe for system events. Thread-safe for concurrent use.

---

### Function `New`

```go
func New(config *Config) *EventBus
```

Creates a new event bus. Pass `nil` for default configuration. Starts a background cleanup goroutine.

**Parameters**:
- `config` -- Bus configuration. If `nil`, `DefaultConfig()` is used.

**Returns**: A pointer to the new `EventBus`.

---

### Method `EventBus.Use`

```go
func (b *EventBus) Use(mw ...middleware.Middleware)
```

Adds one or more middleware to the event bus. Middleware are applied in the order they are added to every published event before delivery to subscribers.

---

### Method `EventBus.Publish`

```go
func (b *EventBus) Publish(e *event.Event)
```

Sends an event to all matching subscribers. The event passes through the middleware chain first. If any middleware returns `nil`, the event is dropped. Publishing `nil` is a no-op. Publishing after `Close()` is a no-op.

---

### Method `EventBus.PublishAsync`

```go
func (b *EventBus) PublishAsync(e *event.Event)
```

Publishes an event asynchronously in a new goroutine. Returns immediately.

---

### Method `EventBus.Subscribe`

```go
func (b *EventBus) Subscribe(eventType event.Type) *event.Subscription
```

Subscribes to events of a specific type. Returns a `Subscription` with a channel for receiving events.

---

### Method `EventBus.SubscribeWithFilter`

```go
func (b *EventBus) SubscribeWithFilter(eventType event.Type, f filter.Filter) *event.Subscription
```

Subscribes to a specific event type with an additional filter. Events matching the type but not the filter are not delivered.

---

### Method `EventBus.SubscribeMultiple`

```go
func (b *EventBus) SubscribeMultiple(types ...event.Type) *event.Subscription
```

Subscribes to multiple event types. Events of any of the specified types are delivered through a single channel.

---

### Method `EventBus.SubscribeMultipleWithFilter`

```go
func (b *EventBus) SubscribeMultipleWithFilter(f filter.Filter, types ...event.Type) *event.Subscription
```

Subscribes to multiple event types with an additional filter.

---

### Method `EventBus.SubscribeAll`

```go
func (b *EventBus) SubscribeAll() *event.Subscription
```

Subscribes to all event types. Every event published to the bus is delivered to this subscription.

---

### Method `EventBus.SubscribeAllWithFilter`

```go
func (b *EventBus) SubscribeAllWithFilter(f filter.Filter) *event.Subscription
```

Subscribes to all events with a filter. Only events passing the filter are delivered.

---

### Method `EventBus.UnsubscribeByChannel`

```go
func (b *EventBus) UnsubscribeByChannel(ch <-chan *event.Event)
```

Removes a subscriber by its channel reference. Closes the subscriber's channel.

---

### Method `EventBus.Wait`

```go
func (b *EventBus) Wait(ctx context.Context, eventType event.Type) (*event.Event, error)
```

Blocks until an event of the specified type is received or the context is cancelled. Creates a temporary subscription internally and cancels it after receiving one event.

**Returns**: The received event and `nil` error, or `nil` and an error if the context was cancelled or the bus was closed.

---

### Method `EventBus.WaitMultiple`

```go
func (b *EventBus) WaitMultiple(ctx context.Context, types ...event.Type) (*event.Event, error)
```

Blocks until an event of any of the specified types is received or the context is cancelled.

**Returns**: The received event and `nil` error, or `nil` and an error if the context was cancelled or the bus was closed.

---

### Method `EventBus.Metrics`

```go
func (b *EventBus) Metrics() *Metrics
```

Returns a snapshot of current bus metrics. The returned `Metrics` struct is a copy; it does not update.

---

### Method `EventBus.SubscriberCount`

```go
func (b *EventBus) SubscriberCount(eventType event.Type) int
```

Returns the number of type-specific subscribers for the given event type. Does not include global (subscribe-all) subscribers.

---

### Method `EventBus.TotalSubscribers`

```go
func (b *EventBus) TotalSubscribers() int
```

Returns the total number of active subscribers (both type-specific and global).

---

### Method `EventBus.Close`

```go
func (b *EventBus) Close() error
```

Shuts down the event bus. Stops the cleanup goroutine, closes all subscriber channels, and marks the bus as closed. Subsequent calls to `Publish` and `Subscribe*` are no-ops. Idempotent: calling `Close()` multiple times returns `nil`.

---

## Package `filter`

**Import**: `digital.vasic.eventbus/pkg/filter`

The `filter` package provides event filtering capabilities. Filters determine which events are delivered to subscribers.

---

### Type `Filter`

```go
type Filter func(*event.Event) bool
```

A function that determines whether an event should be delivered. Returns `true` if the event should pass through.

---

### Function `ByType`

```go
func ByType(eventType event.Type) Filter
```

Returns a filter matching events of the exact specified type.

---

### Function `ByTypes`

```go
func ByTypes(types ...event.Type) Filter
```

Returns a filter matching events of any of the specified types. Uses an internal map for O(1) lookup.

---

### Function `BySource`

```go
func BySource(source string) Filter
```

Returns a filter matching events from the exact specified source.

---

### Function `ByGlob`

```go
func ByGlob(pattern string) Filter
```

Returns a filter matching event types against a glob pattern. Uses `path.Match` syntax: `*` matches any non-separator sequence, `?` matches a single character. Returns `false` on pattern syntax errors.

---

### Function `ByPrefix`

```go
func ByPrefix(prefix string) Filter
```

Returns a filter matching event types starting with the specified prefix. Unlike `ByGlob`, prefix matching works across dot boundaries.

---

### Function `ByMetadata`

```go
func ByMetadata(key, value string) Filter
```

Returns a filter matching events with a specific metadata key-value pair.

---

### Function `HasMetadata`

```go
func HasMetadata(key string) Filter
```

Returns a filter matching events that contain the specified metadata key (regardless of value).

---

### Function `And`

```go
func And(filters ...Filter) Filter
```

Combines multiple filters with logical AND. All filters must return `true` for the event to pass. Short-circuits on the first `false`.

---

### Function `Or`

```go
func Or(filters ...Filter) Filter
```

Combines multiple filters with logical OR. At least one filter must return `true` for the event to pass. Short-circuits on the first `true`.

---

### Function `Not`

```go
func Not(f Filter) Filter
```

Negates a filter. Returns `true` when the inner filter returns `false`, and vice versa.

---

### Function `All`

```go
func All() Filter
```

Returns a filter that always returns `true`. Passes every event through.

---

### Function `None`

```go
func None() Filter
```

Returns a filter that always returns `false`. Blocks every event.

---

## Package `middleware`

**Import**: `digital.vasic.eventbus/pkg/middleware`

The `middleware` package provides event middleware for the EventBus. Middleware can intercept, transform, or enrich events before delivery.

---

### Type `Middleware`

```go
type Middleware func(*event.Event) *event.Event
```

Processes an event and returns the (possibly modified) event. Return `nil` to drop the event, preventing it from reaching any subscribers.

---

### Function `Chain`

```go
func Chain(middlewares ...Middleware) Middleware
```

Composes multiple middleware into a single middleware. Middleware are applied in order (first in the list runs first). If any middleware returns `nil`, the chain short-circuits and returns `nil`.

---

### Function `Logging`

```go
func Logging(logger *log.Logger) Middleware
```

Returns middleware that logs events using a standard `*log.Logger`. Log format: `[event] id=<id> type=<type> source=<source> trace=<traceID>`. If `logger` is `nil`, no logging occurs.

---

### Function `LoggingFunc`

```go
func LoggingFunc(logFn func(string, ...interface{})) Middleware
```

Returns middleware that logs events via a custom format function (compatible with `log.Printf`, `fmt.Printf`, etc.). If `logFn` is `nil`, no logging occurs.

---

### Struct `MetricsCounter`

```go
type MetricsCounter struct {
    Total  int64
    ByType map[event.Type]*int64
}
```

Tracks event counts passing through the middleware.

---

### Function `NewMetricsCounter`

```go
func NewMetricsCounter() *MetricsCounter
```

Creates a new `MetricsCounter` with zeroed counters.

---

### Method `MetricsCounter.Middleware`

```go
func (m *MetricsCounter) Middleware() Middleware
```

Returns the metrics-collecting middleware function. Each event passing through increments the `Total` counter atomically.

---

### Method `MetricsCounter.GetTotal`

```go
func (m *MetricsCounter) GetTotal() int64
```

Returns the total number of events processed through this counter. Uses atomic load for thread safety.

---

### Function `Enrich`

```go
func Enrich(key, value string) Middleware
```

Returns middleware that adds a metadata key-value pair to every event.

---

### Function `Timestamp`

```go
func Timestamp() Middleware
```

Returns middleware that updates the event's `Timestamp` field to `time.Now()`.

---

### Function `Recover`

```go
func Recover(onPanic func(interface{})) Middleware
```

Returns middleware that recovers from panics. If a panic occurs, it calls `onPanic` with the recovered value (if `onPanic` is not nil) and returns the event normally.

---

### Function `RateLimit`

```go
func RateLimit(maxPerSecond int64) Middleware
```

Returns middleware that enforces a per-second event rate limit. Events exceeding the limit are dropped (returns `nil`). Uses a rolling one-second window with atomic counters.
