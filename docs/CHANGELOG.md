# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-01

### Added

- **Package `event`**: Core event types for the pub/sub system.
  - `Type` string alias for dot-notation event topics.
  - `Event` struct with ID, Type, Source, Payload, Timestamp, TraceID, and Metadata fields.
  - `New()` constructor with auto-generated UUID for ID and TraceID.
  - `WithTraceID()` and `WithMetadata()` builder methods for fluent event construction.
  - `Handler` function type for event processing.
  - `Subscription` struct with ID, Types, Channel, and Cancel().

- **Package `bus`**: EventBus implementation with pub/sub orchestration.
  - `Config` struct with BufferSize, PublishTimeout, CleanupInterval, MaxSubscribers.
  - `DefaultConfig()` factory with sensible defaults.
  - `New()` constructor with optional config (nil for defaults).
  - `Publish()` synchronous event dispatch through middleware to subscribers.
  - `PublishAsync()` non-blocking event dispatch in a goroutine.
  - `Subscribe()` for single event type subscriptions.
  - `SubscribeWithFilter()` for filtered single type subscriptions.
  - `SubscribeMultiple()` and `SubscribeMultipleWithFilter()` for multi-type subscriptions.
  - `SubscribeAll()` and `SubscribeAllWithFilter()` for global subscriptions.
  - `UnsubscribeByChannel()` for removing subscribers by channel reference.
  - `Wait()` and `WaitMultiple()` for blocking event consumption with context support.
  - `Use()` for adding middleware to the event processing pipeline.
  - `Metrics()` for retrieving bus statistics (published, delivered, dropped, subscribers).
  - `SubscriberCount()` and `TotalSubscribers()` for subscriber introspection.
  - `Close()` for graceful shutdown with idempotent semantics.
  - Background cleanup goroutine for removing dead subscribers.
  - Full thread safety with `sync.RWMutex` and `sync/atomic`.

- **Package `filter`**: Event filtering with functional composition.
  - `ByType()` exact type matching.
  - `ByTypes()` multi-type matching with O(1) lookup.
  - `BySource()` exact source matching.
  - `ByGlob()` glob pattern matching using `path.Match` syntax.
  - `ByPrefix()` prefix matching across dot boundaries.
  - `ByMetadata()` metadata key-value matching.
  - `HasMetadata()` metadata key existence check.
  - `And()`, `Or()`, `Not()` logical combinators.
  - `All()` and `None()` constant filters.

- **Package `middleware`**: Event transformation pipeline.
  - `Chain()` for composing multiple middleware into one.
  - `Logging()` for logging via `*log.Logger`.
  - `LoggingFunc()` for logging via custom format function.
  - `MetricsCounter` struct with `Middleware()` and `GetTotal()` for event counting.
  - `Enrich()` for adding metadata to events.
  - `Timestamp()` for updating event timestamps.
  - `Recover()` for panic recovery in the middleware chain.
  - `RateLimit()` for per-second event rate limiting with atomic rolling window.
