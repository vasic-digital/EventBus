# CLAUDE.md - EventBus Module

## Overview

`digital.vasic.eventbus` is a generic, reusable Go module for publish/subscribe event-driven communication. It provides typed events, topic-based filtering with glob patterns, middleware chains, and metrics tracking.

**Module**: `digital.vasic.eventbus` (Go 1.24+)

## Build & Test

```bash
go build ./...
go test ./... -count=1 -race
go test ./... -short              # Unit tests only
go test -tags=integration ./...   # Integration tests
go test -bench=. ./tests/benchmark/
```

## Code Style

- Standard Go conventions, `gofmt` formatting
- Imports grouped: stdlib, third-party, internal (blank line separated)
- Line length <= 100 chars
- Naming: `camelCase` private, `PascalCase` exported, acronyms all-caps
- Errors: always check, wrap with `fmt.Errorf("...: %w", err)`
- Tests: table-driven, `testify`, naming `Test<Struct>_<Method>_<Scenario>`

## Package Structure

| Package | Purpose |
|---------|---------|
| `pkg/event` | Core types: Event, Type, Handler, Subscription |
| `pkg/bus` | EventBus implementation (pub/sub with middleware) |
| `pkg/filter` | Event filtering (type, source, glob, prefix, metadata, combinators) |
| `pkg/middleware` | Event middleware (logging, metrics, enrichment, rate limiting) |

## Key Interfaces

- `event.Type` — String-based event type with dot-notation topics
- `event.Event` — Event struct (ID, Type, Source, Payload, Timestamp, TraceID, Metadata)
- `event.Subscription` — Active subscription with Channel and Cancel
- `filter.Filter` — `func(*event.Event) bool` for event filtering
- `middleware.Middleware` — `func(*event.Event) *event.Event` for event transformation

## Design Patterns

- **Observer**: Publish/subscribe with typed events
- **Mediator**: EventBus decouples publishers from subscribers
- **Middleware Chain**: Composable event transformations
- **Builder**: Event creation with fluent WithTraceID/WithMetadata

## Commit Style

Conventional Commits: `feat(bus): add middleware support`
