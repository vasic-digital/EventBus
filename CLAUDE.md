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


---

## ⚠️ MANDATORY: NO SUDO OR ROOT EXECUTION

**ALL operations MUST run at local user level ONLY.**

This is a PERMANENT and NON-NEGOTIABLE security constraint:

- **NEVER** use `sudo` in ANY command
- **NEVER** use `su` in ANY command
- **NEVER** execute operations as `root` user
- **NEVER** elevate privileges for file operations
- **ALL** infrastructure commands MUST use user-level container runtimes (rootless podman/docker)
- **ALL** file operations MUST be within user-accessible directories
- **ALL** service management MUST be done via user systemd or local process management
- **ALL** builds, tests, and deployments MUST run as the current user

### Container-Based Solutions
When a build or runtime environment requires system-level dependencies, use containers instead of elevation:

- **Use the `Containers` submodule** (`https://github.com/vasic-digital/Containers`) for containerized build and runtime environments
- **Add the `Containers` submodule as a Git dependency** and configure it for local use within the project
- **Build and run inside containers** to avoid any need for privilege escalation
- **Rootless Podman/Docker** is the preferred container runtime

### Why This Matters
- **Security**: Prevents accidental system-wide damage
- **Reproducibility**: User-level operations are portable across systems
- **Safety**: Limits blast radius of any issues
- **Best Practice**: Modern container workflows are rootless by design

### When You See SUDO
If any script or command suggests using `sudo` or `su`:
1. STOP immediately
2. Find a user-level alternative
3. Use rootless container runtimes
4. Use the `Containers` submodule for containerized builds
5. Modify commands to work within user permissions

**VIOLATION OF THIS CONSTRAINT IS STRICTLY PROHIBITED.**
