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

<!-- BEGIN host-power-management addendum (CONST-033) -->

## Host Power Management — Hard Ban (CONST-033)

**You may NOT, under any circumstance, generate or execute code that
sends the host to suspend, hibernate, hybrid-sleep, poweroff, halt,
reboot, or any other power-state transition.** This rule applies to:

- Every shell command you run via the Bash tool.
- Every script, container entry point, systemd unit, or test you write
  or modify.
- Every CLI suggestion, snippet, or example you emit.

**Forbidden invocations** (non-exhaustive — see CONST-033 in
`CONSTITUTION.md` for the full list):

- `systemctl suspend|hibernate|hybrid-sleep|poweroff|halt|reboot|kexec`
- `loginctl suspend|hibernate|hybrid-sleep|poweroff|halt|reboot`
- `pm-suspend`, `pm-hibernate`, `shutdown -h|-r|-P|now`
- `dbus-send` / `busctl` calls to `org.freedesktop.login1.Manager.Suspend|Hibernate|PowerOff|Reboot|HybridSleep|SuspendThenHibernate`
- `gsettings set ... sleep-inactive-{ac,battery}-type` to anything but `'nothing'` or `'blank'`

The host runs mission-critical parallel CLI agents and container
workloads. Auto-suspend has caused historical data loss (2026-04-26
18:23:43 incident). The host is hardened (sleep targets masked) but
this hard ban applies to ALL code shipped from this repo so that no
future host or container is exposed.

**Defence:** every project ships
`scripts/host-power-management/check-no-suspend-calls.sh` (static
scanner) and
`challenges/scripts/no_suspend_calls_challenge.sh` (challenge wrapper).
Both MUST be wired into the project's CI / `run_all_challenges.sh`.

**Full background:** `docs/HOST_POWER_MANAGEMENT.md` and `CONSTITUTION.md` (CONST-033).

<!-- END host-power-management addendum (CONST-033) -->


<!-- BEGIN anti-bluff-testing addendum (Article XI) -->

## Article XI — Anti-Bluff Testing (MANDATORY)

**Inherited from the umbrella project's Constitution Article XI.
Tests and Challenges that pass without exercising real end-user
behaviour are forbidden in this submodule too.**

Every test, every Challenge, every HelixQA bank entry MUST:

1. **Assert on a concrete end-user-visible outcome** — rendered DOM,
   DB rows that a real query would return, files on disk, media that
   actually plays, search results that actually contain expected
   items. Not "no error" or "200 OK".
2. **Run against the real system below the assertion.** Mocks/stubs
   are permitted ONLY in unit tests (`*_test.go` under `go test
   -short` or language equivalent). Integration / E2E / Challenge /
   HelixQA tests use real containers, real databases, real
   renderers. Unreachable real-system → skip with `SKIP-OK:
   #<ticket>`, never silently pass.
3. **Include a matching negative.** Every positive assertion is
   paired with an assertion that fails when the feature is broken.
4. **Emit copy-pasteable evidence** — body, screenshot, frame, DB
   row, log excerpt. Boolean pass/fail is insufficient.
5. **Verify "fails when feature is removed."** Author runs locally
   with the feature commented out; the test MUST FAIL. If it still
   passes, it's a bluff — delete and rewrite.
6. **No blind shells.** No `&& echo PASS`, `|| true`, `tee` exit
   laundering, `if [ -f file ]` without content assertion.

**Challenges in this submodule** must replay the user journey
end-to-end through the umbrella project's deliverables — never via
raw `curl` or third-party scripts. Sub-1-second Challenges almost
always indicate a bluff.

**HelixQA banks** declare executable actions
(`adb_shell:`, `playwright:`, `http:`, `assertVisible:`,
`assertNotVisible:`), never prose. Stagnation guard from Article I
§1.3 applies — frame N+1 identical to frame N for >10 s = FAIL.

**PR requirement:** every PR adding/modifying a test or Challenge in
this submodule MUST include a fenced `## Anti-Bluff Verification`
block with: (a) command run, (b) pasted output, (c) proof the test
fails when the feature is broken (second run with feature
commented-out showing FAIL).

**Cross-reference:** umbrella `CONSTITUTION.md` Article XI
(§§ 11.1 — 11.8).

<!-- END anti-bluff-testing addendum (Article XI) -->
