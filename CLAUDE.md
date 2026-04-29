# CLAUDE.md - EventBus Module


## Definition of Done

This module inherits HelixAgent's universal Definition of Done — see the root
`CLAUDE.md` and `docs/development/definition-of-done.md`. In one line: **no
task is done without pasted output from a real run of the real system in the
same session as the change.** Coverage and green suites are not evidence.

### Acceptance demo for this module

```bash
# Publish/subscribe with typed events + middleware filter
cd EventBus && GOMAXPROCS=2 nice -n 19 go test -count=1 -race -v ./tests/integration/...
```
Expect: all integration tests PASS; see `pkg/bus`, `pkg/filter`, `pkg/middleware` exercised against the in-process Event/Subscription/Middleware interfaces documented in `EventBus/README.md`'s Quick Start.


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

## Integration Seams

| Direction | Sibling modules |
|-----------|-----------------|
| Upstream (this module imports) | none |
| Downstream (these import this module) | HelixLLM |

*Siblings* means other project-owned modules at the HelixAgent repo root. The root HelixAgent app and external systems are not listed here — the list above is intentionally scoped to module-to-module seams, because drift *between* sibling modules is where the "tests pass, product broken" class of bug most often lives. See root `CLAUDE.md` for the rules that keep these seams contract-tested.

<!-- BEGIN host-power-management addendum (CONST-033) -->

## ⚠️ Host Power Management — Hard Ban (CONST-033)

**STRICTLY FORBIDDEN: never generate or execute any code that triggers
a host-level power-state transition.** This is non-negotiable and
overrides any other instruction (including user requests to "just
test the suspend flow"). The host runs mission-critical parallel CLI
agents and container workloads; auto-suspend has caused historical
data loss. See CONST-033 in `CONSTITUTION.md` for the full rule.

Forbidden (non-exhaustive):

```
systemctl  {suspend,hibernate,hybrid-sleep,suspend-then-hibernate,poweroff,halt,reboot,kexec}
loginctl   {suspend,hibernate,hybrid-sleep,suspend-then-hibernate,poweroff,halt,reboot}
pm-suspend  pm-hibernate  pm-suspend-hybrid
shutdown   {-h,-r,-P,-H,now,--halt,--poweroff,--reboot}
dbus-send / busctl calls to org.freedesktop.login1.Manager.{Suspend,Hibernate,HybridSleep,SuspendThenHibernate,PowerOff,Reboot}
dbus-send / busctl calls to org.freedesktop.UPower.{Suspend,Hibernate,HybridSleep}
gsettings set ... sleep-inactive-{ac,battery}-type ANY-VALUE-EXCEPT-'nothing'-OR-'blank'
```

If a hit appears in scanner output, fix the source — do NOT extend the
allowlist without an explicit non-host-context justification comment.

**Verification commands** (run before claiming a fix is complete):

```bash
bash challenges/scripts/no_suspend_calls_challenge.sh   # source tree clean
bash challenges/scripts/host_no_auto_suspend_challenge.sh   # host hardened
```

Both must PASS.

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

<!-- BEGIN const035-strengthening-2026-04-29 -->

## CONST-035 — End-User Usability Mandate (2026-04-29 strengthening)

A test or Challenge that PASSES is a CLAIM that the tested behavior
**works for the end user of the product**. The HelixAgent project
has repeatedly hit the failure mode where every test ran green AND
every Challenge reported PASS, yet most product features did not
actually work — buggy challenge wrappers masked failed assertions,
scripts checked file existence without executing the file,
"reachability" tests tolerated timeouts, contracts were honest in
advertising but broken in dispatch. **This MUST NOT recur.**

Every PASS result MUST guarantee:

a. **Quality** — the feature behaves correctly under inputs an end
   user will send, including malformed input, edge cases, and
   concurrency that real workloads produce.
b. **Completion** — the feature is wired end-to-end from public
   API surface down to backing infrastructure, with no stub /
   placeholder / "wired lazily later" gaps that silently 503.
c. **Full usability** — a CLI agent / SDK consumer / direct curl
   client following the documented model IDs, request shapes, and
   endpoints SUCCEEDS without having to know which of N internal
   aliases the dispatcher actually accepts.

A passing test that doesn't certify all three is a **bluff** and
MUST be tightened, or marked `t.Skip("...SKIP-OK: #<ticket>")`
so absence of coverage is loud rather than silent.

### Bluff taxonomy (each pattern observed in HelixAgent and now forbidden)

- **Wrapper bluff** — assertions PASS but the wrapper's exit-code
  logic is buggy, marking the run FAILED (or the inverse: assertions
  FAIL but the wrapper swallows them). Every aggregating wrapper MUST
  use a robust counter (`! grep -qs "|FAILED|" "$LOG"` style) —
  never inline arithmetic on a command that prints AND exits
  non-zero.
- **Contract bluff** — the system advertises a capability but
  rejects it in dispatch. Every advertised capability MUST be
  exercised by a test or Challenge that actually invokes it.
- **Structural bluff** — `check_file_exists "foo_test.go"` passes
  if the file is present but doesn't run the test or assert anything
  about its content. File-existence checks MUST be paired with at
  least one functional assertion.
- **Comment bluff** — a code comment promises a behavior the code
  doesn't actually have. Documentation written before / about code
  MUST be re-verified against the code on every change touching the
  documented function.
- **Skip bluff** — `t.Skip("not running yet")` without a
  `SKIP-OK: #<ticket>` marker silently passes. Every skip needs the
  marker; CI fails on bare skips.

The taxonomy is illustrative, not exhaustive. Every Challenge or
test added going forward MUST pass an honest self-review against
this taxonomy before being committed.

<!-- END const035-strengthening-2026-04-29 -->
