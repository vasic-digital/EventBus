# EventBus Test Coverage Ledger (round-245)

Round-245 deep-doc enrichment under CONST-035 / Article XI §11.9 / CONST-050(B).

This document is the authoritative mapping of every exported symbol to the test sources that exercise it. Drift between this file and `go test -cover` output is a CONST-035 bluff at the documentation-truth layer — fix the document OR add the missing test, never silently leave the gap.

## Verbatim 2026-05-19 operator mandate (CONST-049 §11.4.17)

> "all existing tests and Challenges do work in anti-bluff manner - they MUST confirm that all tested codebase really works as expected! We had been in position that all tests do execute with success and all Challenges as well, but in reality the most of the features does not work and can't be used! This MUST NOT be the case and execution of tests and Challenges MUST guarantee the quality, the completition and full usability by end users of the product!"

## Test-type matrix (CONST-050(B))

| Test type | Location | Status |
|-----------|----------|--------|
| Unit | `pkg/*/`*_test.go` | PRESENT — every package |
| Integration | `tests/integration/` | PRESENT |
| End-to-end | `tests/e2e/` | PRESENT |
| Security | `tests/security/` | PRESENT |
| Stress | `tests/stress/` | PRESENT |
| Benchmark | `tests/benchmark/` | PRESENT |
| Challenges | `challenges/scripts/` | PRESENT — 11 scripts incl. paired-mutation `_describe_` |
| Bilingual fixtures | `tests/fixtures/i18n/` | PRESENT (round-245) |

## `pkg/event`

| Symbol | Kind | Test source(s) |
|--------|------|---------------|
| `Type` | type alias | `pkg/event/event_test.go` (TestTypeString) |
| `Event` | struct | `pkg/event/event_test.go` (TestNewEvent, TestEvent_WithTraceID, TestEvent_WithMetadata) |
| `New(type, source, payload)` | constructor | `pkg/event/event_test.go` (TestNewEvent) |
| `Event.WithTraceID` | method | `pkg/event/event_test.go` (TestEvent_WithTraceID) |
| `Event.WithMetadata` | method | `pkg/event/event_test.go` (TestEvent_WithMetadata) — also exercised by bilingual fixture `eventbus_describe_challenge.sh` |
| `Handler` | func type | `pkg/bus/bus_test.go` (TestEventBus_Subscribe wiring) |
| `Subscription` | struct | `pkg/event/event_test.go` (TestSubscription_Cancel) |
| `NewSubscription` | constructor | `pkg/event/event_test.go` (TestNewSubscription) |
| `Subscription.Cancel` | method | `pkg/event/event_test.go` (TestSubscription_Cancel), `pkg/bus/bus_test.go` (multiple) |

## `pkg/bus`

| Symbol | Kind | Test source(s) |
|--------|------|---------------|
| `Config` | struct | `pkg/bus/bus_test.go` (TestDefaultConfig) |
| `DefaultConfig()` | constructor | `pkg/bus/bus_test.go` (TestDefaultConfig, TestNewWithNilConfig) |
| `Metrics` | struct | `pkg/bus/bus_test.go` (TestEventBus_Metrics) |
| `EventBus` | struct | `pkg/bus/bus_test.go` (TestNewEventBus and ~all) |
| `New(cfg)` | constructor | `pkg/bus/bus_test.go` (TestNewEventBus, TestNewWithNilConfig) |
| `EventBus.Use` | method | `pkg/bus/bus_test.go` (TestEventBus_Middleware) |
| `EventBus.Publish` | method | `pkg/bus/bus_test.go` (TestEventBus_Publish, TestEventBus_PublishNil, TestEventBus_PublishAfterClose) |
| `EventBus.PublishAsync` | method | `pkg/bus/bus_test.go` (TestEventBus_PublishAsync) |
| `EventBus.Subscribe` | method | `pkg/bus/bus_test.go` (TestEventBus_Subscribe) |
| `EventBus.SubscribeWithFilter` | method | `pkg/bus/bus_test.go` (TestEventBus_SubscribeWithFilter) |
| `EventBus.SubscribeMultiple` | method | `pkg/bus/bus_test.go` (TestEventBus_SubscribeMultiple) |
| `EventBus.SubscribeMultipleWithFilter` | method | `pkg/bus/bus_edge_test.go` (TestEventBus_SubscribeMultipleWithFilter) |
| `EventBus.SubscribeAll` | method | `pkg/bus/bus_test.go` (TestEventBus_SubscribeAll) |
| `EventBus.SubscribeAllWithFilter` | method | `pkg/bus/bus_edge_test.go` (TestEventBus_SubscribeAllWithFilter) |
| `EventBus.UnsubscribeByChannel` | method | `pkg/bus/bus_edge_test.go` (TestEventBus_UnsubscribeByChannel) |
| `EventBus.Wait` | method | `pkg/bus/bus_test.go` (TestEventBus_Wait, TestEventBus_WaitCancelled) |
| `EventBus.WaitMultiple` | method | `pkg/bus/bus_test.go` (TestEventBus_WaitMultiple) |
| `EventBus.Metrics` | method | `pkg/bus/bus_test.go` (TestEventBus_Metrics) |
| `EventBus.SubscriberCount` | method | `pkg/bus/bus_test.go` (TestEventBus_SubscriberCount) |
| `EventBus.TotalSubscribers` | method | `pkg/bus/bus_test.go` (TestEventBus_TotalSubscribers) |
| `EventBus.Close` | method | `pkg/bus/bus_test.go` (TestEventBus_Close, TestEventBus_DoubleClose) |
| concurrency safety | runtime invariant | `pkg/bus/bus_test.go` (TestEventBus_Concurrent), `tests/stress/` (sustained-load suite), `make test -race` |

## `pkg/filter`

| Symbol | Kind | Test source(s) |
|--------|------|---------------|
| `Filter` | func type | `pkg/filter/filter_test.go` (all) |
| `ByType(t)` | constructor | `pkg/filter/filter_test.go` (TestByType) |
| `ByTypes(types...)` | constructor | `pkg/filter/filter_test.go` (TestByTypes) |
| `BySource(s)` | constructor | `pkg/filter/filter_test.go` (TestBySource) |
| `ByGlob(pattern)` | constructor | `pkg/filter/filter_test.go` (TestByGlob, TestByGlob_Wildcards) |
| `ByPrefix(p)` | constructor | `pkg/filter/filter_test.go` (TestByPrefix) |
| `ByMetadata(k,v)` | constructor | `pkg/filter/filter_test.go` (TestByMetadata) |
| `HasMetadata(k)` | constructor | `pkg/filter/filter_test.go` (TestHasMetadata) |
| `And(filters...)` | combinator | `pkg/filter/filter_test.go` (TestAnd) |
| `Or(filters...)` | combinator | `pkg/filter/filter_test.go` (TestOr) |
| `Not(f)` | combinator | `pkg/filter/filter_test.go` (TestNot) |
| `All()` | combinator | `pkg/filter/filter_test.go` (TestAll) |
| `None()` | combinator | `pkg/filter/filter_test.go` (TestNone) |

## `pkg/middleware`

| Symbol | Kind | Test source(s) |
|--------|------|---------------|
| `Middleware` | func type | `pkg/middleware/middleware_test.go` (all) |
| `Logging(logger)` | constructor | `pkg/middleware/middleware_test.go` (TestLogging) |
| `LoggingFunc(logf)` | constructor | `pkg/middleware/middleware_test.go` (TestLoggingFunc) |
| `MetricsCounter` | struct | `pkg/middleware/middleware_test.go` (TestNewMetricsCounter) |
| `NewMetricsCounter()` | constructor | `pkg/middleware/middleware_test.go` (TestNewMetricsCounter) |
| `MetricsCounter.Middleware()` | method | `pkg/middleware/middleware_test.go` (TestMetricsCounter_Middleware) |
| `MetricsCounter.GetTotal()` | method | `pkg/middleware/middleware_test.go` (TestMetricsCounter_GetTotal) |
| `Enrich(k,v)` | constructor | `pkg/middleware/middleware_test.go` (TestEnrich) |
| `Timestamp()` | constructor | `pkg/middleware/middleware_test.go` (TestTimestamp) |
| `Recover(onPanic)` | constructor | `pkg/middleware/middleware_test.go` (TestRecover, TestRecover_NilHandler) |
| `RecoverWithProcessor(onPanic,proc)` | constructor | `pkg/middleware/middleware_test.go` (TestRecoverWithProcessor_AllPaths) |
| `RateLimit(eventsPerSec)` | constructor | `pkg/middleware/middleware_test.go` (TestRateLimit) |
| `Chain(mws...)` | combinator | `pkg/middleware/middleware_test.go` (TestChain) |

## Edge cases (round-245)

- Nil event publish — `pkg/bus/bus_test.go` (TestEventBus_PublishNil)
- Publish after Close — `pkg/bus/bus_test.go` (TestEventBus_PublishAfterClose)
- Double Close — `pkg/bus/bus_test.go` (TestEventBus_DoubleClose)
- Subscribe after Close — `pkg/bus/bus_edge_test.go` (TestEventBus_SubscribeAfterClose)
- Middleware that returns nil (event drop) — `pkg/bus/bus_test.go` (TestEventBus_MiddlewareDrop)
- Subscriber channel-full overflow → `EventsDropped` increments — `pkg/bus/bus_test.go` (TestEventBus_DropOnFullChannel)
- Concurrent publish + subscribe + unsubscribe (race) — `pkg/bus/bus_test.go` (TestEventBus_Concurrent), `tests/stress/`
- UTF-8 / bilingual `Metadata` round-trip — `tests/fixtures/i18n/payloads.json` exercised by `challenges/scripts/eventbus_describe_challenge.sh`

## Paired-mutation Challenge

`challenges/scripts/eventbus_describe_challenge.sh` accepts `--anti-bluff-mutate` to plant a deliberate ledger-vs-source mismatch (renames one tracked symbol in the ledger) and asserts the gate FAILS with exit 99. Without the flag the gate runs normal validation and MUST exit 0. Composition: CONST-035 (anti-bluff) × CONST-050(B) (paired mutation) × CONST-047 (cascade).

## Anti-bluff acceptance criteria

1. `go test -count=1 -race -p 1 ./...` exits 0 — all 10 packages PASS (verified round-245).
2. `bash challenges/scripts/eventbus_describe_challenge.sh` exits 0 (gate PASS on clean tree).
3. `bash challenges/scripts/eventbus_describe_challenge.sh --anti-bluff-mutate` exits 99 (gate correctly fails on planted mutation).
4. Every symbol in this ledger appears in the listed test source verbatim — no metadata-only / configuration-only ledger entries.
