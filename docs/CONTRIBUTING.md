# Contributing to EventBus

## Prerequisites

- Go 1.24 or later
- Git with SSH access configured

## Getting Started

1. Clone the repository:
   ```bash
   git clone <ssh-url>
   cd EventBus
   ```

2. Verify the build:
   ```bash
   go build ./...
   ```

3. Run all tests:
   ```bash
   go test ./... -count=1 -race
   ```

## Development Workflow

### Branch Naming

Create a branch from `main` using the conventional prefix:

| Prefix | Use |
|--------|-----|
| `feat/` | New features |
| `fix/` | Bug fixes |
| `refactor/` | Code restructuring without behavior change |
| `test/` | Adding or improving tests |
| `docs/` | Documentation changes |
| `chore/` | Build, CI, tooling changes |

Examples:
```bash
git checkout -b feat/deduplication-middleware
git checkout -b fix/race-condition-cleanup
git checkout -b test/concurrent-unsubscribe
```

### Commit Conventions

Use Conventional Commits with package scope:

```
<type>(<scope>): <description>
```

Scopes correspond to package names: `event`, `bus`, `filter`, `middleware`, or `eventbus` for cross-cutting changes.

Examples:
```
feat(filter): add ByPayloadType filter
fix(bus): prevent race condition in cleanup loop
test(middleware): add rate limiter edge case tests
docs(eventbus): update architecture diagrams
refactor(bus): extract subscriber management to helper
```

### Code Style

Follow standard Go conventions:

- Format with `gofmt` (or `goimports`).
- Imports grouped with blank line separators: stdlib, third-party, internal.
- Line length at most 100 characters.
- Naming: `camelCase` for unexported, `PascalCase` for exported, `UPPER_SNAKE_CASE` for constants, acronyms all-caps (`ID`, `HTTP`, `URL`).
- Receivers: 1-2 letter abbreviations (`b` for bus, `e` for event, `s` for subscriber, `m` for metrics).
- Always check errors, wrap with `fmt.Errorf("...: %w", err)`.
- Use `defer` for cleanup.
- Add doc comments to all exported types, functions, and methods.

### Writing Tests

- Use table-driven tests with the `testify` library (`assert`, `require`).
- Naming convention: `Test<Struct>_<Method>_<Scenario>`.
- Test both success and failure paths.
- Test concurrent access with goroutines and `sync.WaitGroup` or `atomic` counters.
- Use `-race` flag to detect data races.

Example:
```go
func TestEventBus_Publish_NilEvent(t *testing.T) {
    b := New(nil)
    defer func() { _ = b.Close() }()

    // Should not panic
    b.Publish(nil)
}
```

### Running Tests

```bash
# All tests with race detection
go test ./... -count=1 -race

# Unit tests only
go test ./... -short

# Specific package
go test -v ./pkg/bus/

# Specific test
go test -v -run TestEventBus_Publish ./pkg/bus/

# Integration tests
go test -tags=integration ./...

# Benchmarks
go test -bench=. ./tests/benchmark/

# Coverage report
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out -o coverage.html
```

### Pre-Commit Checklist

Before creating a pull request, verify:

1. **Format**: `gofmt -l .` returns no files.
2. **Vet**: `go vet ./...` passes cleanly.
3. **Tests**: `go test ./... -count=1 -race` passes.
4. **Coverage**: No reduction in test coverage.

```bash
gofmt -w . && go vet ./... && go test ./... -count=1 -race
```

## Adding a New Filter

1. Add the filter function to `pkg/filter/filter.go`:
   ```go
   // ByPayloadType returns a filter that matches events
   // whose payload is of the specified Go type.
   func ByPayloadType(typeName string) Filter {
       return func(e *event.Event) bool {
           return fmt.Sprintf("%T", e.Payload) == typeName
       }
   }
   ```

2. Add tests to `pkg/filter/filter_test.go`:
   ```go
   func TestByPayloadType(t *testing.T) {
       // table-driven tests ...
   }
   ```

3. Run tests: `go test -v ./pkg/filter/`

## Adding a New Middleware

1. Add the middleware function to `pkg/middleware/middleware.go`:
   ```go
   // Deduplicate returns middleware that drops events with
   // previously-seen IDs within the specified window.
   func Deduplicate(window time.Duration) Middleware {
       // implementation
   }
   ```

2. Add tests to `pkg/middleware/middleware_test.go`.

3. Run tests: `go test -v ./pkg/middleware/`

## Pull Request Process

1. Create a branch with the appropriate prefix.
2. Make changes, commit with conventional commit messages.
3. Ensure all tests pass with race detection.
4. Push the branch and create a pull request against `main`.
5. PR title should follow the same conventional commit format.
6. Describe the change, why it was made, and how to test it.
7. Wait for review approval before merging.

## Reporting Issues

When reporting a bug, include:
- Go version (`go version`).
- OS and architecture.
- Minimal reproduction case.
- Expected vs. actual behavior.
- Stack trace if applicable.
