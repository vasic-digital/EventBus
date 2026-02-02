package filter

import (
	"testing"

	"digital.vasic.eventbus/pkg/event"
	"github.com/stretchr/testify/assert"
)

func newTestEvent(eventType event.Type, source string) *event.Event {
	return event.New(eventType, source, nil)
}

func TestByType(t *testing.T) {
	f := ByType("test.created")

	assert.True(t, f(newTestEvent("test.created", "src")))
	assert.False(t, f(newTestEvent("test.deleted", "src")))
}

func TestByTypes(t *testing.T) {
	f := ByTypes("test.created", "test.updated")

	assert.True(t, f(newTestEvent("test.created", "src")))
	assert.True(t, f(newTestEvent("test.updated", "src")))
	assert.False(t, f(newTestEvent("test.deleted", "src")))
}

func TestBySource(t *testing.T) {
	f := BySource("my-service")

	assert.True(t, f(newTestEvent("test.event", "my-service")))
	assert.False(t, f(newTestEvent("test.event", "other-service")))
}

func TestByGlob(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		typ     event.Type
		match   bool
	}{
		{"exact match", "test.created", "test.created", true},
		{"wildcard match", "test.*", "test.created", true},
		{"wildcard no match", "test.*", "other.created", false},
		{"question mark", "test.?reated", "test.created", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := ByGlob(tt.pattern)
			assert.Equal(t, tt.match, f(newTestEvent(tt.typ, "src")))
		})
	}
}

func TestByPrefix(t *testing.T) {
	f := ByPrefix("provider.")

	assert.True(t, f(newTestEvent("provider.registered", "src")))
	assert.True(t, f(newTestEvent("provider.health.changed", "src")))
	assert.False(t, f(newTestEvent("cache.hit", "src")))
}

func TestByMetadata(t *testing.T) {
	f := ByMetadata("env", "prod")

	e := newTestEvent("test.event", "src")
	e.WithMetadata("env", "prod")
	assert.True(t, f(e))

	e2 := newTestEvent("test.event", "src")
	e2.WithMetadata("env", "dev")
	assert.False(t, f(e2))

	e3 := newTestEvent("test.event", "src")
	e3.Metadata = nil
	assert.False(t, f(e3))
}

func TestHasMetadata(t *testing.T) {
	f := HasMetadata("traceID")

	e := newTestEvent("test.event", "src")
	e.WithMetadata("traceID", "abc")
	assert.True(t, f(e))

	e2 := newTestEvent("test.event", "src")
	assert.False(t, f(e2))

	e3 := &event.Event{Metadata: nil}
	assert.False(t, f(e3))
}

func TestAnd(t *testing.T) {
	f := And(BySource("my-service"), ByPrefix("test."))

	assert.True(t, f(newTestEvent("test.event", "my-service")))
	assert.False(t, f(newTestEvent("test.event", "other")))
	assert.False(t, f(newTestEvent("cache.hit", "my-service")))
}

func TestAnd_Empty(t *testing.T) {
	f := And()
	assert.True(t, f(newTestEvent("any", "any")))
}

func TestOr(t *testing.T) {
	f := Or(BySource("svc-a"), BySource("svc-b"))

	assert.True(t, f(newTestEvent("test", "svc-a")))
	assert.True(t, f(newTestEvent("test", "svc-b")))
	assert.False(t, f(newTestEvent("test", "svc-c")))
}

func TestOr_Empty(t *testing.T) {
	f := Or()
	assert.False(t, f(newTestEvent("any", "any")))
}

func TestNot(t *testing.T) {
	f := Not(BySource("blocked"))

	assert.True(t, f(newTestEvent("test", "allowed")))
	assert.False(t, f(newTestEvent("test", "blocked")))
}

func TestAll(t *testing.T) {
	f := All()
	assert.True(t, f(newTestEvent("any", "any")))
}

func TestNone(t *testing.T) {
	f := None()
	assert.False(t, f(newTestEvent("any", "any")))
}
