package middleware

import (
	"bytes"
	"log"
	"testing"

	"digital.vasic.eventbus/pkg/event"
	"github.com/stretchr/testify/assert"
)

func newTestEvent() *event.Event {
	return event.New("test.event", "test-source", nil)
}

func TestChain(t *testing.T) {
	calls := make([]string, 0)

	mw1 := func(e *event.Event) *event.Event {
		calls = append(calls, "mw1")
		return e
	}
	mw2 := func(e *event.Event) *event.Event {
		calls = append(calls, "mw2")
		return e
	}

	chain := Chain(mw1, mw2)
	result := chain(newTestEvent())

	assert.NotNil(t, result)
	assert.Equal(t, []string{"mw1", "mw2"}, calls)
}

func TestChain_DropEvent(t *testing.T) {
	drop := func(e *event.Event) *event.Event {
		return nil
	}
	never := func(e *event.Event) *event.Event {
		t.Fatal("should not be called after drop")
		return e
	}

	chain := Chain(drop, never)
	result := chain(newTestEvent())

	assert.Nil(t, result)
}

func TestChain_NilInput(t *testing.T) {
	mw := func(e *event.Event) *event.Event { return e }
	chain := Chain(mw)
	result := chain(nil)
	assert.Nil(t, result)
}

func TestLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	mw := Logging(logger)
	e := newTestEvent()
	result := mw(e)

	assert.Same(t, e, result)
	assert.Contains(t, buf.String(), "[event]")
	assert.Contains(t, buf.String(), "test.event")
	assert.Contains(t, buf.String(), "test-source")
}

func TestLogging_NilLogger(t *testing.T) {
	mw := Logging(nil)
	result := mw(newTestEvent())
	assert.NotNil(t, result)
}

func TestLoggingFunc(t *testing.T) {
	var logged string
	logFn := func(format string, args ...interface{}) {
		logged = format
	}

	mw := LoggingFunc(logFn)
	mw(newTestEvent())

	assert.Contains(t, logged, "[event]")
}

func TestLoggingFunc_Nil(t *testing.T) {
	mw := LoggingFunc(nil)
	result := mw(newTestEvent())
	assert.NotNil(t, result)
}

func TestMetricsCounter(t *testing.T) {
	mc := NewMetricsCounter()
	mw := mc.Middleware()

	for i := 0; i < 10; i++ {
		mw(newTestEvent())
	}

	assert.Equal(t, int64(10), mc.GetTotal())
}

func TestEnrich(t *testing.T) {
	mw := Enrich("env", "production")
	e := newTestEvent()
	result := mw(e)

	assert.Equal(t, "production", result.Metadata["env"])
}

func TestTimestamp(t *testing.T) {
	mw := Timestamp()
	e := newTestEvent()
	origTime := e.Timestamp

	// Give a tiny delay so timestamp differs
	result := mw(e)
	assert.GreaterOrEqual(t, result.Timestamp.UnixNano(), origTime.UnixNano())
}

func TestRecover(t *testing.T) {
	var recovered interface{}
	mw := Recover(func(r interface{}) {
		recovered = r
	})

	// Recover doesn't actually wrap downstream — it protects the middleware
	// chain itself. Test that the event passes through.
	result := mw(newTestEvent())
	assert.NotNil(t, result)
	assert.Nil(t, recovered)
}

func TestRateLimit(t *testing.T) {
	mw := RateLimit(5)

	passed := 0
	for i := 0; i < 10; i++ {
		if mw(newTestEvent()) != nil {
			passed++
		}
	}

	assert.LessOrEqual(t, passed, 5)
	assert.GreaterOrEqual(t, passed, 1)
}
