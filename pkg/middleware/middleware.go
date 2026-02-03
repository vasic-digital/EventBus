// Package middleware provides event middleware for the EventBus.
// Middleware can intercept, transform, or enrich events before delivery.
package middleware

import (
	"log"
	"sync/atomic"
	"time"

	"digital.vasic.eventbus/pkg/event"
)

// Middleware processes an event and returns the (possibly modified) event.
// Return nil to drop the event.
type Middleware func(*event.Event) *event.Event

// Chain composes multiple middleware into a single middleware.
// Middleware are applied in order: first middleware runs first.
func Chain(middlewares ...Middleware) Middleware {
	return func(e *event.Event) *event.Event {
		for _, mw := range middlewares {
			if e == nil {
				return nil
			}
			e = mw(e)
		}
		return e
	}
}

// Logging returns middleware that logs events.
func Logging(logger *log.Logger) Middleware {
	return func(e *event.Event) *event.Event {
		if logger != nil {
			logger.Printf(
				"[event] id=%s type=%s source=%s trace=%s",
				e.ID, e.Type, e.Source, e.TraceID,
			)
		}
		return e
	}
}

// LoggingFunc returns middleware that logs via a custom function.
func LoggingFunc(logFn func(string, ...interface{})) Middleware {
	return func(e *event.Event) *event.Event {
		if logFn != nil {
			logFn(
				"[event] id=%s type=%s source=%s trace=%s",
				e.ID, e.Type, e.Source, e.TraceID,
			)
		}
		return e
	}
}

// Metrics returns middleware that tracks event counts.
type MetricsCounter struct {
	Total   int64
	ByType  map[event.Type]*int64
	typeMu  atomic.Value // stores map for lock-free reads
}

// NewMetricsCounter creates a new metrics counter.
func NewMetricsCounter() *MetricsCounter {
	m := &MetricsCounter{
		ByType: make(map[event.Type]*int64),
	}
	return m
}

// Middleware returns the metrics-collecting middleware function.
func (m *MetricsCounter) Middleware() Middleware {
	return func(e *event.Event) *event.Event {
		atomic.AddInt64(&m.Total, 1)
		return e
	}
}

// GetTotal returns the total number of events processed.
func (m *MetricsCounter) GetTotal() int64 {
	return atomic.LoadInt64(&m.Total)
}

// Enrich returns middleware that adds metadata to every event.
func Enrich(key, value string) Middleware {
	return func(e *event.Event) *event.Event {
		e.WithMetadata(key, value)
		return e
	}
}

// Timestamp returns middleware that updates the event timestamp.
func Timestamp() Middleware {
	return func(e *event.Event) *event.Event {
		e.Timestamp = time.Now()
		return e
	}
}

// Recover returns middleware that recovers from panics in downstream
// processing, preventing a single bad handler from crashing the bus.
// The optional processor function allows wrapping event processing logic
// that may panic - if nil, the event is passed through unchanged.
func Recover(onPanic func(interface{})) Middleware {
	return RecoverWithProcessor(onPanic, nil)
}

// RecoverWithProcessor returns middleware that recovers from panics during
// event processing. The processor function processes the event and may panic;
// any panic is recovered and passed to onPanic. If processor is nil, the
// event is returned unchanged (useful for protecting downstream handlers).
func RecoverWithProcessor(onPanic func(interface{}), processor func(*event.Event) *event.Event) Middleware {
	return func(e *event.Event) (result *event.Event) {
		defer func() {
			if r := recover(); r != nil {
				if onPanic != nil {
					onPanic(r)
				}
				result = nil // Event is dropped on panic
			}
		}()
		if processor != nil {
			return processor(e)
		}
		return e
	}
}

// RateLimit returns middleware that drops events if more than maxPerSecond
// events are published within a rolling window.
func RateLimit(maxPerSecond int64) Middleware {
	var count int64
	var windowStart int64

	return func(e *event.Event) *event.Event {
		now := time.Now().Unix()
		ws := atomic.LoadInt64(&windowStart)
		if now != ws {
			atomic.StoreInt64(&windowStart, now)
			atomic.StoreInt64(&count, 1)
			return e
		}
		c := atomic.AddInt64(&count, 1)
		if c > maxPerSecond {
			return nil // Drop the event
		}
		return e
	}
}
