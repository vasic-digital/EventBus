// Package event provides core event types for the EventBus system.
// Events are the fundamental unit of communication in the pub/sub model.
package event

import (
	"time"

	"github.com/google/uuid"
)

// Type represents the type of event using dot-notation topics.
// Examples: "provider.registered", "cache.hit", "system.startup"
type Type string

// Event represents a system event with typed payload.
type Event struct {
	ID        string
	Type      Type
	Source    string
	Payload   interface{}
	Timestamp time.Time
	TraceID   string
	Metadata  map[string]string
}

// New creates a new event with the given type, source, and payload.
func New(eventType Type, source string, payload interface{}) *Event {
	return &Event{
		ID:        uuid.New().String(),
		Type:      eventType,
		Source:    source,
		Payload:   payload,
		Timestamp: time.Now(),
		TraceID:   uuid.New().String(),
		Metadata:  make(map[string]string),
	}
}

// WithTraceID sets the trace ID and returns the event for chaining.
func (e *Event) WithTraceID(traceID string) *Event {
	e.TraceID = traceID
	return e
}

// WithMetadata adds a metadata key-value pair and returns the event for chaining.
func (e *Event) WithMetadata(key, value string) *Event {
	if e.Metadata == nil {
		e.Metadata = make(map[string]string)
	}
	e.Metadata[key] = value
	return e
}

// Handler is a function that processes events.
type Handler func(*Event)

// Subscription represents an active subscription to events.
type Subscription struct {
	ID      string
	Types   []Type
	Channel <-chan *Event
	cancel  func()
}

// NewSubscription creates a new subscription.
func NewSubscription(id string, types []Type, ch <-chan *Event, cancel func()) *Subscription {
	return &Subscription{
		ID:      id,
		Types:   types,
		Channel: ch,
		cancel:  cancel,
	}
}

// Cancel cancels the subscription, removing it from the bus.
func (s *Subscription) Cancel() {
	if s.cancel != nil {
		s.cancel()
	}
}
