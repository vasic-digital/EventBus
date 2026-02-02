package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		eventType Type
		source    string
		payload   interface{}
	}{
		{
			name:      "basic event",
			eventType: "test.created",
			source:    "test-source",
			payload:   map[string]string{"key": "value"},
		},
		{
			name:      "nil payload",
			eventType: "test.empty",
			source:    "source",
			payload:   nil,
		},
		{
			name:      "string payload",
			eventType: "test.string",
			source:    "source",
			payload:   "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(tt.eventType, tt.source, tt.payload)

			assert.NotEmpty(t, e.ID)
			assert.Equal(t, tt.eventType, e.Type)
			assert.Equal(t, tt.source, e.Source)
			assert.Equal(t, tt.payload, e.Payload)
			assert.NotZero(t, e.Timestamp)
			assert.NotEmpty(t, e.TraceID)
			assert.NotNil(t, e.Metadata)
		})
	}
}

func TestEvent_WithTraceID(t *testing.T) {
	e := New("test.event", "source", nil)
	result := e.WithTraceID("custom-trace-id")

	assert.Equal(t, "custom-trace-id", e.TraceID)
	assert.Same(t, e, result)
}

func TestEvent_WithMetadata(t *testing.T) {
	e := New("test.event", "source", nil)
	result := e.WithMetadata("key1", "value1").WithMetadata("key2", "value2")

	assert.Equal(t, "value1", e.Metadata["key1"])
	assert.Equal(t, "value2", e.Metadata["key2"])
	assert.Same(t, e, result)
}

func TestEvent_WithMetadata_NilMap(t *testing.T) {
	e := &Event{ID: "test", Type: "test.event"}
	e.Metadata = nil

	e.WithMetadata("key", "value")

	assert.NotNil(t, e.Metadata)
	assert.Equal(t, "value", e.Metadata["key"])
}

func TestNewSubscription(t *testing.T) {
	ch := make(chan *Event)
	called := false
	cancel := func() { called = true }

	sub := NewSubscription("sub-1", []Type{"test.event"}, ch, cancel)

	assert.Equal(t, "sub-1", sub.ID)
	assert.Equal(t, []Type{"test.event"}, sub.Types)
	assert.Equal(t, (<-chan *Event)(ch), sub.Channel)

	sub.Cancel()
	assert.True(t, called)
}

func TestSubscription_Cancel_Nil(t *testing.T) {
	sub := &Subscription{ID: "test"}
	// Should not panic
	sub.Cancel()
}
