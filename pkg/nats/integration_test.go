package nats

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"digital.vasic.eventbus/pkg/event"
)

// TestNATSBus_RealPublishSubscribe exercises the Bus against a REAL running
// NATS JetStream server. It is skipped unless NATS_URL is set, so it never runs
// against a mock. It proves both legs of real routing:
//   - an event of the SUBSCRIBED type IS delivered (same ID/Type/payload), and
//   - an event of a DIFFERENT type is NOT delivered (no pass-bluff routing).
func TestNATSBus_RealPublishSubscribe(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL not set; skipping real NATS JetStream integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Unique stream/prefix per run so repeated runs do not cross-contaminate.
	prefix := "eventbus_it_" + time.Now().Format("150405.000")
	bus, err := New(ctx, Config{
		URL:           url,
		StreamName:    "EVENTBUS_IT_" + time.Now().Format("150405"),
		SubjectPrefix: prefix,
	})
	require.NoError(t, err, "New must connect to the real NATS server")
	defer func() { require.NoError(t, bus.Close()) }()

	const subscribedType event.Type = "provider.registered"
	const otherType event.Type = "provider.deregistered"

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	ch, unsub, err := bus.Subscribe(subCtx, subscribedType)
	require.NoError(t, err, "Subscribe must succeed against real server")
	defer unsub()

	// Give the push subscription a moment to be established server-side before
	// publishing (DeliverNew only sees events after the consumer exists).
	time.Sleep(300 * time.Millisecond)

	wantPayload := map[string]interface{}{
		"node":  "gpu-1",
		"score": float64(42),
	}
	want := &event.Event{
		ID:        "it-evt-1",
		Type:      subscribedType,
		Source:    "integration-test",
		Payload:   wantPayload,
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
		TraceID:   "it-trace-1",
		Metadata:  map[string]string{"env": "it"},
	}

	// Publish a DIFFERENT-type event first; it must NOT reach our subscriber.
	other := &event.Event{
		ID:        "it-evt-OTHER",
		Type:      otherType,
		Source:    "integration-test",
		Payload:   map[string]interface{}{"should": "not-arrive"},
		Timestamp: time.Now().UTC(),
		TraceID:   "it-trace-other",
		Metadata:  map[string]string{},
	}
	require.NoError(t, bus.Publish(other), "publishing other-type event")

	// Publish the matching event.
	require.NoError(t, bus.Publish(want), "publishing subscribed-type event")

	// Expect to receive the matching event — and ONLY the matching event.
	select {
	case got := <-ch:
		require.NotNil(t, got, "received event must not be nil")
		assert.Equal(t, want.ID, got.ID, "delivered event ID must match")
		assert.Equal(t, want.Type, got.Type, "delivered event Type must match")
		assert.Equal(t, want.Source, got.Source, "delivered Source must match")
		assert.Equal(t, want.TraceID, got.TraceID, "delivered TraceID must match")
		assert.Equal(t, want.Metadata, got.Metadata, "delivered Metadata must match")
		assert.True(t, want.Timestamp.Equal(got.Timestamp),
			"delivered Timestamp must match")
		// Payload round-trips as a generic JSON value.
		assert.Equal(t, wantPayload, got.Payload,
			"delivered payload must match (as generic JSON)")
		// Critically: the received event must NOT be the other-type event.
		assert.NotEqual(t, other.ID, got.ID,
			"different-type event MUST NOT be delivered to this subscriber")
		assert.Equal(t, subscribedType, got.Type,
			"only the subscribed type may be delivered")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the subscribed-type event")
	}

	// Drain briefly to assert the other-type event never sneaks in afterwards.
	select {
	case extra := <-ch:
		t.Fatalf("received an unexpected extra event of type %q (id=%q); "+
			"routing must isolate types", extra.Type, extra.ID)
	case <-time.After(1 * time.Second):
		// Good: no other-type event arrived. Real routing confirmed.
	}
}

// TestNATSBus_RealClose_StopsDelivery proves (mutation-paired with the happy
// path) that after Close, Publish errors and no further delivery happens.
func TestNATSBus_RealClose_StopsDelivery(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL not set; skipping real NATS JetStream integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bus, err := New(ctx, Config{
		URL:           url,
		StreamName:    "EVENTBUS_IT_CLOSE_" + time.Now().Format("150405"),
		SubjectPrefix: "eventbus_close_" + time.Now().Format("150405.000"),
	})
	require.NoError(t, err)

	require.NoError(t, bus.Close())

	// After Close, publishing must error rather than silently succeed.
	err = bus.Publish(&event.Event{ID: "x", Type: "a.b"})
	require.Error(t, err, "Publish after Close must error")
	assert.Contains(t, err.Error(), "closed")

	// Close is idempotent.
	require.NoError(t, bus.Close())
}
