package bus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
	"digital.vasic.eventbus/pkg/middleware"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Nil / Empty Edge Cases ---

func TestEventBus_Publish_NilEvent(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	// Should not panic
	b.Publish(nil)
}

func TestEventBus_Publish_EmptyEventType(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("")
	defer sub.Cancel()

	e := event.New("", "test-source", "payload")
	b.Publish(e)

	select {
	case received := <-sub.Channel:
		assert.Equal(t, event.Type(""), received.Type)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected to receive event with empty type")
	}
}

func TestEventBus_Subscribe_EmptyType(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("")
	defer sub.Cancel()
	assert.NotNil(t, sub)
	assert.NotNil(t, sub.Channel)
}

// --- Publish With No Subscribers ---

func TestEventBus_Publish_NoSubscribers(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	// Should not block or panic
	e := event.New("test.event", "source", "data")
	b.Publish(e)

	m := b.Metrics()
	assert.Equal(t, int64(1), m.EventsPublished)
	assert.Equal(t, int64(0), m.EventsDelivered)
}

// --- Concurrent Subscribe / Unsubscribe ---

func TestEventBus_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := b.Subscribe("concurrent.test")
			time.Sleep(time.Millisecond)
			sub.Cancel()
		}()
	}

	// Also publish concurrently
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish(event.New("concurrent.test", "src", nil))
		}()
	}

	wg.Wait()
	// Should not panic or deadlock
}

// --- Many Handlers Same Event ---

func TestEventBus_ManySubscribersSameEvent(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	const numSubs = 50
	subs := make([]*event.Subscription, numSubs)
	for i := 0; i < numSubs; i++ {
		subs[i] = b.Subscribe("same.event")
	}

	e := event.New("same.event", "source", "data")
	b.Publish(e)

	for i, sub := range subs {
		select {
		case received := <-sub.Channel:
			assert.Equal(t, "same.event", string(received.Type), "subscriber %d", i)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("subscriber %d did not receive event", i)
		}
		sub.Cancel()
	}
}

// --- Middleware That Panics ---

func TestEventBus_MiddlewarePanic_DoesNotCrashBus(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	var panicRecovered atomic.Int64

	// Use a recovery middleware wrapping a processor that panics
	recoverMw := middleware.RecoverWithProcessor(
		func(v interface{}) {
			panicRecovered.Add(1)
		},
		func(e *event.Event) *event.Event {
			panic("test panic in middleware")
		},
	)
	b.Use(recoverMw)

	sub := b.Subscribe("panic.test")
	defer sub.Cancel()

	e := event.New("panic.test", "source", "data")
	b.Publish(e)

	// Event should be dropped by the recover middleware
	select {
	case <-sub.Channel:
		t.Fatal("should not receive event after middleware panic drops it")
	case <-time.After(50 * time.Millisecond):
		// Expected: event was dropped
	}

	assert.Equal(t, int64(1), panicRecovered.Load())
}

// --- Subscribe After Close ---

func TestEventBus_SubscribeAfterClose(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	b.Close()

	sub := b.Subscribe("test.event")
	// Channel should be closed
	_, ok := <-sub.Channel
	assert.False(t, ok, "channel should be closed on closed bus")
}

func TestEventBus_PublishAfterClose(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	b.Close()

	// Should not panic
	b.Publish(event.New("test.event", "source", nil))
}

// --- Double Close ---

func TestEventBus_DoubleClose(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)

	err := b.Close()
	assert.NoError(t, err)

	err = b.Close()
	assert.NoError(t, err)
}

// --- Wait With Cancelled Context ---

func TestEventBus_Wait_CancelledContext(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := b.Wait(ctx, "test.event")
	assert.Error(t, err)
}

func TestEventBus_Wait_Timeout(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := b.Wait(ctx, "never.happens")
	assert.Error(t, err)
}

// --- WaitMultiple With No Types ---

func TestEventBus_WaitMultiple_NoTypes(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := b.WaitMultiple(ctx)
	assert.Error(t, err)
}

// --- SubscribeAll ---

func TestEventBus_SubscribeAll_ReceivesAllTypes(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	sub := b.SubscribeAll()
	defer sub.Cancel()

	types := []event.Type{"type.a", "type.b", "type.c"}
	for _, et := range types {
		b.Publish(event.New(et, "src", nil))
	}

	for i := 0; i < 3; i++ {
		select {
		case <-sub.Channel:
			// Good
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("did not receive event %d", i)
		}
	}
}

// --- Filter Edge Cases ---

func TestEventBus_SubscribeWithFilter_NilEvent(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	// Filter that always rejects
	sub := b.SubscribeWithFilter("test.event", filter.None())
	defer sub.Cancel()

	b.Publish(event.New("test.event", "src", nil))

	select {
	case <-sub.Channel:
		t.Fatal("filter.None() should reject all events")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}
}

// --- SubscriberCount ---

func TestEventBus_SubscriberCount_Empty(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	assert.Equal(t, 0, b.SubscriberCount("nonexistent"))
	assert.Equal(t, 0, b.TotalSubscribers())
}

func TestEventBus_SubscriberCount_AfterUnsubscribe(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("test.event")
	assert.Equal(t, 1, b.SubscriberCount("test.event"))

	sub.Cancel()
	assert.Equal(t, 0, b.SubscriberCount("test.event"))
}

// --- Metrics Snapshot ---

func TestEventBus_Metrics_Snapshot(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	m := b.Metrics()
	assert.Equal(t, int64(0), m.EventsPublished)
	assert.Equal(t, int64(0), m.EventsDelivered)
	assert.Equal(t, int64(0), m.EventsDropped)
}

// --- Config Edge Cases ---

func TestEventBus_NilConfig_UsesDefaults(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	// Should work fine with defaults
	sub := b.Subscribe("test")
	defer sub.Cancel()

	b.Publish(event.New("test", "src", nil))

	select {
	case <-sub.Channel:
		// Good
	case <-time.After(100 * time.Millisecond):
		t.Fatal("should receive event with nil config")
	}
}

func TestEventBus_ZeroBufferSize(t *testing.T) {
	t.Parallel()
	cfg := &bus.Config{
		BufferSize:      0,
		PublishTimeout:  10 * time.Millisecond,
		CleanupInterval: 30 * time.Second,
		MaxSubscribers:  100,
	}
	b := bus.New(cfg)
	defer b.Close()

	// With zero buffer, publish should use timeout
	sub := b.Subscribe("test")
	defer sub.Cancel()

	b.Publish(event.New("test", "src", nil))

	// Might be dropped due to zero buffer, that's OK
	m := b.Metrics()
	assert.Equal(t, int64(1), m.EventsPublished)
}

// --- PublishAsync ---

func TestEventBus_PublishAsync(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("async.test")
	defer sub.Cancel()

	b.PublishAsync(event.New("async.test", "src", "data"))

	select {
	case received := <-sub.Channel:
		assert.Equal(t, "async.test", string(received.Type))
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive async event")
	}
}

// --- UnsubscribeByChannel edge case ---

func TestEventBus_UnsubscribeByChannel_NonexistentChannel(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	ch := make(chan *event.Event)
	// Should not panic even with a channel that was never subscribed
	b.UnsubscribeByChannel(ch)
}

// --- Subscription Cancel Idempotent ---

func TestSubscription_Cancel_Idempotent(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("test")
	sub.Cancel()
	// Second cancel should not panic
	sub.Cancel()
}

// --- Subscription Cancel nil ---

func TestSubscription_Cancel_NilCancel(t *testing.T) {
	t.Parallel()
	sub := event.NewSubscription("id", nil, nil, nil)
	// Should not panic with nil cancel func
	sub.Cancel()
}

// --- Event creation edge cases ---

func TestEvent_New_EmptyFields(t *testing.T) {
	t.Parallel()
	e := event.New("", "", nil)
	require.NotNil(t, e)
	assert.NotEmpty(t, e.ID)
	assert.Empty(t, string(e.Type))
	assert.Empty(t, e.Source)
	assert.Nil(t, e.Payload)
	assert.NotNil(t, e.Metadata)
}

func TestEvent_WithMetadata_NilMetadata(t *testing.T) {
	t.Parallel()
	e := &event.Event{}
	e.WithMetadata("key", "value")
	assert.Equal(t, "value", e.Metadata["key"])
}
