package eventbus_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBus_Publish_NilEvent(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	// Should not panic
	b.Publish(nil)
}

func TestEventBus_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := b.Subscribe("test.concurrent")
			// Immediately unsubscribe
			sub.Cancel()
		}()
	}
	wg.Wait()

	// After all subscribe/unsubscribe, no subscribers should remain
	assert.Equal(t, 0, b.SubscriberCount("test.concurrent"))
}

func TestEventBus_PublishAfterClose(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)

	sub := b.Subscribe("test.event")
	_ = b.Close()

	// Publish after close should not panic
	e := event.New("test.event", "test", "payload")
	b.Publish(e)

	// Channel should be closed
	_, ok := <-sub.Channel
	assert.False(t, ok, "channel should be closed after bus close")
}

func TestEventBus_EmptyEventType(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe(event.Type(""))
	e := event.New(event.Type(""), "source", "payload")

	b.Publish(e)

	select {
	case received := <-sub.Channel:
		assert.Equal(t, event.Type(""), received.Type)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event with empty type")
	}
}

func TestEventBus_PublishWithNoSubscribers(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	// Should not panic or error
	e := event.New("orphan.event", "source", "payload")
	b.Publish(e)

	metrics := b.Metrics()
	assert.Equal(t, int64(1), metrics.EventsPublished)
	assert.Equal(t, int64(0), metrics.EventsDelivered)
}

func TestEventBus_ManyHandlersForSameEvent(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	const numSubscribers = 50
	subs := make([]*event.Subscription, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subs[i] = b.Subscribe("popular.event")
	}

	e := event.New("popular.event", "source", "payload")
	b.Publish(e)

	var received int64
	var wg sync.WaitGroup
	for _, sub := range subs {
		wg.Add(1)
		go func(s *event.Subscription) {
			defer wg.Done()
			select {
			case <-s.Channel:
				atomic.AddInt64(&received, 1)
			case <-time.After(time.Second):
			}
		}(sub)
	}
	wg.Wait()

	assert.Equal(t, int64(numSubscribers), received,
		"all subscribers should receive the event")
}

func TestEventBus_SubscribeAllReceivesEverything(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	sub := b.SubscribeAll()

	events := []event.Type{"type.a", "type.b", "type.c"}
	for _, et := range events {
		b.Publish(event.New(et, "source", nil))
	}

	for range events {
		select {
		case <-sub.Channel:
			// received
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for event on SubscribeAll")
		}
	}
}

func TestEventBus_DoubleClose(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)

	err := b.Close()
	assert.NoError(t, err)

	// Second close should not panic or error
	err = b.Close()
	assert.NoError(t, err)
}

func TestEventBus_SubscribeAfterClose(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	_ = b.Close()

	// Subscribe after close should return a closed channel
	sub := b.Subscribe("test.event")
	_, ok := <-sub.Channel
	assert.False(t, ok, "channel from subscribe-after-close should be closed")
}

func TestEventBus_Wait_ContextCancelled(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := b.Wait(ctx, "never.happens")
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestEventBus_Wait_Timeout(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := b.Wait(ctx, "never.happens")
	assert.Error(t, err)
}

func TestEventBus_ConcurrentPublish(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("concurrent.event")

	const numPublishers = 50
	var wg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := event.New("concurrent.event", "source", i)
			b.Publish(e)
		}(i)
	}

	received := 0
	done := make(chan struct{})
	go func() {
		for range sub.Channel {
			received++
			if received >= numPublishers {
				close(done)
				return
			}
		}
	}()

	wg.Wait()

	select {
	case <-done:
		assert.Equal(t, numPublishers, received)
	case <-time.After(5 * time.Second):
		t.Fatalf("only received %d/%d events", received, numPublishers)
	}
}

func TestEventBus_UnsubscribeByChannel(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("test.event")
	assert.Equal(t, 1, b.SubscriberCount("test.event"))

	b.UnsubscribeByChannel(sub.Channel)
	assert.Equal(t, 0, b.SubscriberCount("test.event"))
}

func TestEvent_WithMetadata_NilMap(t *testing.T) {
	t.Parallel()
	// Create event and verify WithMetadata initializes map if nil
	e := &event.Event{
		Type:     "test",
		Metadata: nil,
	}
	e.WithMetadata("key", "value")
	assert.Equal(t, "value", e.Metadata["key"])
}

func TestEvent_New_FieldsPopulated(t *testing.T) {
	t.Parallel()
	e := event.New("test.type", "test-source", map[string]string{"data": "value"})

	assert.NotEmpty(t, e.ID)
	assert.Equal(t, event.Type("test.type"), e.Type)
	assert.Equal(t, "test-source", e.Source)
	assert.NotNil(t, e.Payload)
	assert.NotZero(t, e.Timestamp)
	assert.NotEmpty(t, e.TraceID)
	assert.NotNil(t, e.Metadata)
}

func TestEvent_WithTraceID(t *testing.T) {
	t.Parallel()
	e := event.New("test", "source", nil)
	result := e.WithTraceID("custom-trace-id")

	assert.Equal(t, "custom-trace-id", e.TraceID)
	assert.Same(t, e, result, "WithTraceID should return same event for chaining")
}

func TestSubscription_Cancel_Nil(t *testing.T) {
	t.Parallel()
	// Subscription with nil cancel should not panic
	sub := event.NewSubscription("id", nil, nil, nil)
	sub.Cancel() // should not panic
}

func TestEventBus_Metrics_InitialState(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	m := b.Metrics()
	assert.Equal(t, int64(0), m.EventsPublished)
	assert.Equal(t, int64(0), m.EventsDelivered)
	assert.Equal(t, int64(0), m.EventsDropped)
	assert.Equal(t, int64(0), m.SubscribersActive)
	assert.Equal(t, int64(0), m.SubscribersTotal)
}

func TestEventBus_SubscribeMultiple(t *testing.T) {
	t.Parallel()
	b := bus.New(nil)
	defer func() { _ = b.Close() }()

	sub := b.SubscribeMultiple("type.a", "type.b")

	b.Publish(event.New("type.a", "source", nil))
	b.Publish(event.New("type.b", "source", nil))
	b.Publish(event.New("type.c", "source", nil)) // should not be received

	received := 0
	timeout := time.After(time.Second)
	for received < 2 {
		select {
		case <-sub.Channel:
			received++
		case <-timeout:
			t.Fatal("timeout waiting for events")
		}
	}

	assert.Equal(t, 2, received)
}

func TestEventBus_DroppedEvents(t *testing.T) {
	t.Parallel()
	cfg := &bus.Config{
		BufferSize:      1,
		PublishTimeout:  time.Nanosecond, // very short timeout
		CleanupInterval: time.Minute,
		MaxSubscribers:  100,
	}
	b := bus.New(cfg)
	defer func() { _ = b.Close() }()

	_ = b.Subscribe("test.event")

	// Publish many events quickly -- some should be dropped
	for i := 0; i < 100; i++ {
		b.Publish(event.New("test.event", "source", i))
	}

	metrics := b.Metrics()
	assert.Greater(t, metrics.EventsPublished, int64(0))
	// With buffer=1 and nanosecond timeout, drops are likely
	require.True(t,
		metrics.EventsDelivered+metrics.EventsDropped == metrics.EventsPublished,
		"delivered + dropped should equal published",
	)
}
