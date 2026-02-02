package bus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
	"digital.vasic.eventbus/pkg/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, 1000, config.BufferSize)
	assert.Equal(t, 10*time.Millisecond, config.PublishTimeout)
	assert.Equal(t, 30*time.Second, config.CleanupInterval)
	assert.Equal(t, 100, config.MaxSubscribers)
}

func TestNew(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	assert.NotNil(t, b)
	assert.NotNil(t, b.config)
	assert.NotNil(t, b.metrics)
}

func TestNew_WithConfig(t *testing.T) {
	config := &Config{
		BufferSize:     500,
		PublishTimeout: 50 * time.Millisecond,
	}

	b := New(config)
	defer func() { _ = b.Close() }()

	assert.Equal(t, 500, b.config.BufferSize)
	assert.Equal(t, 50*time.Millisecond, b.config.PublishTimeout)
}

func TestEventBus_Subscribe(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("test.event")
	assert.NotNil(t, sub)
	assert.Equal(t, 1, b.SubscriberCount("test.event"))
}

func TestEventBus_Publish(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("test.event")
	e := event.New("test.event", "test", nil)

	b.Publish(e)

	select {
	case received := <-sub.Channel:
		assert.Equal(t, e.ID, received.ID)
		assert.Equal(t, event.Type("test.event"), received.Type)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestEventBus_Publish_NilEvent(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	// Should not panic
	b.Publish(nil)
}

func TestEventBus_PublishAsync(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("test.event")
	e := event.New("test.event", "test", nil)

	b.PublishAsync(e)

	select {
	case received := <-sub.Channel:
		assert.Equal(t, e.ID, received.ID)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestEventBus_SubscribeWithFilter(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	f := filter.BySource("important")
	sub := b.SubscribeWithFilter("test.event", f)

	// This event should be filtered out
	b.Publish(event.New("test.event", "unimportant", nil))

	// This event should pass through
	importantEvent := event.New("test.event", "important", nil)
	b.Publish(importantEvent)

	select {
	case received := <-sub.Channel:
		assert.Equal(t, "important", received.Source)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestEventBus_SubscribeMultiple(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.SubscribeMultiple("type.a", "type.b")

	b.Publish(event.New("type.a", "test", nil))
	b.Publish(event.New("type.b", "test", nil))

	count := 0
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case <-sub.Channel:
			count++
			if count >= 2 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	assert.Equal(t, 2, count)
}

func TestEventBus_SubscribeAll(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.SubscribeAll()

	b.Publish(event.New("type.a", "test", nil))
	b.Publish(event.New("type.b", "test", nil))
	b.Publish(event.New("type.c", "test", nil))

	count := 0
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case <-sub.Channel:
			count++
			if count >= 3 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	assert.Equal(t, 3, count)
}

func TestEventBus_SubscribeAllWithFilter(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	f := filter.ByPrefix("cache.")
	sub := b.SubscribeAllWithFilter(f)

	b.Publish(event.New("provider.registered", "test", nil))
	b.Publish(event.New("cache.hit", "test", nil))
	b.Publish(event.New("cache.miss", "test", nil))

	count := 0
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case <-sub.Channel:
			count++
			if count >= 2 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	assert.Equal(t, 2, count)
}

func TestEventBus_Unsubscribe_Cancel(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("test.event")
	assert.Equal(t, 1, b.SubscriberCount("test.event"))

	sub.Cancel()
	assert.Equal(t, 0, b.SubscriberCount("test.event"))
}

func TestEventBus_UnsubscribeByChannel(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("test.event")
	assert.Equal(t, 1, b.SubscriberCount("test.event"))

	b.UnsubscribeByChannel(sub.Channel)
	assert.Equal(t, 0, b.SubscriberCount("test.event"))
}

func TestEventBus_Close(t *testing.T) {
	b := New(nil)

	sub := b.Subscribe("test.event")

	err := b.Close()
	assert.NoError(t, err)

	_, ok := <-sub.Channel
	assert.False(t, ok)
}

func TestEventBus_Close_Multiple(t *testing.T) {
	b := New(nil)

	err := b.Close()
	assert.NoError(t, err)

	err = b.Close()
	assert.NoError(t, err)
}

func TestEventBus_Subscribe_AfterClose(t *testing.T) {
	b := New(nil)
	_ = b.Close()

	sub := b.Subscribe("test.event")

	_, ok := <-sub.Channel
	assert.False(t, ok)
}

func TestEventBus_Metrics(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	sub := b.Subscribe("test.event")

	for i := 0; i < 5; i++ {
		b.Publish(event.New("test.event", "test", nil))
	}

	for i := 0; i < 5; i++ {
		<-sub.Channel
	}

	metrics := b.Metrics()
	assert.Equal(t, int64(5), metrics.EventsPublished)
	assert.Equal(t, int64(5), metrics.EventsDelivered)
	assert.Equal(t, int64(1), metrics.SubscribersActive)
}

func TestEventBus_TotalSubscribers(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	assert.Equal(t, 0, b.TotalSubscribers())

	b.Subscribe("type.a")
	b.Subscribe("type.b")

	assert.Equal(t, 2, b.TotalSubscribers())
}

func TestEventBus_Wait(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	go func() {
		time.Sleep(50 * time.Millisecond)
		b.Publish(event.New("test.event", "test", nil))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	e, err := b.Wait(ctx, "test.event")
	require.NoError(t, err)
	assert.Equal(t, event.Type("test.event"), e.Type)
}

func TestEventBus_Wait_Timeout(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	ctx, cancel := context.WithTimeout(
		context.Background(), 50*time.Millisecond,
	)
	defer cancel()

	_, err := b.Wait(ctx, "test.event")
	assert.Error(t, err)
}

func TestEventBus_WaitMultiple(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	go func() {
		time.Sleep(50 * time.Millisecond)
		b.Publish(event.New("type.b", "test", nil))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	e, err := b.WaitMultiple(ctx, "type.a", "type.b")
	require.NoError(t, err)
	assert.Equal(t, event.Type("type.b"), e.Type)
}

func TestEventBus_Use_Middleware(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	mc := middleware.NewMetricsCounter()
	b.Use(mc.Middleware())
	b.Use(middleware.Enrich("bus", "test-bus"))

	sub := b.Subscribe("test.event")
	b.Publish(event.New("test.event", "test", nil))

	select {
	case received := <-sub.Channel:
		assert.Equal(t, "test-bus", received.Metadata["bus"])
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	assert.Equal(t, int64(1), mc.GetTotal())
}

func TestEventBus_Use_Middleware_Drop(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	// Middleware that drops all events
	b.Use(func(e *event.Event) *event.Event { return nil })

	sub := b.Subscribe("test.event")
	b.Publish(event.New("test.event", "test", nil))

	select {
	case <-sub.Channel:
		t.Fatal("event should have been dropped")
	case <-time.After(100 * time.Millisecond):
		// Expected: no event delivered
	}

	metrics := b.Metrics()
	assert.Equal(t, int64(0), metrics.EventsPublished)
}

func TestEventBus_ConcurrentPublish(t *testing.T) {
	b := New(&Config{BufferSize: 1000})
	defer func() { _ = b.Close() }()

	sub := b.SubscribeAll()

	var received atomic.Int64
	go func() {
		for range sub.Channel {
			received.Add(1)
		}
	}()

	const numPublishers = 10
	const numEvents = 100

	for i := 0; i < numPublishers; i++ {
		go func() {
			for j := 0; j < numEvents; j++ {
				b.Publish(event.New("test.event", "test", nil))
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	assert.GreaterOrEqual(t, received.Load(), int64(numPublishers*numEvents/4))
}

func TestEventBus_ConcurrentSubscribe(t *testing.T) {
	b := New(nil)
	defer func() { _ = b.Close() }()

	const numSubscribers = 20
	done := make(chan struct{})

	for i := 0; i < numSubscribers; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			b.Subscribe("test.event")
		}()
	}

	for i := 0; i < numSubscribers; i++ {
		<-done
	}

	assert.Equal(t, numSubscribers, b.SubscriberCount("test.event"))
}

func TestEventBus_Publish_AfterClose(t *testing.T) {
	b := New(nil)
	_ = b.Close()

	// Should not panic
	b.Publish(event.New("test.event", "test", nil))
}

func TestEventBus_SubscribeMultiple_AfterClose(t *testing.T) {
	b := New(nil)
	_ = b.Close()

	sub := b.SubscribeMultiple("type.a", "type.b")
	_, ok := <-sub.Channel
	assert.False(t, ok)
}

func TestEventBus_SubscribeAll_AfterClose(t *testing.T) {
	b := New(nil)
	_ = b.Close()

	sub := b.SubscribeAll()
	_, ok := <-sub.Channel
	assert.False(t, ok)
}
