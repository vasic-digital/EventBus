package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
	"digital.vasic.eventbus/pkg/middleware"
)

func TestBusPublishSubscribeFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(&bus.Config{
		BufferSize:      100,
		PublishTimeout:  100 * time.Millisecond,
		CleanupInterval: 1 * time.Minute,
		MaxSubscribers:  50,
	})
	defer b.Close()

	sub := b.Subscribe("user.created")

	e := event.New("user.created", "auth-service", map[string]string{
		"user_id": "123",
	})
	b.Publish(e)

	select {
	case received := <-sub.Channel:
		assert.Equal(t, event.Type("user.created"), received.Type)
		assert.Equal(t, "auth-service", received.Source)
		payload, ok := received.Payload.(map[string]string)
		require.True(t, ok)
		assert.Equal(t, "123", payload["user_id"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestBusMultipleSubscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(nil)
	defer b.Close()

	sub1 := b.Subscribe("order.placed")
	sub2 := b.Subscribe("order.placed")
	sub3 := b.Subscribe("order.shipped")

	e := event.New("order.placed", "order-service", "order-1")
	b.Publish(e)

	for _, sub := range []*event.Subscription{sub1, sub2} {
		select {
		case received := <-sub.Channel:
			assert.Equal(t, event.Type("order.placed"), received.Type)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	}

	select {
	case <-sub3.Channel:
		t.Fatal("should not receive unrelated event")
	case <-time.After(100 * time.Millisecond):
	}

	assert.Equal(t, 2, b.SubscriberCount("order.placed"))
	assert.Equal(t, 1, b.SubscriberCount("order.shipped"))
}

func TestBusSubscribeAllWithFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(nil)
	defer b.Close()

	f := filter.BySource("payment-service")
	sub := b.SubscribeAllWithFilter(f)

	b.Publish(event.New("payment.success", "payment-service", nil))
	b.Publish(event.New("user.login", "auth-service", nil))
	b.Publish(event.New("payment.failed", "payment-service", nil))

	received := 0
	timeout := time.After(2 * time.Second)
	for received < 2 {
		select {
		case e := <-sub.Channel:
			assert.Equal(t, "payment-service", e.Source)
			received++
		case <-timeout:
			t.Fatalf("only received %d/2 events", received)
		}
	}
}

func TestBusMiddlewareIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(nil)
	defer b.Close()

	counter := middleware.NewMetricsCounter()
	b.Use(counter.Middleware())
	b.Use(middleware.Enrich("env", "integration-test"))

	sub := b.Subscribe("test.event")

	for i := 0; i < 5; i++ {
		b.Publish(event.New("test.event", "test", nil))
	}

	for i := 0; i < 5; i++ {
		select {
		case e := <-sub.Channel:
			assert.Equal(t, "integration-test", e.Metadata["env"])
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	}

	assert.Equal(t, int64(5), counter.GetTotal())
}

func TestBusWaitIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(nil)
	defer b.Close()

	go func() {
		time.Sleep(100 * time.Millisecond)
		b.Publish(event.New("system.ready", "boot", "ok"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	e, err := b.Wait(ctx, "system.ready")
	require.NoError(t, err)
	assert.Equal(t, event.Type("system.ready"), e.Type)
	assert.Equal(t, "ok", e.Payload)
}

func TestBusUnsubscribeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("ephemeral.event")
	assert.Equal(t, 1, b.SubscriberCount("ephemeral.event"))

	sub.Cancel()

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, b.SubscriberCount("ephemeral.event"))
}

func TestBusMetricsTracking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(&bus.Config{
		BufferSize:      2,
		PublishTimeout:  1 * time.Millisecond,
		CleanupInterval: 1 * time.Minute,
		MaxSubscribers:  10,
	})
	defer b.Close()

	sub := b.Subscribe("metric.event")

	for i := 0; i < 5; i++ {
		b.Publish(event.New("metric.event", "test", i))
	}

	time.Sleep(100 * time.Millisecond)

	metrics := b.Metrics()
	assert.Equal(t, int64(5), metrics.EventsPublished)
	assert.True(t, metrics.EventsDelivered > 0)
	assert.Equal(t, int64(1), metrics.SubscribersActive)

	_ = sub
}

func TestBusSubscribeMultipleTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	b := bus.New(nil)
	defer b.Close()

	sub := b.SubscribeMultiple("type.a", "type.b", "type.c")

	b.Publish(event.New("type.a", "src", nil))
	b.Publish(event.New("type.b", "src", nil))
	b.Publish(event.New("type.d", "src", nil))

	received := 0
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case <-sub.Channel:
			received++
			if received == 2 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}
	assert.Equal(t, 2, received)
}
