package security

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
	"digital.vasic.eventbus/pkg/middleware"
)

func TestSecurity_NilEventHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	assert.NotPanics(t, func() {
		b.Publish(nil)
	})
}

func TestSecurity_NilFilterHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	e := event.New("test", "src", nil)

	typeFilter := filter.ByType("")
	assert.False(t, typeFilter(e))

	metaFilter := filter.ByMetadata("key", "val")
	noMeta := &event.Event{Type: "test"}
	assert.False(t, metaFilter(noMeta))

	hasMeta := filter.HasMetadata("key")
	assert.False(t, hasMeta(noMeta))
}

func TestSecurity_LargePayloadHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("large.payload")

	largePayload := strings.Repeat("X", 1024*1024)
	e := event.New("large.payload", "test", largePayload)
	b.Publish(e)

	select {
	case received := <-sub.Channel:
		payload, ok := received.Payload.(string)
		assert.True(t, ok)
		assert.Len(t, payload, 1024*1024)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestSecurity_MiddlewarePanicRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	var panicMsg interface{}
	recover := middleware.RecoverWithProcessor(
		func(r interface{}) { panicMsg = r },
		func(e *event.Event) *event.Event {
			panic("middleware panic!")
		},
	)
	b.Use(recover)

	sub := b.Subscribe("panic.test")
	b.Publish(event.New("panic.test", "test", nil))

	select {
	case <-sub.Channel:
		t.Fatal("should not receive event after panic in middleware")
	case <-time.After(200 * time.Millisecond):
	}

	assert.Equal(t, "middleware panic!", panicMsg)
}

func TestSecurity_RateLimitMiddleware(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	b.Use(middleware.RateLimit(5))

	sub := b.Subscribe("ratelimit.test")

	for i := 0; i < 20; i++ {
		b.Publish(event.New("ratelimit.test", "flood", i))
	}

	time.Sleep(200 * time.Millisecond)

	received := 0
	for {
		select {
		case <-sub.Channel:
			received++
		default:
			goto done
		}
	}
done:
	assert.True(t, received <= 10,
		"rate limiter should drop excess events, got %d", received)
}

func TestSecurity_FilterCombinators(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	e := event.New("test.event", "service-a", nil)
	e.WithMetadata("priority", "high")

	noneFilter := filter.None()
	assert.False(t, noneFilter(e))

	allFilter := filter.All()
	assert.True(t, allFilter(e))

	notFilter := filter.Not(filter.None())
	assert.True(t, notFilter(e))

	orFilter := filter.Or(
		filter.BySource("unknown"),
		filter.BySource("service-a"),
	)
	assert.True(t, orFilter(e))

	andFilter := filter.And(
		filter.BySource("service-a"),
		filter.HasMetadata("priority"),
	)
	assert.True(t, andFilter(e))

	failAnd := filter.And(
		filter.BySource("service-a"),
		filter.BySource("service-b"),
	)
	assert.False(t, failAnd(e))
}

func TestSecurity_DoubleCloseHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)

	err1 := b.Close()
	assert.NoError(t, err1)

	err2 := b.Close()
	assert.NoError(t, err2)
}

func TestSecurity_PublishAfterClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping security test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	_ = b.Close()

	assert.NotPanics(t, func() {
		b.Publish(event.New("test", "src", nil))
	})
}
