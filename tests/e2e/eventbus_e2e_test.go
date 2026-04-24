package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
	"digital.vasic.eventbus/pkg/middleware"
)

func TestE2E_PublishSubscribeWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	var received []*event.Event
	var mu sync.Mutex

	sub := b.Subscribe("workflow.step.completed")

	go func() {
		for e := range sub.Channel {
			mu.Lock()
			received = append(received, e)
			mu.Unlock()
		}
	}()

	steps := []string{"init", "validate", "process", "complete"}
	for _, step := range steps {
		e := event.New("workflow.step.completed", "workflow-engine", step)
		e.WithMetadata("step", step)
		b.Publish(e)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 4)
	for i, e := range received {
		assert.Equal(t, steps[i], e.Metadata["step"])
	}
}

func TestE2E_EventFilteringPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	criticalFilter := filter.And(
		filter.ByPrefix("system."),
		filter.ByMetadata("severity", "critical"),
	)

	sub := b.SubscribeAllWithFilter(criticalFilter)

	events := []*event.Event{
		event.New("system.error", "core", nil),
		event.New("system.warning", "core", nil),
		event.New("user.login", "auth", nil),
		event.New("system.crash", "core", nil),
	}
	events[0].WithMetadata("severity", "critical")
	events[1].WithMetadata("severity", "warning")
	events[2].WithMetadata("severity", "info")
	events[3].WithMetadata("severity", "critical")

	for _, e := range events {
		b.Publish(e)
	}

	received := 0
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case e := <-sub.Channel:
			assert.Equal(t, "critical", e.Metadata["severity"])
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

func TestE2E_MiddlewareChainProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	counter := middleware.NewMetricsCounter()
	chain := middleware.Chain(
		middleware.Enrich("env", "e2e"),
		middleware.Enrich("version", "1.0"),
		counter.Middleware(),
		middleware.Timestamp(),
	)
	b.Use(chain)

	sub := b.Subscribe("chain.test")
	b.Publish(event.New("chain.test", "test", nil))

	select {
	case e := <-sub.Channel:
		assert.Equal(t, "e2e", e.Metadata["env"])
		assert.Equal(t, "1.0", e.Metadata["version"])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	assert.Equal(t, int64(1), counter.GetTotal())
}

func TestE2E_AsyncPublishAndCollect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	sub := b.Subscribe("async.event")

	const eventCount = 20
	for i := 0; i < eventCount; i++ {
		b.PublishAsync(event.New("async.event", "async-producer", i))
	}

	received := 0
	timeout := time.After(5 * time.Second)
loop:
	for {
		select {
		case <-sub.Channel:
			received++
			if received == eventCount {
				break loop
			}
		case <-timeout:
			break loop
		}
	}
	assert.Equal(t, eventCount, received)
}

func TestE2E_BusShutdownGraceful(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	sub := b.Subscribe("shutdown.test")

	b.Publish(event.New("shutdown.test", "test", "before-close"))

	select {
	case e := <-sub.Channel:
		assert.Equal(t, "before-close", e.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}

	err := b.Close()
	require.NoError(t, err)

	_, ok := <-sub.Channel
	assert.False(t, ok, "channel should be closed after bus.Close()")
}

func TestE2E_WaitMultipleEventTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	go func() {
		time.Sleep(100 * time.Millisecond)
		b.Publish(event.New("signal.b", "emitter", "B"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	e, err := b.WaitMultiple(ctx, "signal.a", "signal.b", "signal.c")
	require.NoError(t, err)
	assert.Equal(t, event.Type("signal.b"), e.Type)
	assert.Equal(t, "B", e.Payload)
}

func TestE2E_EventBuilderChaining(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")  // SKIP-OK: #short-mode
	}

	e := event.New("build.completed", "ci", map[string]string{"status": "success"}).
		WithTraceID("trace-abc-123").
		WithMetadata("build_id", "42").
		WithMetadata("branch", "main")

	assert.Equal(t, "trace-abc-123", e.TraceID)
	assert.Equal(t, "42", e.Metadata["build_id"])
	assert.Equal(t, "main", e.Metadata["branch"])
	assert.NotEmpty(t, e.ID)
	assert.False(t, e.Timestamp.IsZero())
}
