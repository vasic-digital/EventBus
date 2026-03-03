package benchmark

import (
	"testing"
	"time"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
	"digital.vasic.eventbus/pkg/middleware"
)

func BenchmarkEventPublish(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	eb := bus.New(&bus.Config{
		BufferSize:      100000,
		PublishTimeout:  10 * time.Millisecond,
		CleanupInterval: 1 * time.Minute,
	})
	defer eb.Close()

	_ = eb.Subscribe("bench.event")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish(event.New("bench.event", "bench", i))
	}
}

func BenchmarkEventPublishNoSubscribers(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	eb := bus.New(nil)
	defer eb.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.Publish(event.New("no.sub", "bench", i))
	}
}

func BenchmarkEventCreate(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = event.New("bench.event", "bench", nil)
	}
}

func BenchmarkFilterByType(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	f := filter.ByType("target.type")
	e := event.New("target.type", "src", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f(e)
	}
}

func BenchmarkFilterByPrefix(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	f := filter.ByPrefix("system.")
	e := event.New("system.health.check", "src", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f(e)
	}
}

func BenchmarkFilterComposite(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	f := filter.And(
		filter.ByPrefix("system."),
		filter.BySource("core"),
		filter.HasMetadata("priority"),
	)
	e := event.New("system.event", "core", nil)
	e.WithMetadata("priority", "high")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f(e)
	}
}

func BenchmarkMiddlewareChain(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	chain := middleware.Chain(
		middleware.Enrich("key1", "val1"),
		middleware.Enrich("key2", "val2"),
		middleware.Timestamp(),
	)
	e := event.New("bench", "src", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chain(e)
	}
}

func BenchmarkSubscribeUnsubscribe(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark test in short mode")
	}

	eb := bus.New(nil)
	defer eb.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub := eb.Subscribe("bench.sub")
		sub.Cancel()
	}
}
