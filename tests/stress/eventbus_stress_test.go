package stress

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
)

// Resource limit: GOMAXPROCS=2 recommended for stress tests

func TestStress_ConcurrentPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(&bus.Config{
		BufferSize:      10000,
		PublishTimeout:  100 * time.Millisecond,
		CleanupInterval: 1 * time.Minute,
		MaxSubscribers:  200,
	})
	defer b.Close()

	sub := b.Subscribe("stress.event")

	const goroutines = 100
	const eventsPerGoroutine = 10
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				e := event.New("stress.event", fmt.Sprintf("producer-%d", id), j)
				b.Publish(e)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	metrics := b.Metrics()
	assert.Equal(t, int64(goroutines*eventsPerGoroutine), metrics.EventsPublished)

	_ = sub
}

func TestStress_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	const goroutines = 50
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			topic := event.Type(fmt.Sprintf("topic.%d", id%10))
			sub := b.Subscribe(topic)
			time.Sleep(10 * time.Millisecond)
			sub.Cancel()
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 0, b.TotalSubscribers())
}

func TestStress_ConcurrentPublishAndSubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(&bus.Config{
		BufferSize:      5000,
		PublishTimeout:  50 * time.Millisecond,
		CleanupInterval: 1 * time.Minute,
		MaxSubscribers:  200,
	})
	defer b.Close()

	const publishers = 50
	const subscribers = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	receivedCount := 0

	wg.Add(subscribers)
	for i := 0; i < subscribers; i++ {
		go func() {
			defer wg.Done()
			sub := b.SubscribeAllWithFilter(filter.All())
			defer sub.Cancel()

			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			for {
				select {
				case _, ok := <-sub.Channel:
					if !ok {
						return
					}
					mu.Lock()
					receivedCount++
					mu.Unlock()
				case <-timer.C:
					return
				}
			}
		}()
	}

	wg.Add(publishers)
	for i := 0; i < publishers; i++ {
		go func(id int) {
			defer wg.Done()
			e := event.New("concurrent.event", fmt.Sprintf("pub-%d", id), nil)
			b.Publish(e)
		}(i)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, receivedCount > 0,
		"should have received some events")
}

func TestStress_HighThroughputWithFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(&bus.Config{
		BufferSize:      10000,
		PublishTimeout:  50 * time.Millisecond,
		CleanupInterval: 1 * time.Minute,
	})
	defer b.Close()

	f := filter.ByPrefix("important.")
	sub := b.SubscribeAllWithFilter(f)

	const total = 100
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func(id int) {
			defer wg.Done()
			var eventType event.Type
			if id%2 == 0 {
				eventType = "important.event"
			} else {
				eventType = "noise.event"
			}
			b.Publish(event.New(eventType, "src", id))
		}(i)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	received := 0
drain:
	for {
		select {
		case <-sub.Channel:
			received++
		default:
			break drain
		}
	}

	assert.True(t, received > 0 && received <= total/2+1,
		"should only receive 'important' events, got %d", received)
}

func TestStress_MetricsConcurrentAccess(t *testing.T) {
	// bluff-scan: no-assert-ok (concurrency test — go test -race catches data races; absence of panic == correctness)
	if testing.Short() {
		t.Skip("skipping stress test in short mode")  // SKIP-OK: #short-mode
	}

	b := bus.New(nil)
	defer b.Close()

	const goroutines = 80
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			b.Publish(event.New("metrics.test", "src", nil))
			_ = b.Metrics()
			_ = b.TotalSubscribers()
		}()
	}

	wg.Wait()
}

func TestStress_RapidCreateDestroyBus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")  // SKIP-OK: #short-mode
	}

	const goroutines = 50
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			b := bus.New(nil)
			sub := b.Subscribe("test")
			b.Publish(event.New("test", "src", nil))
			sub.Cancel()
			_ = b.Close()
		}()
	}

	wg.Wait()
}
