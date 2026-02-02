// Package bus provides the core EventBus implementation for pub/sub
// event-driven communication.
package bus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"digital.vasic.eventbus/pkg/event"
	"digital.vasic.eventbus/pkg/filter"
	"digital.vasic.eventbus/pkg/middleware"
)

// Config holds configuration for the event bus.
type Config struct {
	BufferSize      int           // Buffer size for subscriber channels
	PublishTimeout  time.Duration // Timeout for publishing to subscribers
	CleanupInterval time.Duration // Interval for cleaning up dead subscribers
	MaxSubscribers  int           // Maximum subscribers per event type
}

// DefaultConfig returns default bus configuration.
func DefaultConfig() *Config {
	return &Config{
		BufferSize:      1000,
		PublishTimeout:  10 * time.Millisecond,
		CleanupInterval: 30 * time.Second,
		MaxSubscribers:  100,
	}
}

// Metrics tracks event bus statistics.
type Metrics struct {
	EventsPublished   int64
	EventsDelivered   int64
	EventsDropped     int64
	SubscribersActive int64
	SubscribersTotal  int64
}

// subscriber represents an internal event subscriber.
type subscriber struct {
	id      string
	channel chan *event.Event
	filter  filter.Filter
	types   []event.Type
	closed  bool
	mu      sync.RWMutex
}

func (s *subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.channel)
	}
}

func (s *subscriber) trySend(e *event.Event, timeout time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case s.channel <- e:
		return true
	case <-timer.C:
		return false
	}
}

// EventBus provides publish/subscribe for system events.
type EventBus struct {
	subscribers map[event.Type][]*subscriber
	allSubs     []*subscriber
	mu          sync.RWMutex
	config      *Config
	metrics     *Metrics
	middlewares []middleware.Middleware
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
}

// New creates a new event bus with the given configuration.
func New(config *Config) *EventBus {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	b := &EventBus{
		subscribers: make(map[event.Type][]*subscriber),
		allSubs:     make([]*subscriber, 0),
		config:      config,
		metrics:     &Metrics{},
		ctx:         ctx,
		cancel:      cancel,
	}

	go b.cleanupLoop()

	return b
}

// Use adds middleware to the event bus. Middleware are applied in order
// to every published event before delivery to subscribers.
func (b *EventBus) Use(mw ...middleware.Middleware) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.middlewares = append(b.middlewares, mw...)
}

// Publish sends an event to all matching subscribers.
func (b *EventBus) Publish(e *event.Event) {
	if e == nil {
		return
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return
	}

	// Apply middleware
	mws := make([]middleware.Middleware, len(b.middlewares))
	copy(mws, b.middlewares)

	// Copy subscriber slices to avoid races
	origSubs := b.subscribers[e.Type]
	subs := make([]*subscriber, len(origSubs))
	copy(subs, origSubs)

	allSubs := make([]*subscriber, len(b.allSubs))
	copy(allSubs, b.allSubs)
	b.mu.RUnlock()

	// Apply middleware chain
	for _, mw := range mws {
		e = mw(e)
		if e == nil {
			return // Event was dropped by middleware
		}
	}

	atomic.AddInt64(&b.metrics.EventsPublished, 1)

	for _, sub := range subs {
		b.publishToSubscriber(sub, e)
	}
	for _, sub := range allSubs {
		b.publishToSubscriber(sub, e)
	}
}

func (b *EventBus) publishToSubscriber(sub *subscriber, e *event.Event) {
	if sub.filter != nil && !sub.filter(e) {
		return
	}

	if sub.trySend(e, b.config.PublishTimeout) {
		atomic.AddInt64(&b.metrics.EventsDelivered, 1)
	} else {
		atomic.AddInt64(&b.metrics.EventsDropped, 1)
	}
}

// PublishAsync publishes an event asynchronously.
func (b *EventBus) PublishAsync(e *event.Event) {
	go b.Publish(e)
}

// Subscribe subscribes to events of a specific type.
func (b *EventBus) Subscribe(eventType event.Type) *event.Subscription {
	return b.SubscribeWithFilter(eventType, nil)
}

// SubscribeWithFilter subscribes to a specific event type with a filter.
func (b *EventBus) SubscribeWithFilter(
	eventType event.Type, f filter.Filter,
) *event.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		ch := make(chan *event.Event)
		close(ch)
		return event.NewSubscription("", nil, ch, nil)
	}

	sub := &subscriber{
		id:      uuid.New().String(),
		channel: make(chan *event.Event, b.config.BufferSize),
		filter:  f,
		types:   []event.Type{eventType},
	}

	b.subscribers[eventType] = append(b.subscribers[eventType], sub)
	atomic.AddInt64(&b.metrics.SubscribersActive, 1)
	atomic.AddInt64(&b.metrics.SubscribersTotal, 1)

	cancel := func() { b.unsubscribe(sub) }
	return event.NewSubscription(
		sub.id, sub.types, sub.channel, cancel,
	)
}

// SubscribeMultiple subscribes to multiple event types.
func (b *EventBus) SubscribeMultiple(
	types ...event.Type,
) *event.Subscription {
	return b.SubscribeMultipleWithFilter(nil, types...)
}

// SubscribeMultipleWithFilter subscribes to multiple types with a filter.
func (b *EventBus) SubscribeMultipleWithFilter(
	f filter.Filter, types ...event.Type,
) *event.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		ch := make(chan *event.Event)
		close(ch)
		return event.NewSubscription("", nil, ch, nil)
	}

	sub := &subscriber{
		id:      uuid.New().String(),
		channel: make(chan *event.Event, b.config.BufferSize),
		filter:  f,
		types:   types,
	}

	for _, eventType := range types {
		b.subscribers[eventType] = append(
			b.subscribers[eventType], sub,
		)
	}

	atomic.AddInt64(&b.metrics.SubscribersActive, 1)
	atomic.AddInt64(&b.metrics.SubscribersTotal, 1)

	cancel := func() { b.unsubscribe(sub) }
	return event.NewSubscription(sub.id, sub.types, sub.channel, cancel)
}

// SubscribeAll subscribes to all event types.
func (b *EventBus) SubscribeAll() *event.Subscription {
	return b.SubscribeAllWithFilter(nil)
}

// SubscribeAllWithFilter subscribes to all events with a filter.
func (b *EventBus) SubscribeAllWithFilter(
	f filter.Filter,
) *event.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		ch := make(chan *event.Event)
		close(ch)
		return event.NewSubscription("", nil, ch, nil)
	}

	sub := &subscriber{
		id:      uuid.New().String(),
		channel: make(chan *event.Event, b.config.BufferSize),
		filter:  f,
	}

	b.allSubs = append(b.allSubs, sub)
	atomic.AddInt64(&b.metrics.SubscribersActive, 1)
	atomic.AddInt64(&b.metrics.SubscribersTotal, 1)

	cancel := func() { b.unsubscribe(sub) }
	return event.NewSubscription(sub.id, nil, sub.channel, cancel)
}

// unsubscribe removes a subscriber from the bus.
func (b *EventBus) unsubscribe(sub *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove from type-specific subscribers
	for eventType, subs := range b.subscribers {
		for i, s := range subs {
			if s.id == sub.id {
				s.close()
				b.subscribers[eventType] = append(
					subs[:i], subs[i+1:]...,
				)
				atomic.AddInt64(&b.metrics.SubscribersActive, -1)
				return
			}
		}
	}

	// Remove from all-event subscribers
	for i, s := range b.allSubs {
		if s.id == sub.id {
			s.close()
			b.allSubs = append(b.allSubs[:i], b.allSubs[i+1:]...)
			atomic.AddInt64(&b.metrics.SubscribersActive, -1)
			return
		}
	}
}

// UnsubscribeByChannel removes a subscriber by its channel reference.
func (b *EventBus) UnsubscribeByChannel(ch <-chan *event.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for eventType, subs := range b.subscribers {
		for i, sub := range subs {
			if sub.channel == ch {
				sub.close()
				b.subscribers[eventType] = append(
					subs[:i], subs[i+1:]...,
				)
				atomic.AddInt64(&b.metrics.SubscribersActive, -1)
				return
			}
		}
	}

	for i, sub := range b.allSubs {
		if sub.channel == ch {
			sub.close()
			b.allSubs = append(b.allSubs[:i], b.allSubs[i+1:]...)
			atomic.AddInt64(&b.metrics.SubscribersActive, -1)
			return
		}
	}
}

// Wait blocks until an event of the specified type is received or context
// is cancelled.
func (b *EventBus) Wait(
	ctx context.Context, eventType event.Type,
) (*event.Event, error) {
	sub := b.Subscribe(eventType)
	defer sub.Cancel()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e, ok := <-sub.Channel:
		if !ok {
			return nil, fmt.Errorf("event bus closed")
		}
		return e, nil
	}
}

// WaitMultiple waits for any event from the specified types.
func (b *EventBus) WaitMultiple(
	ctx context.Context, types ...event.Type,
) (*event.Event, error) {
	sub := b.SubscribeMultiple(types...)
	defer sub.Cancel()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e, ok := <-sub.Channel:
		if !ok {
			return nil, fmt.Errorf("event bus closed")
		}
		return e, nil
	}
}

func (b *EventBus) cleanupLoop() {
	interval := b.config.CleanupInterval
	if interval <= 0 {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.cleanup()
		}
	}
}

func (b *EventBus) cleanup() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for eventType, subs := range b.subscribers {
		active := make([]*subscriber, 0, len(subs))
		for _, sub := range subs {
			sub.mu.RLock()
			if !sub.closed {
				active = append(active, sub)
			}
			sub.mu.RUnlock()
		}
		b.subscribers[eventType] = active
	}

	active := make([]*subscriber, 0, len(b.allSubs))
	for _, sub := range b.allSubs {
		sub.mu.RLock()
		if !sub.closed {
			active = append(active, sub)
		}
		sub.mu.RUnlock()
	}
	b.allSubs = active
}

// Metrics returns a snapshot of current bus metrics.
func (b *EventBus) Metrics() *Metrics {
	return &Metrics{
		EventsPublished:   atomic.LoadInt64(&b.metrics.EventsPublished),
		EventsDelivered:   atomic.LoadInt64(&b.metrics.EventsDelivered),
		EventsDropped:     atomic.LoadInt64(&b.metrics.EventsDropped),
		SubscribersActive: atomic.LoadInt64(&b.metrics.SubscribersActive),
		SubscribersTotal:  atomic.LoadInt64(&b.metrics.SubscribersTotal),
	}
}

// SubscriberCount returns the number of subscribers for an event type.
func (b *EventBus) SubscriberCount(eventType event.Type) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers[eventType])
}

// TotalSubscribers returns the total number of active subscribers.
func (b *EventBus) TotalSubscribers() int {
	return int(atomic.LoadInt64(&b.metrics.SubscribersActive))
}

// Close shuts down the event bus and closes all subscriber channels.
func (b *EventBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.closed = true
	b.cancel()

	for _, subs := range b.subscribers {
		for _, sub := range subs {
			sub.close()
		}
	}
	for _, sub := range b.allSubs {
		sub.close()
	}

	return nil
}
