// Package nats provides a NATS JetStream-backed event bus that mirrors the
// core publish/subscribe semantics of pkg/bus, allowing EventBus to be used
// as a real, durable, distributed backend instead of an in-memory one.
//
// Events are serialized to JSON and published to a NATS subject derived from
// the event's dot-notation Type (which maps naturally onto NATS subject
// tokens). JetStream is used so published events are persisted in a stream
// and delivered durably to subscribers.
//
// Known limitation: Event.Payload is declared as interface{}. JSON has no way
// to preserve the original concrete Go type, so on the receiving side a
// payload that was published as a struct, map, or number arrives as a generic
// JSON value (map[string]interface{}, float64, string, []interface{}, etc.).
// Callers that need typed payloads should re-marshal the received payload into
// their concrete type, or carry a discriminator in Event.Type / Event.Metadata.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	natsgo "github.com/nats-io/nats.go"

	"digital.vasic.eventbus/pkg/event"
)

// Config holds configuration for the NATS-backed event bus.
type Config struct {
	// URL is the NATS server URL, e.g. "nats://127.0.0.1:4222". Required.
	URL string
	// StreamName is the JetStream stream that captures published events.
	// Defaults to "EVENTBUS" when empty.
	StreamName string
	// SubjectPrefix is prepended to every event subject so multiple logical
	// buses can share one NATS server without colliding. Defaults to
	// "eventbus" when empty. The wire subject is
	// "<SubjectPrefix>.<sanitized event.Type>".
	SubjectPrefix string
	// ConnectTimeout bounds the initial connection attempt. Defaults to 5s.
	ConnectTimeout time.Duration
}

const (
	defaultStreamName    = "EVENTBUS"
	defaultSubjectPrefix = "eventbus"
	defaultConnTimeout   = 5 * time.Second
)

// Bus is a NATS JetStream-backed event bus mirroring pkg/bus pub/sub
// semantics. It is safe for concurrent use by multiple goroutines.
type Bus struct {
	conn          *natsgo.Conn
	js            natsgo.JetStreamContext
	streamName    string
	subjectPrefix string

	mu     sync.Mutex
	subs   []*natsgo.Subscription
	closed bool
}

// New creates a NATS-backed event bus, connecting to cfg.URL and ensuring a
// JetStream stream exists that captures every subject under the configured
// prefix. The provided context bounds the connection setup.
func New(ctx context.Context, cfg Config) (*Bus, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("nats: Config.URL is required")
	}
	streamName := cfg.StreamName
	if streamName == "" {
		streamName = defaultStreamName
	}
	prefix := cfg.SubjectPrefix
	if prefix == "" {
		prefix = defaultSubjectPrefix
	}
	connTimeout := cfg.ConnectTimeout
	if connTimeout <= 0 {
		connTimeout = defaultConnTimeout
	}

	// Honour an already-cancelled context before dialing.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("nats: context error before connect: %w", err)
	}

	conn, err := natsgo.Connect(
		cfg.URL,
		natsgo.Timeout(connTimeout),
		natsgo.RetryOnFailedConnect(false),
	)
	if err != nil {
		return nil, fmt.Errorf("nats: connect %q: %w", cfg.URL, err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("nats: jetstream context: %w", err)
	}

	b := &Bus{
		conn:          conn,
		js:            js,
		streamName:    streamName,
		subjectPrefix: prefix,
	}

	if err := b.ensureStream(); err != nil {
		conn.Close()
		return nil, err
	}

	return b, nil
}

// ensureStream creates the JetStream stream (capturing every subject under the
// prefix) if it does not already exist. It is idempotent across processes.
func (b *Bus) ensureStream() error {
	wildcard := b.subjectPrefix + ".>"

	if _, err := b.js.StreamInfo(b.streamName); err == nil {
		// Stream already exists; nothing to create.
		return nil
	}

	_, err := b.js.AddStream(&natsgo.StreamConfig{
		Name:      b.streamName,
		Subjects:  []string{wildcard},
		Storage:   natsgo.FileStorage,
		Retention: natsgo.LimitsPolicy,
	})
	if err != nil {
		return fmt.Errorf("nats: add stream %q: %w", b.streamName, err)
	}
	return nil
}

// subjectFor derives the NATS subject for an event type. The dot-notation
// topic maps directly onto NATS subject tokens; tokens are sanitized so that
// NATS wildcard/separator characters (space, '*', '>') and empty tokens cannot
// break routing. An empty type maps to a single "_" token.
func (b *Bus) subjectFor(t event.Type) string {
	return b.subjectPrefix + "." + sanitizeType(t)
}

// sanitizeType converts an event.Type into a safe NATS subject suffix. It is
// exported-equivalent behaviour but kept unexported; tested directly.
func sanitizeType(t event.Type) string {
	raw := string(t)
	if raw == "" {
		return "_"
	}
	tokens := strings.Split(raw, ".")
	out := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		out = append(out, sanitizeToken(tok))
	}
	return strings.Join(out, ".")
}

// sanitizeToken replaces characters that are illegal or special in a NATS
// subject token with '_'. Empty tokens (e.g. from a leading/trailing/double
// dot) also become '_' so the token count — and thus routing — is preserved.
func sanitizeToken(tok string) string {
	if tok == "" {
		return "_"
	}
	var sb strings.Builder
	sb.Grow(len(tok))
	for _, r := range tok {
		switch r {
		case '*', '>', ' ', '\t', '\n', '\r', '.':
			sb.WriteByte('_')
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// Publish serializes the event to JSON and publishes it to the JetStream
// subject derived from the event's Type. It returns an error if the event is
// nil, the bus is closed, serialization fails, or JetStream rejects the
// message.
func (b *Bus) Publish(e *event.Event) error {
	if e == nil {
		return fmt.Errorf("nats: cannot publish nil event")
	}

	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return fmt.Errorf("nats: bus is closed")
	}

	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("nats: marshal event: %w", err)
	}

	subject := b.subjectFor(e.Type)
	if _, err := b.js.Publish(subject, data); err != nil {
		return fmt.Errorf("nats: publish to %q: %w", subject, err)
	}
	return nil
}

// Subscribe subscribes to events of the given type. It returns a receive-only
// channel that delivers decoded events, an unsubscribe function that stops
// delivery and closes the channel, and an error.
//
// Delivery uses an ephemeral JetStream push subscription. The provided context
// bounds the lifetime of the delivery goroutine: when it is cancelled (or the
// returned unsubscribe func is called, or the bus is closed) delivery stops and
// the channel is closed.
func (b *Bus) Subscribe(
	ctx context.Context, t event.Type,
) (<-chan *event.Event, func(), error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, nil, fmt.Errorf("nats: bus is closed")
	}
	b.mu.Unlock()

	subject := b.subjectFor(t)
	out := make(chan *event.Event)

	// DeliverNew so a subscription only receives events published after it is
	// established — mirroring the in-memory bus, where a subscriber never sees
	// events that predate it.
	sub, err := b.js.Subscribe(
		subject,
		func(msg *natsgo.Msg) {
			var e event.Event
			if decErr := json.Unmarshal(msg.Data, &e); decErr != nil {
				// Drop undecodable messages but ack so they are not redelivered
				// forever; an undecodable message is unrecoverable.
				_ = msg.Ack()
				return
			}
			select {
			case out <- &e:
				_ = msg.Ack()
			case <-ctx.Done():
				// Subscriber going away; let JetStream redeliver to a future
				// subscriber rather than silently dropping.
			}
		},
		natsgo.DeliverNew(),
		natsgo.AckExplicit(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("nats: subscribe to %q: %w", subject, err)
	}

	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = sub.Unsubscribe()
			b.removeSub(sub)
			close(out)
		})
	}

	// Stop delivery when the caller's context is cancelled.
	go func() {
		<-ctx.Done()
		stop()
	}()

	return out, stop, nil
}

// removeSub drops a subscription from the tracked slice.
func (b *Bus) removeSub(target *natsgo.Subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, s := range b.subs {
		if s == target {
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			return
		}
	}
}

// Close unsubscribes all active subscriptions and closes the NATS connection.
// It is idempotent.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	subs := b.subs
	b.subs = nil
	conn := b.conn
	b.mu.Unlock()

	for _, s := range subs {
		_ = s.Unsubscribe()
	}
	if conn != nil {
		conn.Close()
	}
	return nil
}
