package nats

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"digital.vasic.eventbus/pkg/event"
)

// --- Unit: event JSON round-trip preserves every field ---

func TestEventJSONRoundTrip_AllFieldsPreserved(t *testing.T) {
	ts := time.Date(2026, 5, 31, 12, 30, 0, 0, time.UTC)
	orig := &event.Event{
		ID:     "evt-123",
		Type:   "provider.registered",
		Source: "registry-service",
		Payload: map[string]interface{}{
			"name":  "gpu-node-1",
			"count": float64(7), // float64: JSON numbers decode as float64
		},
		Timestamp: ts,
		TraceID:   "trace-abc",
		Metadata:  map[string]string{"region": "eu-west", "tier": "gold"},
	}

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	var got event.Event
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, orig.ID, got.ID, "ID must round-trip")
	assert.Equal(t, orig.Type, got.Type, "Type must round-trip")
	assert.Equal(t, orig.Source, got.Source, "Source must round-trip")
	assert.True(t, orig.Timestamp.Equal(got.Timestamp), "Timestamp must round-trip")
	assert.Equal(t, orig.TraceID, got.TraceID, "TraceID must round-trip")
	assert.Equal(t, orig.Metadata, got.Metadata, "Metadata must round-trip")
	// Payload round-trips as a generic JSON value (documented limitation).
	assert.Equal(t, orig.Payload, got.Payload, "Payload (generic JSON) must round-trip")
}

// Mutation-paired (§1.1): prove the round-trip assertion is real by mutating a
// field on the wire and asserting the decoded event DIFFERS. If round-trip were
// a no-op bluff, this would fail.
func TestEventJSONRoundTrip_MutationDetectsTamper(t *testing.T) {
	orig := event.New("cache.hit", "cache", map[string]interface{}{"k": "v"})

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	// Tamper the serialized bytes: change the ID.
	var asMap map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &asMap))
	asMap["ID"] = "TAMPERED"
	tampered, err := json.Marshal(asMap)
	require.NoError(t, err)

	var got event.Event
	require.NoError(t, json.Unmarshal(tampered, &got))

	assert.NotEqual(t, orig.ID, got.ID,
		"a tampered ID MUST NOT equal the original — proves the assertion has teeth")
	assert.Equal(t, "TAMPERED", got.ID)
}

// --- Unit: subject derivation from event.Type ---

func TestSubjectFor_DotNotationMapsToTokens(t *testing.T) {
	b := &Bus{subjectPrefix: "eventbus"}

	assert.Equal(t, "eventbus.provider.registered",
		b.subjectFor("provider.registered"))
	assert.Equal(t, "eventbus.cache.hit", b.subjectFor("cache.hit"))
	assert.Equal(t, "eventbus.system.startup", b.subjectFor("system.startup"))
}

func TestSanitizeType(t *testing.T) {
	cases := []struct {
		in   event.Type
		want string
	}{
		{"provider.registered", "provider.registered"},
		{"", "_"},                               // empty type
		{"a.b.c", "a.b.c"},                       // multi-token
		{"with space", "with_space"},             // space illegal in NATS token
		{"wild*card", "wild_card"},               // '*' is a NATS wildcard
		{"greater>than", "greater_than"},         // '>' is a NATS wildcard
		{"a..b", "a._.b"},                         // empty middle token preserved
		{".leading", "_.leading"},                // leading dot
		{"trailing.", "trailing._"},              // trailing dot
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, sanitizeType(c.in),
			"sanitizeType(%q)", c.in)
	}
}

// Mutation-paired (§1.1): prove that DIFFERENT event types derive DIFFERENT
// subjects — i.e. routing actually depends on the type. If subjectFor ignored
// its input (a routing bluff), these would collide and the test fails.
func TestSubjectFor_DifferentTypesDoNotCollide(t *testing.T) {
	b := &Bus{subjectPrefix: "eventbus"}

	a := b.subjectFor("provider.registered")
	c := b.subjectFor("provider.deregistered")
	d := b.subjectFor("cache.hit")

	assert.NotEqual(t, a, c, "distinct types must map to distinct subjects")
	assert.NotEqual(t, a, d, "distinct types must map to distinct subjects")
	assert.NotEqual(t, c, d, "distinct types must map to distinct subjects")
}

func TestSubjectFor_RespectsPrefix(t *testing.T) {
	b1 := &Bus{subjectPrefix: "alpha"}
	b2 := &Bus{subjectPrefix: "beta"}
	assert.NotEqual(t, b1.subjectFor("x.y"), b2.subjectFor("x.y"),
		"different prefixes must isolate subjects")
}

// --- Unit: New input validation (no server needed) ---

func TestNew_RequiresURL(t *testing.T) {
	_, err := New(t.Context(), Config{})
	require.Error(t, err, "empty URL must error")
	assert.Contains(t, err.Error(), "URL is required")
}
