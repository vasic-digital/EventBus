// Package filter provides event filtering capabilities for the EventBus.
// Filters determine which events are delivered to subscribers.
package filter

import (
	"path"
	"strings"

	"digital.vasic.eventbus/pkg/event"
)

// Filter is a function that determines whether an event should be delivered.
// Returns true if the event should pass through.
type Filter func(*event.Event) bool

// ByType returns a filter that matches events of the specified type.
func ByType(eventType event.Type) Filter {
	return func(e *event.Event) bool {
		return e.Type == eventType
	}
}

// ByTypes returns a filter that matches events of any of the specified types.
func ByTypes(types ...event.Type) Filter {
	typeSet := make(map[event.Type]struct{}, len(types))
	for _, t := range types {
		typeSet[t] = struct{}{}
	}
	return func(e *event.Event) bool {
		_, ok := typeSet[e.Type]
		return ok
	}
}

// BySource returns a filter that matches events from the specified source.
func BySource(source string) Filter {
	return func(e *event.Event) bool {
		return e.Source == source
	}
}

// ByGlob returns a filter that matches event types against a glob pattern.
// Uses path.Match syntax: "*" matches any non-separator sequence,
// "?" matches a single character.
// Example: "provider.*" matches "provider.registered", "provider.health.changed"
func ByGlob(pattern string) Filter {
	return func(e *event.Event) bool {
		matched, err := path.Match(pattern, string(e.Type))
		if err != nil {
			return false
		}
		return matched
	}
}

// ByPrefix returns a filter that matches event types starting with the prefix.
// Example: "provider." matches "provider.registered", "provider.health.changed"
func ByPrefix(prefix string) Filter {
	return func(e *event.Event) bool {
		return strings.HasPrefix(string(e.Type), prefix)
	}
}

// ByMetadata returns a filter that matches events with a specific metadata value.
func ByMetadata(key, value string) Filter {
	return func(e *event.Event) bool {
		if e.Metadata == nil {
			return false
		}
		return e.Metadata[key] == value
	}
}

// HasMetadata returns a filter that matches events containing the specified
// metadata key.
func HasMetadata(key string) Filter {
	return func(e *event.Event) bool {
		if e.Metadata == nil {
			return false
		}
		_, ok := e.Metadata[key]
		return ok
	}
}

// And combines multiple filters with logical AND.
// All filters must pass for the event to be delivered.
func And(filters ...Filter) Filter {
	return func(e *event.Event) bool {
		for _, f := range filters {
			if !f(e) {
				return false
			}
		}
		return true
	}
}

// Or combines multiple filters with logical OR.
// At least one filter must pass for the event to be delivered.
func Or(filters ...Filter) Filter {
	return func(e *event.Event) bool {
		for _, f := range filters {
			if f(e) {
				return true
			}
		}
		return false
	}
}

// Not negates a filter.
func Not(f Filter) Filter {
	return func(e *event.Event) bool {
		return !f(e)
	}
}

// All returns a filter that always passes.
func All() Filter {
	return func(e *event.Event) bool {
		return true
	}
}

// None returns a filter that never passes.
func None() Filter {
	return func(e *event.Event) bool {
		return false
	}
}
