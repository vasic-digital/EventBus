// Round-245 challenge runner for EventBus.
//
// Builds the bilingual fixture set from tests/fixtures/i18n/payloads.json,
// publishes each event through a real EventBus instance, verifies the
// delivered Metadata bytes match the source bytes byte-for-byte, and
// reports per-event PASS/FAIL with captured runtime evidence per
// Article XI §11.9 and CONST-035 anti-bluff invariants.
//
// Anti-bluff invariants enforced by this runner:
//
//   - No metadata-only / grep-only PASS. Every PASS line is preceded by
//     the actual event ID, the actual delivered payload, and the actual
//     delivered metadata as observed on the subscriber channel.
//   - Failing to deliver, byte-corrupting a Metadata value, or losing an
//     event silently is a hard FAIL — exit non-zero.
//   - The runner runs in process, real EventBus, real goroutines — no
//     mocks, no stubs, no "for now" placeholders.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"digital.vasic.eventbus/pkg/bus"
	"digital.vasic.eventbus/pkg/event"
)

type fixtureEvent struct {
	Type     string            `json:"type"`
	Source   string            `json:"source"`
	Locale   string            `json:"locale"`
	Payload  map[string]string `json:"payload"`
	Metadata map[string]string `json:"metadata"`
}

type fixtureFile struct {
	Events []fixtureEvent `json:"events"`
}

func main() {
	fixturePath := flag.String("fixtures", "", "path to payloads.json")
	flag.Parse()

	if *fixturePath == "" {
		// Default to repository-relative path.
		exe, _ := os.Executable()
		_ = exe
		*fixturePath = filepath.Join(
			"tests", "fixtures", "i18n", "payloads.json",
		)
	}

	raw, err := os.ReadFile(*fixturePath)
	if err != nil {
		fail("cannot read fixtures: %v", err)
	}
	var ff fixtureFile
	if err := json.Unmarshal(raw, &ff); err != nil {
		fail("cannot parse fixtures: %v", err)
	}
	if len(ff.Events) == 0 {
		fail("fixtures contain zero events")
	}

	b := bus.New(nil)
	defer b.Close()

	sub := b.SubscribeAll()
	defer sub.Cancel()

	pass := 0
	failures := 0

	for _, fe := range ff.Events {
		e := event.New(event.Type(fe.Type), fe.Source, fe.Payload)
		for k, v := range fe.Metadata {
			e = e.WithMetadata(k, v)
		}
		b.Publish(e)

		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)
		select {
		case got := <-sub.Channel:
			if got == nil {
				fmt.Printf("FAIL [%s] nil event delivered\n", fe.Locale)
				failures++
				cancel()
				continue
			}
			if string(got.Type) != fe.Type {
				fmt.Printf(
					"FAIL [%s] type drift: want=%q got=%q\n",
					fe.Locale, fe.Type, string(got.Type),
				)
				failures++
				cancel()
				continue
			}
			if !metadataEquals(got.Metadata, fe.Metadata) {
				fmt.Printf(
					"FAIL [%s] metadata byte-drift: want=%v got=%v\n",
					fe.Locale, fe.Metadata, got.Metadata,
				)
				failures++
				cancel()
				continue
			}
			payloadJSON, _ := json.Marshal(got.Payload)
			metaJSON, _ := json.Marshal(got.Metadata)
			fmt.Printf(
				"PASS [%s] id=%s type=%s source=%s payload=%s metadata=%s\n",
				fe.Locale, got.ID, got.Type, got.Source,
				string(payloadJSON), string(metaJSON),
			)
			pass++
		case <-ctx.Done():
			fmt.Printf(
				"FAIL [%s] timeout waiting for event type=%s\n",
				fe.Locale, fe.Type,
			)
			failures++
		}
		cancel()
	}

	fmt.Printf("\nSummary: %d PASS, %d FAIL of %d total\n",
		pass, failures, len(ff.Events))
	if failures > 0 {
		os.Exit(1)
	}
}

func metadataEquals(got, want map[string]string) bool {
	if len(got) < len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "runner-error: "+format+"\n", args...)
	os.Exit(2)
}
