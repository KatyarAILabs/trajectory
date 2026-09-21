// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"strings"
	"testing"

	"github.com/trajectory-project/trajectory/pkg/record"
)

// F-10.3: the published JSON Schema advertises both the protobuf JSON names
// and the spec §7 values for every enum. A decoder that accepted only one would
// make the published contract a promise the collector does not keep.
func TestBothEnumSpellingsAccepted(t *testing.T) {
	cases := []struct {
		proto, spec, want string
		fn                func(string) (string, error)
	}{
		{"STATUS_COMPLETE", "complete", record.StatusComplete, NormaliseStatus},
		{"STATUS_TIMED_OUT", "timed_out", record.StatusTimedOut, NormaliseStatus},
		{"STATUS_EVICTED", "evicted", record.StatusEvicted, NormaliseStatus},
		{"KIND_LLM", "llm", record.KindLLM, NormaliseKind},
		{"KIND_RETRIEVAL", "retrieval", record.KindRetrieval, NormaliseKind},
		{"TRAINABLE_TRUE", "true", record.TrainableTrue, NormaliseTrainable},
		{"ENCODING_ZSTD", "zstd", record.EncodingZstd, NormaliseEncoding},
	}

	for _, tc := range cases {
		for _, in := range []string{tc.proto, tc.spec} {
			got, err := tc.fn(in)
			if err != nil {
				t.Errorf("%q: unexpected error %v", in, err)
				continue
			}
			if got != tc.want {
				t.Errorf("%q -> %q, want %q", in, got, tc.want)
			}
		}
	}
}

// UNSPECIFIED is a protobuf artefact, not a value a consumer should see.
func TestUnspecifiedBecomesEmpty(t *testing.T) {
	got, err := NormaliseStatus("STATUS_UNSPECIFIED")
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("STATUS_UNSPECIFIED -> %q, want empty", got)
	}
}

// F-4.4: a source that cannot say must not be coerced to "false".
func TestTrainableUnspecifiedBecomesUnknown(t *testing.T) {
	for _, in := range []string{"", "TRAINABLE_UNSPECIFIED", "unknown"} {
		got, err := NormaliseTrainable(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got != record.TrainableUnknown {
			t.Errorf("%q -> %q, want unknown", in, got)
		}
	}
}

// An invalid enum against a published schema is a producer bug; saying so is
// more useful than silently reclassifying the data.
func TestInvalidEnumIsAnError(t *testing.T) {
	_, err := NormaliseKind("banana")
	if err == nil {
		t.Fatal("invalid kind accepted")
	}
	if !strings.Contains(err.Error(), "banana") || !strings.Contains(err.Error(), "llm") {
		t.Errorf("error should name the bad value and the accepted set: %v", err)
	}
}

// protojson encodes 64-bit integers as strings; the schema says both are
// accepted, so both must decode.
func TestInt64AcceptsStringAndNumber(t *testing.T) {
	eps, err := DecodeEpisodes([]byte(`{
      "episode_id":"e1","status":"complete",
      "started_at":"1757000000000000",
      "steps":[{"kind":"llm","started_at":1757000000000000}]
    }`))
	if err != nil {
		t.Fatal(err)
	}
	if got := int64(eps[0].StartedAt); got != 1757000000000000 {
		t.Errorf("string-encoded started_at = %d", got)
	}
	if got := int64(eps[0].Steps[0].StartedAt); got != 1757000000000000 {
		t.Errorf("number-encoded started_at = %d", got)
	}
}

func TestDecodeSingleAndArray(t *testing.T) {
	single := `{"episode_id":"e1","steps":[{"kind":"llm"}]}`
	arr := `[{"episode_id":"e1","steps":[{"kind":"llm"}]},{"episode_id":"e2","steps":[{"kind":"tool"}]}]`

	if eps, err := DecodeEpisodes([]byte(single)); err != nil || len(eps) != 1 {
		t.Errorf("single: %d episodes, err %v", len(eps), err)
	}
	if eps, err := DecodeEpisodes([]byte(arr)); err != nil || len(eps) != 2 {
		t.Errorf("array: %d episodes, err %v", len(eps), err)
	}
	if _, err := DecodeEpisodes([]byte("  ")); err == nil {
		t.Error("empty body accepted")
	}
}

// UC-3: the native path is the only one that can carry token spans and
// trainable masks, because no tracing convention models them.
func TestTokenSpansSurvive(t *testing.T) {
	eps, err := DecodeEpisodes([]byte(`{
      "episode_id":"e1","status":"complete","started_at":1,
      "steps":[{
        "kind":"KIND_LLM","content":"hello world","trainable":"TRAINABLE_TRUE",
        "token_spans":[
          {"start":0,"end":5,"trainable":"false"},
          {"start":5,"end":11,"trainable":"TRAINABLE_TRUE"}
        ]
      }]
    }`))
	if err != nil {
		t.Fatal(err)
	}

	envs, err := eps[0].ToEnvelopes("native", "acme")
	if err != nil {
		t.Fatal(err)
	}
	step := envs[0].Step

	if step.Trainable != record.TrainableTrue {
		t.Errorf("trainable = %q, want true", step.Trainable)
	}
	if len(step.TokenSpans) != 2 {
		t.Fatalf("got %d token spans, want 2", len(step.TokenSpans))
	}
	if step.TokenSpans[0].Trainable != record.TrainableFalse {
		t.Errorf("span 0 trainable = %q, want false", step.TokenSpans[0].Trainable)
	}
	if step.TokenSpans[1].Start != 5 || step.TokenSpans[1].End != 11 {
		t.Errorf("span 1 = %+v", step.TokenSpans[1])
	}
}

// A reversed token span would silently corrupt a trainable mask.
func TestReversedTokenSpanRejected(t *testing.T) {
	eps, _ := DecodeEpisodes([]byte(`{
      "episode_id":"e1","steps":[{"kind":"llm","token_spans":[{"start":10,"end":2}]}]}`))
	_, err := eps[0].ToEnvelopes("native", "acme")
	if err == nil {
		t.Fatal("reversed token span accepted")
	}
}

// The tree must survive a native POST as it does an OTLP stream (F-3.4).
func TestParentLinksPreserved(t *testing.T) {
	eps, err := DecodeEpisodes([]byte(`{
      "episode_id":"e1","status":"complete","started_at":1,
      "steps":[
        {"kind":"llm","step_idx":0,"started_at":1},
        {"kind":"tool","step_idx":1,"parent_idx":0,"started_at":2},
        {"kind":"tool","step_idx":2,"parent_idx":0,"started_at":3}
      ]}`))
	if err != nil {
		t.Fatal(err)
	}
	envs, err := eps[0].ToEnvelopes("native", "acme")
	if err != nil {
		t.Fatal(err)
	}

	if len(envs) != 3 {
		t.Fatalf("got %d envelopes, want 3", len(envs))
	}
	// Both tool steps reference step 0 as their parent: a retry branch.
	if envs[1].ParentSpanID != envs[0].SpanID {
		t.Errorf("step 1 parent = %q, want %q", envs[1].ParentSpanID, envs[0].SpanID)
	}
	if envs[2].ParentSpanID != envs[0].SpanID {
		t.Errorf("step 2 parent = %q, want %q", envs[2].ParentSpanID, envs[0].SpanID)
	}
	// Only the last step closes the episode, so a partial POST does not
	// close it early.
	if envs[0].Terminal || envs[1].Terminal {
		t.Error("a non-final step carried the terminal marker")
	}
	if !envs[2].Terminal {
		t.Error("the final step did not carry the terminal marker")
	}
}

func TestMissingEpisodeIDRejected(t *testing.T) {
	eps, _ := DecodeEpisodes([]byte(`{"steps":[{"kind":"llm"}]}`))
	if _, err := eps[0].ToEnvelopes("native", "acme"); err == nil {
		t.Fatal("episode with no id accepted")
	}
}

func TestEmptyStepsRejected(t *testing.T) {
	eps, _ := DecodeEpisodes([]byte(`{"episode_id":"e1","steps":[]}`))
	if _, err := eps[0].ToEnvelopes("native", "acme"); err == nil {
		t.Fatal("episode with no steps accepted")
	}
}
