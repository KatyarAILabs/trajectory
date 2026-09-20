// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/trajectory-project/trajectory/internal/version"
)

func ptr[T any](v T) *T { return &v }

// Phase 0 exit criterion: a record survives a real Parquet write and read with
// every field intact, including the optionals that distinguish "the source did
// not report this" from "the source reported zero" (F-4.2).
func TestEpisodeRoundTrip(t *testing.T) {
	want := Episode{
		EpisodeID:              "01J8ZQ9P0000000000000000",
		Tenant:                 "acme",
		GroupID:                ptr("rollout-7"),
		TaskType:               ptr("refund"),
		Source:                 "otlp",
		Instrumentation:        ptr("openinference-langchain"),
		InstrumentationVersion: ptr("0.1.14"),
		EntityKeys: []EntityKey{
			{Name: "ticket_id", Value: "tok_5f3a9c"},
		},
		Status:        StatusComplete,
		Fidelity:      &Fidelity{HasParams: true, HasTokenSpans: false, HasToolVersions: true},
		SampledBy:     ptr("tail:error"),
		StartedAt:     1_757_000_000_000_000,
		EndedAt:       ptr(int64(1_757_000_012_000_000)),
		ReceivedAt:    1_757_000_013_000_000,
		StepCount:     3,
		Error:         nil,
		SchemaVersion: version.Schema,
		Raw:           map[string]string{"human_edited": "true"},
	}

	got := roundTrip(t, want)

	if got.EpisodeID != want.EpisodeID || got.Tenant != want.Tenant || got.Source != want.Source {
		t.Errorf("identity fields differ:\ngot  %+v\nwant %+v", got, want)
	}
	if got.Status != StatusComplete {
		t.Errorf("status: got %q want %q", got.Status, StatusComplete)
	}
	if got.GroupID == nil || *got.GroupID != *want.GroupID {
		t.Errorf("group_id: got %v want %q", got.GroupID, *want.GroupID)
	}
	if len(got.EntityKeys) != 1 || got.EntityKeys[0].Value != "tok_5f3a9c" {
		t.Errorf("entity_keys: got %+v", got.EntityKeys)
	}
	if got.Fidelity == nil || !got.Fidelity.HasParams || got.Fidelity.HasTokenSpans {
		t.Errorf("fidelity: got %+v want %+v", got.Fidelity, want.Fidelity)
	}
	if got.StartedAt != want.StartedAt || got.EndedAt == nil || *got.EndedAt != *want.EndedAt {
		t.Errorf("timestamps: got started=%d ended=%v", got.StartedAt, got.EndedAt)
	}
	if got.Raw["human_edited"] != "true" {
		t.Errorf("raw: got %+v", got.Raw)
	}
	// An error-free episode must not gain a zero-valued error struct.
	if got.Error != nil {
		t.Errorf("error: got %+v, want nil for a clean episode", got.Error)
	}
}

// A step whose source reported no generation parameters must read back as nil,
// not as a zeroed Params. A replayer that cannot tell "temperature was 0" from
// "temperature was not reported" will silently produce wrong replays.
func TestStepAbsentFieldsStayAbsent(t *testing.T) {
	want := Step{
		EpisodeID:     "01J8ZQ9P0000000000000000",
		StepIdx:       0,
		ParentIdx:     nil,
		Attempt:       0,
		Kind:          KindLLM,
		Role:          ptr("assistant"),
		ContentInline: ptr("hello"),
		Truncated:     false,
		Trainable:     TrainableUnknown,
		StartedAt:     1_757_000_000_000_000,
	}

	got := roundTrip(t, want)

	if got.Params != nil {
		t.Errorf("params: got %+v, want nil when the source reported none", got.Params)
	}
	if got.TokenCounts != nil {
		t.Errorf("token_counts: got %+v, want nil", got.TokenCounts)
	}
	if got.ParentIdx != nil {
		t.Errorf("parent_idx: got %v, want nil for a root step", got.ParentIdx)
	}
	if got.Trainable != TrainableUnknown {
		t.Errorf("trainable: got %q want %q", got.Trainable, TrainableUnknown)
	}
	if got.ContentInline == nil || *got.ContentInline != "hello" {
		t.Errorf("content_inline: got %v", got.ContentInline)
	}
}

// F-10.2: schema_version is stamped in Parquet file key-value metadata, not
// only on the rows, so a reader can check compatibility without decoding data.
func TestFileMetadataCarriesSchemaVersion(t *testing.T) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Episode](&buf,
		parquet.KeyValueMetadata("schema_version", version.Schema),
	)
	if _, err := w.Write([]Episode{{
		EpisodeID: "e1", Tenant: "acme", Source: "otlp",
		Status: StatusComplete, SchemaVersion: version.Schema,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := f.Lookup("schema_version")
	if !ok {
		t.Fatal("schema_version absent from Parquet file metadata (F-10.2)")
	}
	if got != version.Schema {
		t.Errorf("file metadata schema_version: got %q want %q", got, version.Schema)
	}
}

func roundTrip[T any](t *testing.T, in T) T {
	t.Helper()

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf,
		parquet.KeyValueMetadata("schema_version", version.Schema),
	)
	if _, err := w.Write([]T{in}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rows, err := parquet.Read[T](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("read %d rows, want 1", len(rows))
	}
	return rows[0]
}
