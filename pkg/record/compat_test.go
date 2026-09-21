// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// stepV010 is the steps row as a reader built before cost_usd existed would
// declare it: every column up to raw, and nothing after.
type stepV010 struct {
	EpisodeID     string            `parquet:"episode_id"`
	StepIdx       int32             `parquet:"step_idx"`
	ParentIdx     *int32            `parquet:"parent_idx,optional"`
	Attempt       int32             `parquet:"attempt"`
	Kind          string            `parquet:"kind,enum"`
	ContentInline *string           `parquet:"content_inline,optional"`
	Model         *string           `parquet:"model,optional"`
	Trainable     string            `parquet:"trainable,enum"`
	StartedAt     int64             `parquet:"started_at,timestamp(microsecond)"`
	Raw           map[string]string `parquet:"raw,optional"`
}

// F-10.4: a reader written against X.Y reads X.Z without modification.
//
// This is the promise the whole additive-only rule exists to keep, so it is
// tested directly: write a file with today's schema, including a column the old
// reader has never heard of, and read it with the old reader's struct.
func TestOlderReaderReadsNewerFile(t *testing.T) {
	content, model := `{"input":"hi"}`, "claude-opus-5"
	cost := 0.004
	parent := int32(0)

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Step](&buf)
	if _, err := w.Write([]Step{
		{EpisodeID: "e1", StepIdx: 0, Kind: KindLLM, ContentInline: &content,
			Model: &model, Trainable: TrainableTrue, StartedAt: 1758362400000000, CostUSD: &cost},
		{EpisodeID: "e1", StepIdx: 1, ParentIdx: &parent, Kind: KindTool,
			Trainable: TrainableUnknown, StartedAt: 1758362401000000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	rows, err := parquet.Read[stepV010](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("a reader built before cost_usd cannot read today's files: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("read %d rows, want 2", len(rows))
	}
	if rows[0].Model == nil || *rows[0].Model != model || *rows[0].ContentInline != content {
		t.Errorf("row 0 = %+v", rows[0])
	}
	if rows[1].ParentIdx == nil || *rows[1].ParentIdx != 0 || rows[1].Kind != KindTool {
		t.Errorf("row 1 = %+v", rows[1])
	}
}
