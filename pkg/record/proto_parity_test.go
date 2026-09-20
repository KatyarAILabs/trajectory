// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"reflect"
	"strings"
	"testing"

	v1 "github.com/trajectory-project/trajectory/gen/go/trajectory/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The wire contract (spec/proto) and the storage contract (this package) are
// separate Go types on purpose: generated protobuf structs carry internal state
// fields and pointer semantics that make poor Parquet rows.
//
// That separation is only safe if it cannot become a divergence. This test
// walks every proto descriptor and asserts the Parquet row type agrees on field
// names, order and cardinality. Adding a field to one side without the other
// fails here.
func TestProtoParity(t *testing.T) {
	cases := []struct {
		name string
		msg  proto.Message
		row  any
	}{
		{"Episode", &v1.Episode{}, Episode{}},
		{"Step", &v1.Step{}, Step{}},
		{"Blob", &v1.Blob{}, Blob{}},
		{"Outcome", &v1.Outcome{}, Outcome{}},
		{"Label", &v1.Label{}, Label{}},
		{"Reward", &v1.Reward{}, Reward{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := protoFields(tc.msg.ProtoReflect().Descriptor())
			got := parquetFields(reflect.TypeOf(tc.row))

			if len(want) != len(got) {
				t.Fatalf("field count: proto has %d %v, Parquet row has %d %v",
					len(want), names(want), len(got), names(got))
			}
			for i := range want {
				if want[i] != got[i] {
					t.Errorf("field %d: proto has %+v, Parquet row has %+v", i, want[i], got[i])
				}
			}
		})
	}
}

type field struct {
	Name string
	Card string // "single" | "pointer" | "list" | "map"
}

func names(fs []field) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Name
	}
	return out
}

func protoFields(d protoreflect.MessageDescriptor) []field {
	fds := d.Fields()
	out := make([]field, 0, fds.Len())
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		card := "single"
		switch {
		case fd.IsMap():
			card = "map"
		case fd.IsList():
			card = "list"
		case fd.HasPresence():
			card = "pointer"
		}
		out = append(out, field{Name: string(fd.Name()), Card: card})
	}
	return out
}

func parquetFields(t reflect.Type) []field {
	out := make([]field, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		tag := sf.Tag.Get("parquet")
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")

		card := "single"
		switch sf.Type.Kind() {
		case reflect.Ptr:
			card = "pointer"
		case reflect.Slice:
			card = "list"
		case reflect.Map:
			card = "map"
		}
		out = append(out, field{Name: name, Card: card})
	}
	return out
}

// Every enum-tagged column must be populated from the proto vocabulary through
// a conversion in enums.go, never by writing the proto's prefixed name into the
// file. This asserts the stored values are the spec §7 strings.
func TestEnumVocabulary(t *testing.T) {
	if got := StatusFromProto(v1.Status_STATUS_TIMED_OUT); got != StatusTimedOut {
		t.Errorf("status: got %q want %q", got, StatusTimedOut)
	}
	if got := KindFromProto(v1.Kind_KIND_RETRIEVAL); got != KindRetrieval {
		t.Errorf("kind: got %q want %q", got, KindRetrieval)
	}
	if got := EncodingFromProto(v1.Encoding_ENCODING_ZSTD); got != EncodingZstd {
		t.Errorf("encoding: got %q want %q", got, EncodingZstd)
	}

	// A source that cannot say must not be coerced into a boolean (F-4.4).
	if got := TrainableFromProto(v1.Trainable_TRAINABLE_UNSPECIFIED); got != TrainableUnknown {
		t.Errorf("unspecified trainable: got %q want %q", got, TrainableUnknown)
	}
	// An unrecognised span kind is retained, never dropped (F-2.5).
	if got := KindFromProto(v1.Kind(9999)); got != KindOther {
		t.Errorf("unknown kind: got %q want %q", got, KindOther)
	}
}

// The reserved tables are created empty and never written in v1 (§2.3, §7.4).
func TestReservedTablesAreNotWritten(t *testing.T) {
	written := map[string]bool{}
	for _, name := range Written() {
		written[name] = true
	}
	for _, reserved := range []string{TableOutcomes, TableLabels, TableRewards} {
		if written[reserved] {
			t.Errorf("%q is reserved surface but appears in Written(); "+
				"adding a writer requires reopening spec §2.2", reserved)
		}
	}
	if len(written) != 3 {
		t.Errorf("Written() has %d tables, want 3 (episodes, steps, blobs)", len(written))
	}
}
