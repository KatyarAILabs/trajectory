// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Command gen-jsonschema emits JSON Schema for the trajectory record types
// (F-10.3) from the protobuf descriptors.
//
// The proto is the single source of truth. Nothing here is hand-maintained:
// a second, hand-written schema would drift, and the whole point of F-10 is
// that a reader can trust the published contract.
//
// Run: go run ./tools/gen-jsonschema -out spec/jsonschema
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	v1 "github.com/trajectory-project/trajectory/gen/go/trajectory/v1"
	"github.com/trajectory-project/trajectory/internal/version"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func main() {
	out := flag.String("out", "spec/jsonschema", "output directory")
	flag.Parse()

	msgs := []proto.Message{
		&v1.Episode{}, &v1.Step{}, &v1.Blob{},
		&v1.Outcome{}, &v1.Label{}, &v1.Reward{},
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail(err)
	}

	for _, m := range msgs {
		d := m.ProtoReflect().Descriptor()
		schema := rootSchema(d)

		b, err := json.MarshalIndent(schema, "", "  ")
		if err != nil {
			fail(err)
		}
		b = append(b, '\n')

		path := filepath.Join(*out, string(d.Name())+".schema.json")
		if err := os.WriteFile(path, b, 0o644); err != nil {
			fail(err)
		}
		fmt.Println("wrote", path)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "gen-jsonschema:", err)
	os.Exit(1)
}

type schema = map[string]any

func rootSchema(d protoreflect.MessageDescriptor) schema {
	s := messageSchema(d)
	s["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	s["$id"] = fmt.Sprintf("https://trajectory.dev/schema/v%s/%s.schema.json", version.Schema, d.Name())
	s["title"] = string(d.Name())

	defs := schema{}
	collectDefs(d, defs)
	if len(defs) > 0 {
		s["$defs"] = defs
	}
	return s
}

// collectDefs walks nested message types so each is defined once and
// referenced, rather than inlined repeatedly.
func collectDefs(d protoreflect.MessageDescriptor, defs schema) {
	fds := d.Fields()
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		if fd.IsMap() || fd.Kind() != protoreflect.MessageKind {
			continue
		}
		name := string(fd.Message().Name())
		if _, seen := defs[name]; seen {
			continue
		}
		defs[name] = messageSchema(fd.Message())
		collectDefs(fd.Message(), defs)
	}
}

func messageSchema(d protoreflect.MessageDescriptor) schema {
	props := schema{}
	var required []string

	fds := d.Fields()
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		name := string(fd.Name())
		props[name] = fieldSchema(fd)

		// A field is required exactly when the spec §7 tables mark it
		// "Null: no" — a singular field with no presence semantics.
		if !fd.HasPresence() && !fd.IsList() && !fd.IsMap() {
			required = append(required, name)
		}
	}

	s := schema{"type": "object", "properties": props}
	if len(required) > 0 {
		s["required"] = required
	}
	// Unrecognised attributes belong in `raw`, not at the top level
	// (F-2.2). Rejecting them here makes a producer's mistake visible
	// instead of silently dropping data.
	s["additionalProperties"] = false
	return s
}

func fieldSchema(fd protoreflect.FieldDescriptor) schema {
	switch {
	case fd.IsMap():
		return schema{
			"type":                 "object",
			"additionalProperties": scalarSchema(fd.MapValue()),
		}
	case fd.IsList():
		return schema{"type": "array", "items": scalarSchema(fd)}
	default:
		return scalarSchema(fd)
	}
}

func scalarSchema(fd protoreflect.FieldDescriptor) schema {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return schema{"type": "boolean"}

	case protoreflect.StringKind, protoreflect.BytesKind:
		return schema{"type": "string"}

	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return schema{"type": "number"}

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return schema{"type": "integer"}

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind,
		protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		// protojson encodes 64-bit integers as strings to survive
		// JavaScript's 2^53 limit, but decoders accept either. Timestamps
		// are microseconds since the Unix epoch and exceed 2^53 in the
		// year 2255, so this is a correctness matter, not a formality.
		return schema{"type": []string{"string", "integer"}}

	case protoreflect.EnumKind:
		return schema{"type": "string", "enum": enumValues(fd.Enum())}

	case protoreflect.MessageKind, protoreflect.GroupKind:
		return schema{"$ref": "#/$defs/" + string(fd.Message().Name())}
	}
	return schema{}
}

// enumValues lists both vocabularies a decoder accepts: the protobuf JSON
// names, and the lowercase spec §7 values that are what actually land in
// Parquet. A producer may send either.
func enumValues(e protoreflect.EnumDescriptor) []string {
	vals := e.Values()
	out := make([]string, 0, vals.Len()*2)
	for i := 0; i < vals.Len(); i++ {
		out = append(out, string(vals.Get(i).Name()))
	}
	for i := 0; i < vals.Len(); i++ {
		if alias, ok := specAlias(string(vals.Get(i).Name())); ok {
			out = append(out, alias)
		}
	}
	return out
}

// specAlias strips the proto's TYPE_ prefix to recover the spec §7 value,
// e.g. STATUS_TIMED_OUT -> timed_out. UNSPECIFIED has no spec value.
func specAlias(name string) (string, bool) {
	for _, prefix := range []string{"STATUS_", "KIND_", "TRAINABLE_", "ENCODING_"} {
		if len(name) > len(prefix) && name[:len(prefix)] == prefix {
			rest := name[len(prefix):]
			if rest == "UNSPECIFIED" {
				return "", false
			}
			return lower(rest), true
		}
	}
	return "", false
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
