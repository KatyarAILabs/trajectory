// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package wire decodes the native JSON and protobuf episode formats (§9.1,
// §9.2).
//
// The published JSON Schema advertises two spellings for every enum: the
// protobuf JSON names a standard protobuf encoder emits (STATUS_COMPLETE), and
// the lowercase spec §7 values that land in Parquet (complete). Honouring both
// is not a convenience — the schema is a published contract (F-10.3), and a
// decoder that accepted only one would make it a promise the collector does not
// keep.
package wire

import (
	"fmt"
	"strings"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// enumSet is one canonical vocabulary plus the prefix its protobuf names use.
type enumSet struct {
	name   string
	prefix string
	values map[string]string
}

var (
	statusSet = enumSet{
		name:   "status",
		prefix: "STATUS_",
		values: set(record.StatusComplete, record.StatusTimedOut,
			record.StatusEvicted, record.StatusPatched),
	}
	kindSet = enumSet{
		name:   "kind",
		prefix: "KIND_",
		values: set(record.KindLLM, record.KindTool, record.KindRetrieval,
			record.KindHuman, record.KindOther),
	}
	trainableSet = enumSet{
		name:   "trainable",
		prefix: "TRAINABLE_",
		values: set(record.TrainableTrue, record.TrainableFalse, record.TrainableUnknown),
	}
	encodingSet = enumSet{
		name:   "encoding",
		prefix: "ENCODING_",
		values: set(record.EncodingUTF8, record.EncodingBase64, record.EncodingZstd),
	}
)

func set(vals ...string) map[string]string {
	m := make(map[string]string, len(vals))
	for _, v := range vals {
		m[v] = v
	}
	return m
}

// normalise accepts either spelling and returns the canonical one.
func (e enumSet) normalise(v string) (string, error) {
	t := strings.TrimSpace(v)
	if t == "" {
		return "", nil
	}

	// The spec §7 spelling, which is what Parquet stores.
	if canon, ok := e.values[strings.ToLower(t)]; ok {
		return canon, nil
	}

	// The protobuf JSON spelling: STATUS_TIMED_OUT -> timed_out.
	up := strings.ToUpper(t)
	if strings.HasPrefix(up, e.prefix) {
		rest := strings.ToLower(strings.TrimPrefix(up, e.prefix))
		if rest == "unspecified" {
			return "", nil
		}
		if canon, ok := e.values[rest]; ok {
			return canon, nil
		}
	}

	return "", fmt.Errorf("unknown %s %q; accepted values are %s (or their %s… equivalents)",
		e.name, v, strings.Join(sorted(e.values), ", "), e.prefix)
}

func sorted(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Small fixed sets; insertion-order instability would make error
	// messages non-deterministic, which is irritating in tests and docs.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// NormaliseStatus accepts either spelling of an episode status.
func NormaliseStatus(v string) (string, error) { return statusSet.normalise(v) }

// NormaliseKind accepts either spelling of a step kind.
//
// An unrecognised value is an error here rather than being coerced to "other".
// F-2.5's retention rule is about span kinds a *tracing convention* did not
// define; a native producer sending an invalid enum against a published schema
// is a bug in that producer, and telling it so is more useful than silently
// reclassifying its data.
func NormaliseKind(v string) (string, error) { return kindSet.normalise(v) }

// NormaliseTrainable accepts either spelling. An empty or unspecified value
// becomes "unknown", never "false" (F-4.4).
func NormaliseTrainable(v string) (string, error) {
	out, err := trainableSet.normalise(v)
	if err != nil {
		return "", err
	}
	if out == "" {
		return record.TrainableUnknown, nil
	}
	return out, nil
}

// NormaliseEncoding accepts either spelling of a blob encoding.
func NormaliseEncoding(v string) (string, error) { return encodingSet.normalise(v) }
