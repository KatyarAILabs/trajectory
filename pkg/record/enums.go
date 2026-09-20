// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import v1 "github.com/trajectory-project/trajectory/gen/go/trajectory/v1"

// Enums are stored in Parquet as their spec §7 string values, not as the
// proto's prefixed names, so a file is readable in DuckDB without a mapping
// table. The conversions below are the only place the two vocabularies meet.

// Status values (§7.1).
const (
	StatusComplete = "complete"
	StatusTimedOut = "timed_out"
	StatusEvicted  = "evicted"
	StatusPatched  = "patched"
)

// Kind values (§7.2).
const (
	KindLLM       = "llm"
	KindTool      = "tool"
	KindRetrieval = "retrieval"
	KindHuman     = "human"
	KindOther     = "other"
)

// Trainable values (§7.2). Three-valued: a source that cannot say must not be
// coerced into a boolean (F-4.4).
const (
	TrainableTrue    = "true"
	TrainableFalse   = "false"
	TrainableUnknown = "unknown"
)

// Encoding values (§7.3).
const (
	EncodingUTF8   = "utf8"
	EncodingBase64 = "base64"
	EncodingZstd   = "zstd"
)

var statusToProto = map[string]v1.Status{
	StatusComplete: v1.Status_STATUS_COMPLETE,
	StatusTimedOut: v1.Status_STATUS_TIMED_OUT,
	StatusEvicted:  v1.Status_STATUS_EVICTED,
	StatusPatched:  v1.Status_STATUS_PATCHED,
}

var kindToProto = map[string]v1.Kind{
	KindLLM:       v1.Kind_KIND_LLM,
	KindTool:      v1.Kind_KIND_TOOL,
	KindRetrieval: v1.Kind_KIND_RETRIEVAL,
	KindHuman:     v1.Kind_KIND_HUMAN,
	KindOther:     v1.Kind_KIND_OTHER,
}

var trainableToProto = map[string]v1.Trainable{
	TrainableTrue:    v1.Trainable_TRAINABLE_TRUE,
	TrainableFalse:   v1.Trainable_TRAINABLE_FALSE,
	TrainableUnknown: v1.Trainable_TRAINABLE_UNKNOWN,
}

var encodingToProto = map[string]v1.Encoding{
	EncodingUTF8:   v1.Encoding_ENCODING_UTF8,
	EncodingBase64: v1.Encoding_ENCODING_BASE64,
	EncodingZstd:   v1.Encoding_ENCODING_ZSTD,
}

func invert[T comparable](m map[string]T) map[T]string {
	out := make(map[T]string, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

var (
	statusFromProto    = invert(statusToProto)
	kindFromProto      = invert(kindToProto)
	trainableFromProto = invert(trainableToProto)
	encodingFromProto  = invert(encodingToProto)
)

// StatusFromProto maps a wire Status onto its stored string. An unrecognised
// value maps to the empty string rather than a guess.
func StatusFromProto(s v1.Status) string { return statusFromProto[s] }

// KindFromProto maps a wire Kind onto its stored string. Unknown span kinds are
// normalised to "other" rather than dropped (F-2.5).
func KindFromProto(k v1.Kind) string {
	if s, ok := kindFromProto[k]; ok {
		return s
	}
	return KindOther
}

// TrainableFromProto maps a wire Trainable onto its stored string. An
// unspecified value becomes "unknown", never "false" (F-4.4).
func TrainableFromProto(t v1.Trainable) string {
	if s, ok := trainableFromProto[t]; ok {
		return s
	}
	return TrainableUnknown
}

// EncodingFromProto maps a wire Encoding onto its stored string.
func EncodingFromProto(e v1.Encoding) string { return encodingFromProto[e] }
