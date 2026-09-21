// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Outcome is the wire shape of a business outcome (§9.4).
//
// It is posted by the customer's own job — a nightly export from ticketing, an
// order-status webhook relay — never pulled by the collector: there are no CDC
// connectors (N-1, Q-6).
type Outcome struct {
	OutcomeID  string    `json:"outcome_id"`
	EntityName string    `json:"entity_name"`
	EntityKey  string    `json:"entity_key"`
	Kind       string    `json:"kind"`
	Value      string    `json:"value"`
	OccurredAt Timestamp `json:"occurred_at"`
	ObservedAt Timestamp `json:"observed_at"`
	Source     string    `json:"source"`
}

// Timestamp accepts what business systems actually export: RFC 3339 strings,
// or epoch seconds, milliseconds or microseconds as numbers or strings. The
// magnitude decides the unit, because an export rarely says. Stored as
// microseconds.
type Timestamp int64

func (t *Timestamp) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		return nil
	}
	v, err := ParseTimestamp(s)
	if err != nil {
		return err
	}
	*t = Timestamp(v)
	return nil
}

// ParseTimestamp converts a timestamp string to microseconds.
func ParseTimestamp(s string) (int64, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMicro(), nil
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("unrecognised timestamp %q; use RFC 3339 or epoch seconds, ms or µs", s)
	}
	switch {
	case f < 1e11:
		return int64(f * 1e6), nil
	case f < 1e14:
		return int64(f * 1e3), nil
	default:
		return int64(f), nil
	}
}

// DecodeOutcomes parses one outcome or an array.
func DecodeOutcomes(body []byte) ([]Outcome, error) {
	t := strings.TrimSpace(string(body))
	if t == "" {
		return nil, fmt.Errorf("empty request body")
	}
	if t[0] == '[' {
		var out []Outcome
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, fmt.Errorf("invalid outcome array: %w", err)
		}
		return out, nil
	}
	var one Outcome
	if err := json.Unmarshal(body, &one); err != nil {
		return nil, fmt.Errorf("invalid outcome: %w", err)
	}
	return []Outcome{one}, nil
}

// Validate reports the first missing required field. An outcome without an
// entity name, key, kind or time cannot be joined to anything, so accepting it
// would only create rows that silently never match.
func (o Outcome) Validate() error {
	switch {
	case o.EntityName == "":
		return fmt.Errorf("entity_name is required (e.g. ticket_id); it must match an `entities` key name")
	case o.EntityKey == "":
		return fmt.Errorf("entity_key is required")
	case o.Kind == "":
		return fmt.Errorf("kind is required (e.g. refund_status)")
	case o.OccurredAt == 0:
		return fmt.Errorf("occurred_at is required; the as-of join orders outcomes by it")
	}
	return nil
}
