// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// outcomeColumns are the CSV header names. Only entity_name, entity_key, kind
// and occurred_at are required; the rest may be absent from the header.
var outcomeColumns = []string{
	"outcome_id", "entity_name", "entity_key", "kind", "value",
	"occurred_at", "observed_at", "source",
}

// ReadOutcomesCSV parses a CSV export with a header row.
//
// A CSV is how outcomes arrive in practice at first — a query result exported
// from a ticketing or orders system — so it is accepted directly rather than
// asking a design partner to write a converter.
func ReadOutcomesCSV(r io.Reader) ([]Outcome, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, req := range []string{"entity_name", "entity_key", "kind", "occurred_at"} {
		if _, ok := idx[req]; !ok {
			return nil, fmt.Errorf("CSV header lacks %q; expected columns: %s",
				req, strings.Join(outcomeColumns, ", "))
		}
	}

	get := func(row []string, col string) string {
		if i, ok := idx[col]; ok && i < len(row) {
			return strings.TrimSpace(row[i])
		}
		return ""
	}

	var out []Outcome
	line := 1
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		line++
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}

		o := Outcome{
			OutcomeID:  get(row, "outcome_id"),
			EntityName: get(row, "entity_name"),
			EntityKey:  get(row, "entity_key"),
			Kind:       get(row, "kind"),
			Value:      get(row, "value"),
			Source:     get(row, "source"),
		}
		for col, dst := range map[string]*Timestamp{"occurred_at": &o.OccurredAt, "observed_at": &o.ObservedAt} {
			if v := get(row, col); v != "" {
				ts, err := ParseTimestamp(v)
				if err != nil {
					return nil, fmt.Errorf("line %d: %s: %w", line, col, err)
				}
				*dst = Timestamp(ts)
			}
		}
		if err := o.Validate(); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		out = append(out, o)
	}
	return out, nil
}
