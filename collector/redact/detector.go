// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

// Detector finds entities that no regex catches — names, addresses — by asking
// an external service (F-5.8).
//
// It is an interface to a service rather than a library for two reasons. The
// spec forbids compiling it in, because the good detectors (Presidio and its
// relatives) are Python and model-backed, and pulling that into a static Go
// binary would break the deployment story (Q-4). And a detector is the kind of
// component a security team wants to choose, run and update themselves.
//
// The service receives payload text, so it must run inside the same perimeter
// as the collector — as a sidecar, typically. F-12.6 still holds: the endpoint
// is configured, and nothing else is called.
type Detector interface {
	Detect(ctx context.Context, text string) ([]Finding, error)
}

// Finding is one detected entity, as a byte range into the text.
type Finding struct {
	EntityType string  `json:"entity_type"`
	Start      int     `json:"start"`
	End        int     `json:"end"`
	Score      float64 `json:"score"`
}

// PresidioDetector calls a Presidio analyzer's /analyze endpoint.
type PresidioDetector struct {
	Endpoint string
	Language string
	Entities []string
	MinScore float64
	Client   *http.Client
}

// Detect implements Detector.
func (p *PresidioDetector) Detect(ctx context.Context, text string) ([]Finding, error) {
	body, _ := json.Marshal(map[string]any{
		"text":            text,
		"language":        orDefault(p.Language, "en"),
		"entities":        p.Entities,
		"score_threshold": p.MinScore,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		// The error names the endpoint, never the text sent.
		return nil, fmt.Errorf("entity detector unreachable at %s", p.Endpoint)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("entity detector returned status %d", resp.StatusCode)
	}

	var out []Finding
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return nil, fmt.Errorf("entity detector returned an unreadable response")
	}

	kept := out[:0]
	for _, f := range out {
		if f.Score >= p.MinScore && f.Start >= 0 && f.End <= len(text) && f.Start < f.End {
			kept = append(kept, f)
		}
	}
	return kept, nil
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// applyDetector redacts every finding in a value. A detector failure is an
// error, and therefore a quarantine (F-5.5): if the service that finds names
// is down, the safe assumption is that the names are still there.
func (r *Redactor) applyDetector(path, value string) (string, []ManifestEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.detectTimeout)
	defer cancel()

	found, err := r.detector.Detect(ctx, value)
	if err != nil {
		r.countError("detector")
		return "", nil, fmt.Errorf("redaction detector: %w", err)
	}
	if len(found) == 0 {
		return value, nil, nil
	}

	// Merge overlaps, then replace from the end so earlier offsets stay
	// valid while later ranges are rewritten.
	sort.Slice(found, func(i, j int) bool { return found[i].Start < found[j].Start })
	merged := []Finding{found[0]}
	for _, f := range found[1:] {
		last := &merged[len(merged)-1]
		if f.Start < last.End {
			if f.End > last.End {
				last.End = f.End
			}
			continue
		}
		merged = append(merged, f)
	}

	counts := map[string]int{}
	out := value
	for i := len(merged) - 1; i >= 0; i-- {
		f := merged[i]
		var repl string
		if r.detectAction == ActionTokenize {
			if len(r.key) == 0 {
				r.countError("detector")
				return "", nil, fmt.Errorf("redaction detector: action tokenize requires a key")
			}
			repl = r.tokenize(out[f.Start:f.End])
		} else {
			repl = "[redacted:" + f.EntityType + "]"
		}
		out = out[:f.Start] + repl + out[f.End:]
		counts[f.EntityType]++
	}

	var entries []ManifestEntry
	for _, t := range sortedCountKeys(counts) {
		r.countMatch("detector:"+t, counts[t])
		entries = append(entries, ManifestEntry{
			RuleID: "detector:" + t, Path: path, Action: r.detectAction, Matches: counts[t],
		})
	}
	return out, entries, nil
}

func sortedCountKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
