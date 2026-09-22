// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package conformance is the definition of compliance with the trajectory
// format (F-10.3, §18).
//
// §18 says the suite is the definition of compliance and anyone may claim it.
// That only means something if the suite is runnable against a third party's
// output rather than only against this implementation, so everything here takes
// a directory of Parquet files and checks it — no interfaces to implement, no
// build-time coupling.
//
// The checks are the promises a reader is entitled to rely on. Each one names
// the requirement it enforces, because a failure should tell an implementer
// what they broke, not just that something is wrong.
package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Result is the outcome of one check.
type Result struct {
	Check  string
	Ref    string // the requirement it enforces
	Passed bool
	Detail string
	// Skipped marks a check that could not run, which is distinct from one
	// that ran and passed. Reporting a skip as a pass is how a suite
	// quietly stops testing anything.
	Skipped bool
}

// Report is the full outcome.
type Report struct {
	Root    string
	Results []Result
}

// Passed reports whether every check that ran passed.
func (r Report) Passed() bool {
	for _, res := range r.Results {
		if !res.Passed && !res.Skipped {
			return false
		}
	}
	return true
}

// Counts summarises the report.
func (r Report) Counts() (passed, failed, skipped int) {
	for _, res := range r.Results {
		switch {
		case res.Skipped:
			skipped++
		case res.Passed:
			passed++
		default:
			failed++
		}
	}
	return
}

// String renders a human-readable report.
func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "conformance report for %s\n\n", r.Root)

	for _, res := range r.Results {
		mark := "PASS"
		switch {
		case res.Skipped:
			mark = "SKIP"
		case !res.Passed:
			mark = "FAIL"
		}
		fmt.Fprintf(&b, "  [%s] %-38s %s\n", mark, res.Check, res.Ref)
		if res.Detail != "" {
			fmt.Fprintf(&b, "         %s\n", res.Detail)
		}
	}

	p, f, s := r.Counts()
	fmt.Fprintf(&b, "\n  %d passed, %d failed, %d skipped\n", p, f, s)
	return b.String()
}

// Dataset is a lake loaded for checking.
type Dataset struct {
	Root      string
	Episodes  []record.Episode
	Steps     []record.Step
	Blobs     []record.Blob
	Outcomes  []record.Outcome
	Rewards   []record.Reward
	Manifests []manifest
	// SchemaVersions are the versions found in file metadata.
	SchemaVersions map[string]int
	// Unreadable lists files that could not be parsed. They are recorded
	// rather than aborting the run: a tool pointed at a broken dataset
	// should report what is broken, not refuse to look.
	Unreadable []string
}

type manifest struct {
	BatchID       string `json:"batch_id"`
	SchemaVersion string `json:"schema_version"`
	Files         []struct {
		Path  string `json:"path"`
		Rows  int64  `json:"rows"`
		Table string `json:"table"`
	} `json:"files"`
	Counts struct {
		Episodes int64 `json:"episodes"`
		Steps    int64 `json:"steps"`
	} `json:"counts"`
}

// Load reads a lake directory.
func Load(root string) (*Dataset, error) {
	d := &Dataset{Root: root, SchemaVersions: map[string]int{}}

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, root))

		switch {
		case strings.HasSuffix(path, ".parquet"):
			return d.loadParquet(path, rel)
		case strings.Contains(rel, "/manifests/") && strings.HasSuffix(path, ".json"):
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var m manifest
			if err := json.Unmarshal(b, &m); err != nil {
				d.Unreadable = append(d.Unreadable,
					fmt.Sprintf("%s: manifest is not valid JSON: %v", rel, err))
				return nil
			}
			d.Manifests = append(d.Manifests, m)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Dataset) loadParquet(path, rel string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		d.Unreadable = append(d.Unreadable, fmt.Sprintf("%s: %v", rel, err))
		return nil
	}
	if v, ok := pf.Lookup("schema_version"); ok {
		d.SchemaVersions[v]++
	} else {
		d.SchemaVersions[""]++
	}

	switch {
	case strings.Contains(rel, "/"+record.TableEpisodes+"/"):
		rows, err := parquet.ReadFile[record.Episode](path)
		if err != nil {
			d.Unreadable = append(d.Unreadable, fmt.Sprintf("%s: %v", rel, err))
			return nil
		}
		d.Episodes = append(d.Episodes, rows...)
	case strings.Contains(rel, "/"+record.TableSteps+"/"):
		rows, err := parquet.ReadFile[record.Step](path)
		if err != nil {
			d.Unreadable = append(d.Unreadable, fmt.Sprintf("%s: %v", rel, err))
			return nil
		}
		d.Steps = append(d.Steps, rows...)
	case strings.Contains(rel, "/"+record.TableOutcomes+"/"):
		rows, err := parquet.ReadFile[record.Outcome](path)
		if err != nil {
			d.Unreadable = append(d.Unreadable, fmt.Sprintf("%s: %v", rel, err))
			return nil
		}
		d.Outcomes = append(d.Outcomes, rows...)
	case strings.Contains(rel, "/"+record.TableRewards+"/"):
		rows, err := parquet.ReadFile[record.Reward](path)
		if err != nil {
			d.Unreadable = append(d.Unreadable, fmt.Sprintf("%s: %v", rel, err))
			return nil
		}
		d.Rewards = append(d.Rewards, rows...)
	case strings.Contains(rel, "/"+record.TableBlobs+"/"):
		rows, err := parquet.ReadFile[record.Blob](path)
		if err != nil {
			d.Unreadable = append(d.Unreadable, fmt.Sprintf("%s: %v", rel, err))
			return nil
		}
		d.Blobs = append(d.Blobs, rows...)
	}
	return nil
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
