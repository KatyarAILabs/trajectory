// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/trajectory-project/trajectory/pkg/record"
)

// cmdInspect implements F-14.4: summarise a Parquet file or blob without a
// query engine.
//
// The point is that an operator debugging at 3am should not need DuckDB
// installed, or a notebook, or credentials to a warehouse, to answer "did
// anything land, and does it look right".
func cmdInspect(args []string) int {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	schemaOnly := fs.Bool("schema", false, "print the file schema and exit")
	limit := fs.Int("n", 3, "how many sample rows to print")
	_ = fs.Parse(args)

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintln(os.Stderr, "cc inspect: a file is required")
		return 2
	}
	path := rest[0]

	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %v\n", err)
		return 1
	}

	// A blob is a bare content-addressed file, not Parquet. Recognising it
	// by location rather than by trying to parse it avoids a confusing
	// "not a parquet file" error for something that was never meant to be.
	if strings.Contains(filepath.ToSlash(path), "/blobs/sha256/") {
		return inspectBlob(path, info)
	}
	if strings.HasSuffix(path, ".json") {
		return inspectManifest(path)
	}
	return inspectParquet(path, info, *schemaOnly, *limit)
}

func inspectBlob(path string, info os.FileInfo) int {
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %v\n", err)
		return 1
	}

	fmt.Printf("blob %s\n", filepath.Base(path))
	fmt.Printf("  bytes      %d\n", info.Size())

	// The filename is the sha256 of the content, so it can be verified
	// here rather than taken on trust.
	sum := sha256Hex(b)
	name := filepath.Base(path)
	if sum == name {
		fmt.Printf("  sha256     verified\n")
	} else {
		fmt.Printf("  sha256     MISMATCH: content hashes to %s\n", sum)
		fmt.Printf("             this blob is corrupt or was not written by the collector\n")
	}

	fmt.Printf("\n%s\n", truncate(string(b), 400))
	return 0
}

func inspectManifest(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %v\n", err)
		return 1
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %s is not valid JSON: %v\n", path, err)
		return 1
	}

	out, _ := json.MarshalIndent(m, "", "  ")
	fmt.Println(string(out))
	return 0
}

func inspectParquet(path string, info os.FileInfo, schemaOnly bool, limit int) int {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %v\n", err)
		return 1
	}
	defer f.Close()

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %s is not a readable Parquet file: %v\n", path, err)
		return 1
	}

	fmt.Printf("%s\n", path)
	fmt.Printf("  bytes        %d\n", info.Size())
	fmt.Printf("  rows         %d\n", pf.NumRows())
	fmt.Printf("  row groups   %d\n", len(pf.RowGroups()))

	if v, ok := pf.Lookup("schema_version"); ok {
		fmt.Printf("  schema       %s\n", v)
		if v != schemaVersion() {
			fmt.Printf("               note: this build writes %s\n", schemaVersion())
		}
	} else {
		fmt.Printf("  schema       not recorded in file metadata\n")
	}

	table := tableOf(path)
	fmt.Printf("  table        %s\n", table)

	if schemaOnly {
		fmt.Printf("\n%s\n", pf.Schema().String())
		return 0
	}

	switch table {
	case record.TableEpisodes:
		return summariseEpisodes(path, limit)
	case record.TableSteps:
		return summariseSteps(path, limit)
	case record.TableBlobs:
		return summariseBlobs(path, limit)
	default:
		fmt.Printf("\n%s\n", pf.Schema().String())
		return 0
	}
}

func summariseEpisodes(path string, limit int) int {
	rows, err := parquet.ReadFile[record.Episode](path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %v\n", err)
		return 1
	}

	byStatus := map[string]int{}
	withKeys, totalSteps := 0, 0
	for _, e := range rows {
		byStatus[e.Status]++
		totalSteps += int(e.StepCount)
		if len(e.EntityKeys) > 0 {
			withKeys++
		}
	}

	fmt.Printf("\nepisodes by status\n")
	for _, s := range sortedCountKeys(byStatus) {
		fmt.Printf("  %-12s %d\n", s, byStatus[s])
	}
	fmt.Printf("\n  steps total       %d\n", totalSteps)
	if len(rows) > 0 {
		fmt.Printf("  entity coverage   %d/%d episodes carry a key (%.0f%%)\n",
			withKeys, len(rows), float64(withKeys)/float64(len(rows))*100)
	}

	fmt.Printf("\nsample\n")
	for i, e := range rows {
		if i >= limit {
			break
		}
		fmt.Printf("  %s  %-9s steps=%-3d %s\n",
			e.EpisodeID, e.Status, e.StepCount, tsOf(e.StartedAt))
		if len(e.EntityKeys) > 0 {
			var ks []string
			for _, k := range e.EntityKeys {
				ks = append(ks, k.Name+"="+k.Value)
			}
			fmt.Printf("      keys: %s\n", strings.Join(ks, " "))
		}
	}
	return 0
}

func summariseSteps(path string, limit int) int {
	rows, err := parquet.ReadFile[record.Step](path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %v\n", err)
		return 1
	}

	byKind := map[string]int{}
	inline, blobbed, truncated := 0, 0, 0
	for _, s := range rows {
		byKind[s.Kind]++
		if s.ContentRef != nil {
			blobbed++
		} else if s.ContentInline != nil {
			inline++
		}
		if s.Truncated {
			truncated++
		}
	}

	fmt.Printf("\nsteps by kind\n")
	for _, k := range sortedCountKeys(byKind) {
		fmt.Printf("  %-12s %d\n", k, byKind[k])
	}
	fmt.Printf("\n  inline payloads   %d\n", inline)
	fmt.Printf("  externalised      %d\n", blobbed)
	if truncated > 0 {
		fmt.Printf("  truncated         %d  (payload exceeded max_payload_bytes)\n", truncated)
	}

	fmt.Printf("\nsample\n")
	for i, s := range rows {
		if i >= limit {
			break
		}
		where := "inline"
		if s.ContentRef != nil {
			where = "blob:" + (*s.ContentRef)[:8]
		}
		fmt.Printf("  [%d] %-9s parent=%-4s %s\n", s.StepIdx, s.Kind, idxStr(s.ParentIdx), where)
	}
	return 0
}

func summariseBlobs(path string, limit int) int {
	rows, err := parquet.ReadFile[record.Blob](path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc inspect: %v\n", err)
		return 1
	}

	var total int64
	for _, b := range rows {
		total += b.Bytes
	}
	fmt.Printf("\n  blobs        %d\n", len(rows))
	fmt.Printf("  total bytes  %d\n", total)

	fmt.Printf("\nsample\n")
	for i, b := range rows {
		if i >= limit {
			break
		}
		fmt.Printf("  %s  %8d bytes  %s\n", b.Sha256[:16], b.Bytes, b.ContentType)
	}
	return 0
}

func tableOf(path string) string {
	p := filepath.ToSlash(path)
	for _, t := range []string{record.TableEpisodes, record.TableSteps, record.TableBlobs} {
		if strings.Contains(p, "/"+t+"/") || strings.HasPrefix(p, t+"/") {
			return t
		}
	}
	return "unknown"
}

func idxStr(p *int32) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *p)
}

func tsOf(us int64) string {
	if us == 0 {
		return "-"
	}
	return time.UnixMicro(us).UTC().Format(time.RFC3339)
}

func sortedStrings(in []string) []string {
	sort.Strings(in)
	return in
}
