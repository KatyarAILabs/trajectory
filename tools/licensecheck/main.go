// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Command licensecheck fails if any module in the build graph carries a
// licence that is not on the permissive allow-list (§13, F-12.4).
//
// This is deliberately a small, self-owned tool rather than a dependency.
// go-licenses broke against the Go 1.26 standard library, and a supply-chain
// gate that fails spuriously is worse than no gate, because the first thing
// anyone does with a noisy gate is switch it off.
//
// It fails closed: a licence it cannot positively identify is a failure, not a
// pass. A false alarm costs someone five minutes; a missed copyleft dependency
// costs a legal review.
//
// Run: go run ./tools/licensecheck
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// allowed lists the licences the project accepts. Apache 2.0 is the project's
// own licence; the rest are compatible permissive ones.
var allowed = map[string]bool{
	"Apache-2.0":   true,
	"MIT":          true,
	"BSD-2-Clause": true,
	"BSD-3-Clause": true,
	"ISC":          true,
	"MPL-2.0":      true,
	"Unlicense":    true,
	"Zlib":         true,
}

type module struct {
	Path string
	Dir  string
	Main bool
}

func main() {
	mods, err := modules()
	if err != nil {
		fmt.Fprintln(os.Stderr, "licensecheck:", err)
		os.Exit(2)
	}

	var problems []string
	results := make([]string, 0, len(mods))

	for _, m := range mods {
		if m.Main || m.Dir == "" {
			continue
		}
		lic, file := classify(m.Dir)

		switch {
		case lic == "":
			problems = append(problems, fmt.Sprintf(
				"  %s\n      no LICENSE file found in %s\n"+
					"      A module whose licence cannot be read is treated as disallowed.", m.Path, m.Dir))
		case !allowed[lic]:
			problems = append(problems, fmt.Sprintf(
				"  %s\n      licence %s (%s) is not on the allow-list", m.Path, lic, file))
		default:
			results = append(results, fmt.Sprintf("  %-55s %s", m.Path, lic))
		}
	}

	sort.Strings(results)
	for _, r := range results {
		fmt.Println(r)
	}
	fmt.Printf("\n%d modules checked, all permissive\n", len(results))

	if len(problems) > 0 {
		fmt.Fprintf(os.Stderr, "\nlicensecheck: %d module(s) failed:\n\n%s\n\n",
			len(problems), strings.Join(problems, "\n\n"))
		fmt.Fprintln(os.Stderr, "Allowed:", strings.Join(sortedKeys(allowed), ", "))
		os.Exit(1)
	}
}

// modules lists every module in the build graph. Standard-library packages
// have no module and are simply absent, which is why this reads the module
// list rather than the package list.
func modules() ([]module, error) {
	cmd := exec.Command("go", "list", "-m", "-json", "all")
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("go list -m: %s", ee.Stderr)
		}
		return nil, err
	}

	var mods []module
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for dec.More() {
		var m module
		if err := dec.Decode(&m); err != nil {
			return nil, err
		}
		mods = append(mods, m)
	}
	return mods, nil
}

var licenseFiles = []string{
	"LICENSE", "LICENSE.txt", "LICENSE.md",
	"COPYING", "COPYING.txt",
	"LICENCE", "LICENCE.txt",
}

// classify identifies a licence from distinctive phrases in its text.
//
// Phrase matching rather than full-text comparison, because projects reformat
// and re-wrap licence text freely. Each phrase below is chosen to be present in
// every variant of its licence and absent from the others.
func classify(dir string) (licence, file string) {
	for _, name := range licenseFiles {
		path := filepath.Join(dir, name)
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		return identify(string(b)), name
	}
	return "", ""
}

func identify(text string) string {
	t := strings.Join(strings.Fields(text), " ")
	has := func(s string) bool { return strings.Contains(t, s) }

	// Copyleft first: these must be recognised even when a file also
	// contains permissive-looking boilerplate, so they cannot slip through
	// on a later match.
	switch {
	case has("GNU AFFERO GENERAL PUBLIC LICENSE"):
		return "AGPL"
	case has("GNU LESSER GENERAL PUBLIC LICENSE"):
		return "LGPL"
	case has("GNU GENERAL PUBLIC LICENSE"):
		return "GPL"
	}

	switch {
	case has("Apache License") && has("Version 2.0"):
		return "Apache-2.0"
	case has("Mozilla Public License"):
		return "MPL-2.0"
	case has("Permission is hereby granted, free of charge"):
		return "MIT"
	case has("Permission to use, copy, modify, and/or distribute"):
		return "ISC"
	case has("This is free and unencumbered software released into the public domain"):
		return "Unlicense"
	case has("altered source versions must be plainly marked"):
		return "Zlib"
	case has("Redistribution and use in source and binary forms"):
		// The third clause is the no-endorsement clause; without it
		// this is the 2-clause variant.
		if has("Neither the name of") || has("nor the names of") {
			return "BSD-3-Clause"
		}
		return "BSD-2-Clause"
	}
	return ""
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
