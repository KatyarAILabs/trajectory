// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/KatyarAILabs/trajectory/spec/conformance"
)

// cmdConform implements the conformance suite as a command (F-10.3, §18).
//
// §18 makes the suite the definition of compliance and says anyone may claim
// it. That is only true if it is runnable against someone else's output, so it
// takes a directory and checks what is in it — no interface to implement, no
// build-time coupling to this codebase.
func cmdConform(args []string) int {
	fs := flag.NewFlagSet("conform", flag.ExitOnError)
	verbose := fs.Bool("v", false, "list every check, including passes")
	_ = fs.Parse(args)

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintln(os.Stderr, "cc conform: a lake directory is required")
		fmt.Fprintln(os.Stderr, "\n  cc conform ./var/lake")
		return 2
	}
	root := rest[0]

	if _, err := os.Stat(root); err != nil {
		fmt.Fprintf(os.Stderr, "cc conform: %v\n", err)
		return 1
	}

	d, err := conformance.Load(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cc conform: cannot read %s: %v\n", root, err)
		return 1
	}

	report := conformance.Check(d)

	if *verbose {
		fmt.Print(report)
	} else {
		fmt.Printf("conformance report for %s\n\n", report.Root)
		shown := 0
		for _, res := range report.Results {
			if res.Passed && !res.Skipped {
				continue
			}
			mark := "FAIL"
			if res.Skipped {
				mark = "SKIP"
			}
			fmt.Printf("  [%s] %-38s %s\n", mark, res.Check, res.Ref)
			if res.Detail != "" {
				fmt.Printf("         %s\n", res.Detail)
			}
			shown++
		}
		p, f, s := report.Counts()
		if shown == 0 {
			fmt.Printf("  all %d checks passed\n", p)
		}
		fmt.Printf("\n  %d passed, %d failed, %d skipped\n", p, f, s)
	}

	// logcheck:allow — prints row counts, never row contents.
	fmt.Printf("\n  %d episodes, %d steps, %d blobs, %d manifests\n",
		len(d.Episodes), len(d.Steps), len(d.Blobs), len(d.Manifests))

	if !report.Passed() {
		return 1
	}
	return 0
}
