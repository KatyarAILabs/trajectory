// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Command logcheck fails if any log call site could write payload content
// (F-12.2).
//
// The spec makes this a CI lint rather than a review item on purpose (§14,
// "enforced by a lint rule in CI on log call sites"), and the reasoning holds
// up: a payload reaching a debug log is invisible until someone turns on debug
// logging in production, at which point prompts and tool arguments are in a log
// aggregator that a much larger set of people can read. Review does not catch
// this reliably, because the offending line always looks reasonable — it is
// usually someone debugging a real problem.
//
// The check is deliberately syntactic and conservative. It flags any log call
// whose arguments mention a field known to carry payload content, and it is
// silenced only by an explicit, reviewable comment.
//
// Run: go run ./tools/logcheck ./...
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// payloadIdents name things that hold, or plausibly hold, captured content.
// A name matching any of these inside a log call is a finding.
var payloadIdents = []string{
	"ContentInline", "ContentRef", "Payload", "payload",
	"Content", "content",
	"Prompt", "prompt", "Completion", "completion",
	"Args", "args", "Arguments", "arguments",
	"Result", "result", "Input", "input", "Output", "output",
	"Raw", "raw",
	"EntityKeys", "entity_keys",
	"Steps", "steps",
	"Value", "value",
}

// logFuncs are the call names treated as logging.
var logFuncs = map[string]bool{
	"Debug": true, "Info": true, "Warn": true, "Error": true, "Fatal": true,
	"Debugf": true, "Infof": true, "Warnf": true, "Errorf": true, "Fatalf": true,
	"Print": true, "Printf": true, "Println": true,
	"Log": true, "Logf": true,
	"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
}

// allowComment silences a finding. It must be on or immediately above the line,
// so a reviewer sees the justification next to the risk.
const allowComment = "logcheck:allow"

type finding struct {
	pos   token.Position
	call  string
	ident string
}

func main() {
	roots := os.Args[1:]
	if len(roots) == 0 {
		roots = []string{"."}
	}

	var findings []finding
	fset := token.NewFileSet()

	for _, root := range roots {
		root = strings.TrimSuffix(root, "/...")
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				// Generated code and vendored trees are not ours
				// to police, and tests do not run in production.
				base := filepath.Base(path)
				if base == "gen" || base == "vendor" || base == ".git" || base == "bin" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				return nil // a file that does not parse is the compiler's problem
			}
			findings = append(findings, check(fset, f)...)
			return nil
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "logcheck:", err)
			os.Exit(2)
		}
	}

	if len(findings) == 0 {
		fmt.Println("logcheck: no log call site can reach payload content")
		return
	}

	sort.Slice(findings, func(i, j int) bool {
		return findings[i].pos.String() < findings[j].pos.String()
	})

	fmt.Fprintf(os.Stderr, "logcheck: %d log call site(s) may write payload content (F-12.2):\n\n", len(findings))
	for _, f := range findings {
		fmt.Fprintf(os.Stderr, "  %s\n      %s(...) references %q\n", f.pos, f.call, f.ident)
	}
	fmt.Fprintf(os.Stderr, "\nPayload content must never reach the collector's own logs at any level,\n"+
		"including debug. Log an identifier instead — episode_id, step_idx, a rule id.\n"+
		"If this call genuinely cannot leak (for example it logs only a length),\n"+
		"add a %q comment on the line with a one-line reason.\n", allowComment)
	os.Exit(1)
}

func check(fset *token.FileSet, file *ast.File) []finding {
	allowed := allowedLines(fset, file)
	var out []finding

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		name := calleeName(call)
		if !logFuncs[name] {
			return true
		}

		pos := fset.Position(call.Pos())
		if allowed[pos.Line] {
			return true
		}

		for _, arg := range call.Args {
			if id := suspiciousIdent(arg); id != "" {
				out = append(out, finding{pos: pos, call: name, ident: id})
				break
			}
		}
		return true
	})
	return out
}

func calleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fn.Sel.Name
	case *ast.Ident:
		return fn.Name
	}
	return ""
}

// suspiciousIdent walks an argument expression for a name that suggests
// payload content.
func suspiciousIdent(e ast.Expr) string {
	var found string

	ast.Inspect(e, func(n ast.Node) bool {
		if found != "" {
			return false
		}
		switch t := n.(type) {
		case *ast.Ident:
			if matches(t.Name) {
				found = t.Name
				return false
			}
		case *ast.SelectorExpr:
			if matches(t.Sel.Name) {
				found = t.Sel.Name
				return false
			}
		case *ast.BasicLit:
			// A string literal naming a payload field is how a
			// structured logger labels a value: log.Info("x",
			// "content", body).
			if t.Kind == token.STRING {
				v := strings.Trim(t.Value, `"`)
				if matches(v) {
					found = v
					return false
				}
			}
		}
		return true
	})
	return found
}

func matches(name string) bool {
	for _, p := range payloadIdents {
		if name == p {
			return true
		}
	}
	return false
}

// allowedLines collects lines carrying the silencing comment.
//
// A justification worth reading is usually more than one line, so the whole
// comment group counts: the marker may appear anywhere in it, and it covers
// every line of the group plus the line immediately after. That lets a reason
// be written above the call it excuses, which is where a reviewer looks,
// without allowing it to sit pages away.
func allowedLines(fset *token.FileSet, file *ast.File) map[int]bool {
	out := map[int]bool{}

	for _, group := range file.Comments {
		marked := false
		for _, c := range group.List {
			if strings.Contains(c.Text, allowComment) {
				marked = true
				break
			}
		}
		if !marked {
			continue
		}

		start := fset.Position(group.Pos()).Line
		end := fset.Position(group.End()).Line
		for line := start; line <= end+1; line++ {
			out[line] = true
		}
	}
	return out
}
