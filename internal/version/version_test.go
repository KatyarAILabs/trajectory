// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package version

import (
	"os"
	"strings"
	"testing"
)

// spec/VERSION is consumed by tooling that cannot import Go. It must never
// drift from the constant that actually gets stamped onto records.
func TestSpecVersionFileMatchesConstant(t *testing.T) {
	b, err := os.ReadFile("../../spec/VERSION")
	if err != nil {
		t.Fatalf("read spec/VERSION: %v", err)
	}
	if got := strings.TrimSpace(string(b)); got != Schema {
		t.Fatalf("spec/VERSION is %q but version.Schema is %q; update both", got, Schema)
	}
}
