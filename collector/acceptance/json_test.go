// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"encoding/json"
	"testing"
)

func jsonMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
