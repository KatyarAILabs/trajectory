// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"github.com/trajectory-project/trajectory/collector/pipeline"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// cloneAssembled deep-copies the parts of an episode that redaction mutates,
// so Preview can report what a policy would do without doing it.
//
// Only the mutable fields are copied. A shallow copy would share the maps and
// the pointed-to payload strings, which would make `cc redact --test` silently
// modify the file it was asked to inspect — the opposite of a dry run.
func cloneAssembled(in *pipeline.Assembled) *pipeline.Assembled {
	out := &pipeline.Assembled{
		Episode: in.Episode,
		Steps:   make([]record.Step, len(in.Steps)),
	}
	out.Episode.Raw = cloneMap(in.Episode.Raw)

	for i, s := range in.Steps {
		s.Raw = cloneMap(s.Raw)
		if s.ContentInline != nil {
			v := *s.ContentInline
			s.ContentInline = &v
		}
		out.Steps[i] = s
	}
	return out
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
