// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"golang.org/x/time/rate"

	"github.com/KatyarAILabs/trajectory/collector/config"
)

// quotas holds a token bucket per source (F-7.3).
//
// Single-tenant deployments (Q-5) make the source the natural unit: it is the
// thing an operator configured, named and can reason about. A limit is what
// stops one runaway producer from filling the buffer that every other producer
// depends on.
type quotas struct {
	limiters map[string]*rate.Limiter
}

func newQuotas(sources []config.Source) *quotas {
	q := &quotas{limiters: map[string]*rate.Limiter{}}
	for _, s := range sources {
		if s.RateLimit.RecordsPerSecond > 0 {
			q.limiters[s.Name] = rate.NewLimiter(
				rate.Limit(s.RateLimit.RecordsPerSecond), s.RateLimit.Burst)
		}
	}
	return q
}

// allow reports whether a source may send one more record. A source with no
// configured limit is always allowed.
func (q *quotas) allow(source string) bool {
	l, ok := q.limiters[source]
	if !ok {
		return true
	}
	return l.Allow()
}
