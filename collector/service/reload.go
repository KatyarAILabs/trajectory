// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"fmt"
	"reflect"

	"github.com/trajectory-project/trajectory/collector/config"
)

// Reload applies a new configuration without dropping in-flight data (F-11.4).
//
// What reloads live is the policy — redaction, entity extraction, sampling and
// quotas. Those decide what happens to a record and are safe to swap between
// records, because the swap is atomic and an episode is processed under a
// single policy from start to finish.
//
// What does not reload is structure: listeners, the buffer, the sink and the
// assembly window. Changing those live would mean re-binding ports with
// traffic in flight or moving a buffer out from under its own cursor, which is
// how a reload turns into data loss. Those changes are reported and need a
// restart.
//
// A config that fails to build leaves the running policy untouched and is
// counted (§12, "config invalid at reload: keep running the previous config").
func (s *Service) Reload(next *config.Config) (needsRestart []string, err error) {
	pol, err := buildPolicy(next, s.onManifest)
	if err != nil {
		s.metrics.ConfigReloads.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("reload rejected, previous config still in force: %w", err)
	}

	s.mu.Lock()
	prev := s.cfg
	s.mu.Unlock()

	needsRestart = structuralChanges(prev, next)

	s.pol.Store(pol)

	s.mu.Lock()
	// Keep the structural fields of the running config: they were not
	// applied, so recording them as current would misreport what is live.
	merged := *next
	merged.Sources = mergeSourcePolicy(prev.Sources, next.Sources)
	merged.Buffer = prev.Buffer
	merged.Sinks = prev.Sinks
	merged.Assembly = prev.Assembly
	merged.Tenant = prev.Tenant
	s.cfg = &merged
	s.mu.Unlock()

	s.metrics.ConfigReloads.WithLabelValues("ok").Inc()
	s.log.Info("configuration reloaded",
		"key_id", pol.redactor.KeyID(),
		"rules", len(next.Redaction.Rules),
		"needs_restart", needsRestart)
	return needsRestart, nil
}

// structuralChanges names the sections that differ but were not applied.
func structuralChanges(prev, next *config.Config) []string {
	var out []string
	if prev.Tenant != next.Tenant {
		out = append(out, "tenant")
	}
	if !reflect.DeepEqual(prev.Assembly, next.Assembly) {
		out = append(out, "assembly")
	}
	if !reflect.DeepEqual(prev.Buffer, next.Buffer) {
		out = append(out, "buffer")
	}
	if !reflect.DeepEqual(prev.Sinks, next.Sinks) {
		out = append(out, "sinks")
	}
	if !reflect.DeepEqual(listeners(prev.Sources), listeners(next.Sources)) {
		out = append(out, "sources (listeners, auth or TLS)")
	}
	return out
}

// listeners reduces sources to the parts a reload cannot change.
func listeners(ss []config.Source) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%s|%v|%v",
			s.Name, s.Type, s.HTTP.Listen, s.GRPC.Listen, s.Auth.TokenEnv,
			s.HTTP.TLS, s.GRPC.TLS))
	}
	return out
}

// mergeSourcePolicy keeps each running source's structure and takes only its
// policy fields — rate limit and redaction override — from the new config.
func mergeSourcePolicy(prev, next []config.Source) []config.Source {
	byName := map[string]config.Source{}
	for _, s := range next {
		byName[s.Name] = s
	}
	out := make([]config.Source, len(prev))
	for i, s := range prev {
		if n, ok := byName[s.Name]; ok {
			s.RateLimit = n.RateLimit
			s.Redaction = n.Redaction
		}
		out[i] = s
	}
	return out
}
