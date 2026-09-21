// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"fmt"
	"os"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/extract"
	"github.com/trajectory-project/trajectory/collector/redact"
	"github.com/trajectory-project/trajectory/collector/sample"
)

// policy is every setting that decides what happens to a record, as opposed to
// where it comes from or goes to.
//
// It is built whole and swapped atomically, which is what makes hot reload
// safe (F-11.4): an episode is processed entirely under one policy or entirely
// under the next, never redacted by the old rules and sampled by the new ones.
// A half-applied redaction policy is exactly the state that leaks.
type policy struct {
	redactor *redact.Redactor
	// bySource holds per-source redaction overrides (F-5.7).
	bySource  map[string]*redact.Redactor
	extractor *extract.Extractor
	sampler   *sample.Sampler
	quotas    *quotas
}

// buildPolicy compiles a policy from config. Any error means nothing is
// swapped in, so a bad reload leaves the running policy untouched (§12).
func buildPolicy(cfg *config.Config, onManifest func(redact.Manifest)) (*policy, error) {
	key := tokenKey(cfg.Redaction)

	p := &policy{bySource: map[string]*redact.Redactor{}}

	var err error
	p.redactor, err = redact.New(cfg.Redaction, key, onManifest)
	if err != nil {
		return nil, err
	}

	for _, src := range cfg.Sources {
		if src.Redaction == nil {
			continue
		}
		r, err := redact.New(*src.Redaction, tokenKey(*src.Redaction), onManifest)
		if err != nil {
			return nil, fmt.Errorf("source %q redaction: %w", src.Name, err)
		}
		p.bySource[src.Name] = r
	}

	if p.extractor, err = extract.New(cfg.Entities); err != nil {
		return nil, err
	}
	if p.sampler, err = sample.New(cfg.Sampling); err != nil {
		return nil, err
	}
	for _, src := range cfg.Sources {
		if src.HeadSampleRate != nil {
			p.sampler.SetSourceRate(src.Name, *src.HeadSampleRate)
		}
	}
	p.quotas = newQuotas(cfg.Sources)
	return p, nil
}

// redactorFor returns the policy that applies to a source.
func (p *policy) redactorFor(source string) *redact.Redactor {
	if r, ok := p.bySource[source]; ok {
		return r
	}
	return p.redactor
}

// tokenKey reads the HMAC key from the environment (F-11.1). It is never read
// from the config file.
func tokenKey(r config.Redaction) []byte {
	if env := r.Tokenization.KeyEnv; env != "" {
		if v := os.Getenv(env); v != "" {
			return []byte(v)
		}
	}
	return nil
}
