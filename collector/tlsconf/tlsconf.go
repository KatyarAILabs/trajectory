// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package tlsconf builds TLS configuration for ingest listeners (F-12.1).
//
// Every ingest path can be TLS, and optionally mTLS. The defaults here are
// deliberately stricter than Go's: a collector receives prompts and tool
// arguments, so the transport carrying them should not negotiate down to
// something from 2015 because an old client asked nicely.
package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Config is the YAML shape for a listener's TLS settings.
type Config struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// ClientCAFile turns on mutual TLS: a client must present a
	// certificate signed by this CA.
	ClientCAFile string `yaml:"client_ca_file"`
	// MinVersion is "1.2" or "1.3". The default is 1.2, because 1.3-only
	// still breaks some SDK HTTP stacks in the wild, and refusing to start
	// is worse than a well-configured 1.2.
	MinVersion string `yaml:"min_version"`
}

// Enabled reports whether TLS is configured.
func (c Config) Enabled() bool { return c.CertFile != "" || c.KeyFile != "" }

// MutualTLS reports whether client certificates are required.
func (c Config) MutualTLS() bool { return c.ClientCAFile != "" }

// Validate checks the settings without loading them, for `cc validate`.
func (c Config) Validate(key string, bad func(string, string, ...any)) {
	if !c.Enabled() {
		if c.ClientCAFile != "" {
			bad(key+".client_ca_file",
				"mutual TLS needs a server certificate too; set cert_file and key_file")
		}
		return
	}

	if c.CertFile == "" {
		bad(key+".cert_file", "required when key_file is set")
	}
	if c.KeyFile == "" {
		bad(key+".key_file", "required when cert_file is set")
	}

	for name, path := range map[string]string{
		key + ".cert_file":      c.CertFile,
		key + ".key_file":       c.KeyFile,
		key + ".client_ca_file": c.ClientCAFile,
	} {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			bad(name, "cannot read %s: %v", path, err)
		}
	}

	switch c.MinVersion {
	case "", "1.2", "1.3":
	default:
		bad(key+".min_version", "must be \"1.2\" or \"1.3\", got %q", c.MinVersion)
	}
}

// Build returns a *tls.Config, or nil when TLS is not configured.
func Build(c Config) (*tls.Config, error) {
	if !c.Enabled() {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load key pair: %w", err)
	}

	min := uint16(tls.VersionTLS12)
	if c.MinVersion == "1.3" {
		min = tls.VersionTLS13
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   min,
		// An explicit suite list for TLS 1.2. Go's defaults are
		// reasonable, but pinning forward-secret AEAD suites only means
		// a future Go that re-admits something weaker for compatibility
		// does not quietly change what this collector accepts. TLS 1.3
		// ignores this field and has no weak suites to pick from.
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
	}

	if c.ClientCAFile != "" {
		pem, err := os.ReadFile(c.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls: client CA file %s contains no usable certificate",
				c.ClientCAFile)
		}
		cfg.ClientCAs = pool
		// Require and verify: anything weaker makes the client
		// certificate decorative.
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// Describe summarises the configuration for startup logging. It names files,
// never their contents.
func Describe(c Config) string {
	if !c.Enabled() {
		return "plaintext"
	}
	s := "TLS"
	if c.MinVersion != "" {
		s += " min " + c.MinVersion
	}
	if c.MutualTLS() {
		s += " with required client certificates"
	}
	return s
}
