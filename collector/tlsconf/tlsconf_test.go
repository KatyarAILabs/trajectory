// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package tlsconf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeCert generates a self-signed certificate for testing.
func writeCert(t *testing.T, dir, name string, isCA bool) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")

	certOut, _ := os.Create(certPath)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	certOut.Close()

	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyOut, _ := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	keyOut.Close()

	return certPath, keyPath
}

func TestDisabledByDefault(t *testing.T) {
	cfg, err := Build(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Error("TLS was built from an empty config")
	}
}

func TestBuildServerTLS(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCert(t, dir, "server", false)

	cfg, err := Build(Config{CertFile: cert, KeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("no TLS config returned")
	}

	// TLS 1.0 and 1.1 are broken; a collector carrying prompts must not
	// negotiate down to them because an old client asked.
	if cfg.MinVersion < tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want at least TLS 1.2", cfg.MinVersion)
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Error("client certificates required without a CA configured")
	}

	// Every negotiable 1.2 suite must be forward-secret and AEAD.
	for _, s := range cfg.CipherSuites {
		name := tls.CipherSuiteName(s)
		if !contains(name, "ECDHE") {
			t.Errorf("cipher suite %s is not forward-secret", name)
		}
		if !contains(name, "GCM") && !contains(name, "CHACHA20") {
			t.Errorf("cipher suite %s is not AEAD", name)
		}
	}
}

// mTLS must actually require and verify, not merely request. Anything weaker
// makes the client certificate decorative.
func TestMutualTLSRequiresAndVerifies(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCert(t, dir, "server", false)
	ca, _ := writeCert(t, dir, "ca", true)

	cfg, err := Build(Config{CertFile: cert, KeyFile: key, ClientCAFile: ca})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Error("no client CA pool was loaded")
	}
}

func TestMinVersion13(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCert(t, dir, "server", false)

	cfg, err := Build(Config{CertFile: cert, KeyFile: key, MinVersion: "1.3"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %x, want TLS 1.3", cfg.MinVersion)
	}
}

// A CA file with no usable certificate must fail loudly. Silently accepting it
// would leave mTLS configured but verifying nothing.
func TestGarbageCAFileRejected(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCert(t, dir, "server", false)

	bad := filepath.Join(dir, "not-a-ca.pem")
	os.WriteFile(bad, []byte("this is not a certificate"), 0o600)

	if _, err := Build(Config{CertFile: cert, KeyFile: key, ClientCAFile: bad}); err == nil {
		t.Fatal("a CA file containing no certificate was accepted")
	}
}

// Validation must catch a missing file before the listener binds, not at the
// first connection.
func TestValidateCatchesMissingFiles(t *testing.T) {
	var errs []string
	bad := func(key, format string, args ...any) { errs = append(errs, key) }

	Config{CertFile: "/nonexistent.crt", KeyFile: "/nonexistent.key"}.Validate("s.tls", bad)
	if len(errs) == 0 {
		t.Error("missing certificate files were not reported")
	}

	// mTLS without a server certificate is incoherent.
	errs = nil
	Config{ClientCAFile: "/some/ca.pem"}.Validate("s.tls", bad)
	if len(errs) == 0 {
		t.Error("client_ca_file without cert_file was accepted")
	}
}

func TestDescribeNamesNoSecrets(t *testing.T) {
	d := Describe(Config{CertFile: "/certs/tls.crt", KeyFile: "/certs/tls.key", ClientCAFile: "/certs/ca.crt"})
	if !contains(d, "client certificates") {
		t.Errorf("Describe did not mention mTLS: %q", d)
	}
	if contains(d, "tls.key") {
		t.Errorf("Describe leaked a key path: %q", d)
	}
	if got := Describe(Config{}); got != "plaintext" {
		t.Errorf("Describe(empty) = %q, want plaintext", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
