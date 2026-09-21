// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Encryption at rest for the buffer (F-8.6).
//
// Redaction already runs before the buffer (F-5.3), so an unencrypted buffer
// holds no unredacted payloads. Encryption covers what redaction deliberately
// keeps — the allow-listed prompts and results — for a deployment where the
// buffer volume is a snapshot away from someone who should not read it.
//
// AES-256-GCM per record, with a random nonce stored in front of the
// ciphertext. GCM authenticates as well as encrypts, so a tampered record fails
// to open rather than decrypting to garbage that a sink would then write.

// NewAEAD builds a cipher from a key. The key may be 32 raw bytes or 64 hex
// characters, because both are how operators actually store keys.
func NewAEAD(key []byte) (cipher.AEAD, error) {
	k := key
	if t := strings.TrimSpace(string(key)); len(t) == 64 {
		if dec, err := hex.DecodeString(t); err == nil {
			k = dec
		}
	}
	if len(k) != 32 {
		return nil, fmt.Errorf(
			"buffer encryption key must be 32 bytes or 64 hex characters, got %d bytes", len(k))
	}
	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Fingerprint identifies a key without revealing it.
func Fingerprint(key []byte) string {
	sum := sha256.Sum256(append([]byte("trajectory-buffer-key:"), key...))
	return hex.EncodeToString(sum[:8])
}

func seal(aead cipher.AEAD, plain []byte) ([]byte, error) {
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plain, nil), nil
}

func openSealed(aead cipher.AEAD, sealed []byte) ([]byte, error) {
	n := aead.NonceSize()
	if len(sealed) < n {
		return nil, errDecrypt
	}
	out, err := aead.Open(nil, sealed[:n], sealed[n:], nil)
	if err != nil {
		return nil, errDecrypt
	}
	return out, nil
}

var errDecrypt = fmt.Errorf("buffer: record failed to decrypt")

// checkKey guards against the one way encryption turns into data loss.
//
// Records sealed under one key cannot be opened with another, and a record that
// cannot be opened is indistinguishable from a corrupt one — so changing the
// key with undelivered data in the buffer would silently discard all of it. The
// buffer records which key (or none) it was written with, and refuses to start
// on a mismatch while anything is left to deliver.
func checkKey(dir, fingerprint string, pending bool) error {
	path := filepath.Join(dir, "KEY")
	prev, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	stored := strings.TrimSpace(string(prev))

	if os.IsNotExist(err) || !pending || stored == fingerprint {
		return os.WriteFile(path, []byte(fingerprint+"\n"), 0o600)
	}

	describe := func(fp string) string {
		if fp == "none" {
			return "no encryption"
		}
		return "key " + fp
	}
	return fmt.Errorf(
		"buffer %s holds undelivered records written with %s, but this process is "+
			"configured with %s. Starting would make those records unreadable and they "+
			"would be discarded. Start once with the previous setting to drain the buffer, "+
			"then change it", dir, describe(stored), describe(fingerprint))
}
