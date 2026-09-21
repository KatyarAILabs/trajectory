// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package objstore is the object-writing boundary the sinks share.
//
// Local filesystem and S3 differ in exactly one respect that matters to the
// sink: how bytes become durably visible at a key. Everything else — the §8
// layout, blob fan-out, manifest-written-last, partitioning — is identical, and
// keeping it identical is what makes "it worked against MinIO locally" mean
// something about production.
package objstore

import (
	"context"
	"fmt"
)

// Store writes immutable objects addressed by key.
//
// Implementations must make an object visible atomically: a reader either sees
// the whole object or does not see it at all (F-9.4). No implementation may
// expose a partially written object under its final key.
type Store interface {
	// Put writes an object. Overwriting an existing key is allowed; the
	// collector only does so for idempotent rewrites of the same content.
	Put(ctx context.Context, key string, body []byte) error
	// Exists reports whether a key is present, used for blob deduplication
	// (F-9.3). It is authoritative, unlike the in-process cache in front of
	// it, so a restarted collector does not rewrite every blob it has seen.
	Exists(ctx context.Context, key string) (bool, error)
	// Get reads an object back, used by cc replay and cc inspect.
	Get(ctx context.Context, key string) ([]byte, error)
	// Describe is a human-readable destination, for logs and `cc validate`.
	// It must never contain a credential.
	Describe() string
	// Close releases resources.
	Close() error
}

// ErrNotFound is returned by Get for a missing key.
var ErrNotFound = fmt.Errorf("objstore: not found")
