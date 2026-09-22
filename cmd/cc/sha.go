// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/KatyarAILabs/trajectory/internal/version"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func schemaVersion() string { return version.Schema }
