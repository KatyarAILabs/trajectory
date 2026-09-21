// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "errors"

// ErrQuotaExceeded is returned by Next when a source has exceeded its rate
// limit (F-7.3). Sources map it to 429 rather than 503, because the remedy is
// different: a 503 says the collector is struggling and the producer should
// back off; a 429 says this producer in particular is sending too much.
var ErrQuotaExceeded = errors.New("quota exceeded")

// IsQuotaExceeded reports whether a refusal was a quota rather than
// backpressure.
func IsQuotaExceeded(err error) bool { return errors.Is(err, ErrQuotaExceeded) }
