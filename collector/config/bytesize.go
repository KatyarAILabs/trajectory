// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"strconv"
	"strings"
)

// ByteSize accepts either a plain number of bytes or a human suffix.
//
// The spec's example config writes `max_bytes: 8Gi` (§10), and an operator
// sizing a buffer volume thinks in gibibytes, not in 8589934592. Accepting only
// the number would make the published example invalid.
type ByteSize int64

var byteUnits = []struct {
	suffix string
	mult   int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
	{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000},
	{"TB", 1000 * 1000 * 1000 * 1000},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
	{"B", 1},
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(unmarshal func(any) error) error {
	// A bare number is bytes.
	var n int64
	if err := unmarshal(&n); err == nil {
		*b = ByteSize(n)
		return nil
	}

	var s string
	if err := unmarshal(&s); err != nil {
		return fmt.Errorf("must be a number of bytes or a size like 8Gi")
	}

	v, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// ParseByteSize parses a size string.
func ParseByteSize(s string) (ByteSize, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0, nil
	}

	for _, u := range byteUnits {
		if strings.HasSuffix(t, u.suffix) {
			num := strings.TrimSpace(strings.TrimSuffix(t, u.suffix))
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			if f < 0 {
				return 0, fmt.Errorf("size %q is negative", s)
			}
			return ByteSize(f * float64(u.mult)), nil
		}
	}

	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q; use a number of bytes or a suffix like 8Gi", s)
	}
	return ByteSize(n), nil
}

// String renders a size with the largest exact binary suffix.
func (b ByteSize) String() string {
	n := int64(b)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"Ti", 1 << 40}, {"Gi", 1 << 30}, {"Mi", 1 << 20}, {"Ki", 1 << 10}} {
		if n >= u.mult && n%u.mult == 0 {
			return fmt.Sprintf("%d%s", n/u.mult, u.suffix)
		}
	}
	return fmt.Sprintf("%d", n)
}
