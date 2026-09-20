// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package fsstore

import "bytes"

// bytesBuffer adapts bytes.Buffer to the io.WriteSeeker the Parquet writer
// wants. Parquet writes its footer after the data and seeks back to record
// offsets, so a plain io.Writer is not enough.
type bytesBuffer struct {
	buf bytes.Buffer
	pos int64
}

func (b *bytesBuffer) Write(p []byte) (int, error) {
	// The Parquet writer appends sequentially and only seeks within what it
	// has already written, so an append is always correct here.
	n, err := b.buf.Write(p)
	b.pos += int64(n)
	return n, err
}

func (b *bytesBuffer) Bytes() []byte { return b.buf.Bytes() }
func (b *bytesBuffer) Len() int      { return b.buf.Len() }
