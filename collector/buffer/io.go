// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// Append writes a record durably (F-8.1).
//
// Returns ErrFull when the buffer is at its byte bound and cannot make room.
// The caller surfaces that to the producer as a retryable 503 rather than
// dropping the data (§12, "disk full").
func (b *Buffer) Append(payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("buffer: closed")
	}

	need := int64(headerBytes + len(payload))

	// Make room by discarding fully-delivered segments before declaring the
	// buffer full; a queue that is 90% acknowledged is not actually full.
	if b.totalBytes+need > b.opts.MaxBytes {
		b.reclaimLocked()
	}
	if b.totalBytes+need > b.opts.MaxBytes {
		if err := b.evictOldestLocked("max_bytes"); err != nil {
			return err
		}
	}
	if b.totalBytes+need > b.opts.MaxBytes {
		return ErrFull
	}

	if err := b.ensureActiveLocked(); err != nil {
		return err
	}

	now := b.opts.Now()
	buf := make([]byte, headerBytes+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], magic)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[8:12], crc32.ChecksumIEEE(payload))
	binary.BigEndian.PutUint64(buf[12:20], uint64(now.UnixMicro()))
	copy(buf[headerBytes:], payload)

	if _, err := b.active.file.Write(buf); err != nil {
		return fmt.Errorf("buffer: append: %w", err)
	}

	if b.active.oldest.IsZero() {
		b.active.oldest = now
	}
	b.active.bytes += int64(len(buf))
	b.totalBytes += int64(len(buf))

	if b.active.bytes >= b.opts.SegmentBytes {
		if err := b.sealActiveLocked(); err != nil {
			return err
		}
	}
	return nil
}

// Sync flushes the active segment to disk.
//
// Called on a timer and before shutdown rather than on every append: an fsync
// per episode would cap throughput far below the 5k spans/s target in §13, and
// the at-least-once contract (F-8.4) already tolerates re-delivering the last
// unsynced records after a crash.
func (b *Buffer) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.syncLocked()
}

func (b *Buffer) syncLocked() error {
	if b.active == nil || b.active.file == nil {
		return nil
	}
	return b.active.file.Sync()
}

func (b *Buffer) ensureActiveLocked() error {
	if b.active != nil && b.active.file != nil && !b.active.sealed {
		return nil
	}

	var id uint64 = 1
	if len(b.segments) > 0 {
		id = b.segments[len(b.segments)-1].id + 1
	}

	path := b.segPath(id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("buffer: create segment: %w", err)
	}

	seg := &segment{id: id, path: path, file: f}
	b.segments = append(b.segments, seg)
	b.active = seg
	return nil
}

func (b *Buffer) sealActiveLocked() error {
	if b.active == nil {
		return nil
	}
	if b.active.file != nil {
		if err := b.active.file.Sync(); err != nil {
			return err
		}
		if err := b.active.file.Close(); err != nil {
			return err
		}
		b.active.file = nil
	}
	b.active.sealed = true
	b.active = nil
	return nil
}

// Entry is one record read from the buffer.
type Entry struct {
	Payload []byte
	// WrittenAt is when the record entered the buffer, for age metrics.
	WrittenAt time.Time

	segID  uint64
	endOff int64
}

// Next returns the record at the read cursor, or nil when the buffer is empty.
//
// It does not advance the cursor. The caller calls Ack only after the record is
// durably in a sink, which is what makes delivery at-least-once across a crash
// (F-8.4) rather than at-most-once.
func (b *Buffer) Next() (*Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		seg := b.segmentAtCursorLocked()
		if seg == nil {
			return nil, nil
		}

		if b.cursorOff >= seg.bytes {
			// Exhausted this segment. Move on; reclamation deletes
			// it once nothing needs it.
			next := b.nextSegmentLocked(seg.id)
			if next == nil {
				return nil, nil
			}
			b.cursorSeg = next.id
			b.cursorOff = 0
			continue
		}

		rf, err := b.readHandleLocked(seg)
		if err != nil {
			return nil, err
		}
		entry, err := readFromFile(rf, b.cursorOff)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errCorrupt) {
				// A corrupt tail on a sealed segment means the
				// rest is unreadable. Skipping to the next
				// segment loses those records, so it is
				// reported rather than silently swallowed.
				if b.opts.OnEvict != nil {
					b.opts.OnEvict("corrupt_record", 1, seg.bytes-b.cursorOff)
				}
				next := b.nextSegmentLocked(seg.id)
				if next == nil {
					return nil, nil
				}
				b.cursorSeg = next.id
				b.cursorOff = 0
				continue
			}
			return nil, err
		}

		entry.segID = seg.id
		return entry, nil
	}
}

// Ack marks an entry delivered and advances the cursor durably.
func (b *Buffer) Ack(e *Entry) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.cursorSeg = e.segID
	b.cursorOff = e.endOff

	if err := b.saveStateLocked(); err != nil {
		return err
	}
	b.reclaimLocked()
	return nil
}

// reclaimLocked deletes segments entirely behind the read cursor.
func (b *Buffer) reclaimLocked() {
	var keep []*segment
	for _, seg := range b.segments {
		if seg.id < b.cursorSeg && seg.sealed {
			seg.closeReadHandle()
			if err := os.Remove(seg.path); err == nil {
				b.totalBytes -= seg.bytes
				continue
			}
		}
		keep = append(keep, seg)
	}
	b.segments = keep
}

// evictOldestLocked drops the oldest sealed segment to make room.
//
// This is data loss, and it is deliberate: the alternative when a sink has been
// down long enough to fill the disk is to stop accepting entirely, and §12 says
// the buffer must never be corrupted and the eviction policy must be explicit
// and observable. Dropping the oldest keeps the most recent trajectories, which
// are the ones most likely still to matter.
func (b *Buffer) evictOldestLocked(reason string) error {
	for i, seg := range b.segments {
		if !seg.sealed {
			continue
		}
		seg.closeReadHandle()
		if err := os.Remove(seg.path); err != nil {
			return err
		}
		b.totalBytes -= seg.bytes
		b.segments = append(b.segments[:i], b.segments[i+1:]...)

		if b.opts.OnEvict != nil {
			b.opts.OnEvict(reason, 0, seg.bytes)
		}
		// The cursor may now point at a deleted segment.
		if b.cursorSeg <= seg.id {
			if len(b.segments) > 0 {
				b.cursorSeg = b.segments[0].id
				b.cursorOff = 0
			}
		}
		return nil
	}
	return nil
}

// EvictExpired drops segments older than MaxAge (F-8.5).
func (b *Buffer) EvictExpired() {
	b.mu.Lock()
	defer b.mu.Unlock()

	cutoff := b.opts.Now().Add(-b.opts.MaxAge)
	var keep []*segment

	for _, seg := range b.segments {
		if seg.sealed && !seg.oldest.IsZero() && seg.oldest.Before(cutoff) {
			seg.closeReadHandle()
			if err := os.Remove(seg.path); err == nil {
				b.totalBytes -= seg.bytes
				if b.opts.OnEvict != nil {
					b.opts.OnEvict("max_age", 0, seg.bytes)
				}
				if b.cursorSeg <= seg.id {
					b.cursorSeg = seg.id + 1
					b.cursorOff = 0
				}
				continue
			}
		}
		keep = append(keep, seg)
	}
	b.segments = keep
}

func (b *Buffer) segmentAtCursorLocked() *segment {
	for _, seg := range b.segments {
		if seg.id == b.cursorSeg {
			return seg
		}
	}
	// The cursor points before the first surviving segment, which happens
	// after eviction. Resume from the oldest that remains.
	if len(b.segments) > 0 && b.cursorSeg < b.segments[0].id {
		b.cursorSeg = b.segments[0].id
		b.cursorOff = 0
		return b.segments[0]
	}
	return nil
}

func (b *Buffer) nextSegmentLocked(after uint64) *segment {
	for _, seg := range b.segments {
		if seg.id > after {
			return seg
		}
	}
	return nil
}

// Bytes reports current on-disk size, which is what fills the volume.
func (b *Buffer) Bytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.totalBytes
}

// PendingBytes reports bytes not yet delivered (cc_buffer_bytes, §11).
//
// This is deliberately not the same as Bytes. Reclamation is per-segment, so
// acknowledged records occupy disk until the whole segment behind the cursor
// can be deleted. Reporting on-disk size as the backlog makes the gauge stay
// flat while delivery is in fact making progress — an operator watching it
// during an incident would conclude delivery was stuck when it was not.
//
// It is also the right input to backpressure: refusing new data because of
// bytes that have already been delivered would shed load for no reason.
func (b *Buffer) PendingBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pendingBytesLocked()
}

func (b *Buffer) pendingBytesLocked() int64 {
	var n int64
	for _, seg := range b.segments {
		switch {
		case seg.id > b.cursorSeg:
			n += seg.bytes
		case seg.id == b.cursorSeg:
			if rest := seg.bytes - b.cursorOff; rest > 0 {
				n += rest
			}
		}
	}
	return n
}

// OldestAge reports how long the oldest undelivered record has waited
// (cc_buffer_oldest_age_seconds, §11).
func (b *Buffer) OldestAge() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, seg := range b.segments {
		if seg.id >= b.cursorSeg && !seg.oldest.IsZero() {
			return b.opts.Now().Sub(seg.oldest)
		}
	}
	return 0
}

// UnderBackpressure reports whether sources should slow down (F-8.2).
//
// Keyed on undelivered bytes, not on-disk bytes: data already delivered but not
// yet reclaimed is not a reason to refuse a producer.
func (b *Buffer) UnderBackpressure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return float64(b.pendingBytesLocked()) >= float64(b.opts.MaxBytes)*b.opts.BackpressureAt
}

// Close seals and flushes.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true

	if err := b.syncLocked(); err != nil {
		return err
	}
	if b.active != nil && b.active.file != nil {
		if err := b.active.file.Close(); err != nil {
			return err
		}
		b.active.file = nil
	}
	for _, seg := range b.segments {
		seg.closeReadHandle()
	}
	if err := b.saveStateLocked(); err != nil {
		return err
	}
	return b.lock.release()
}

// PeekAfter returns the record following the last entry in seen, without
// advancing the cursor.
//
// It exists so a deliverer can assemble a batch before acknowledging any of it.
// Passing the batch so far, rather than holding a read position, keeps the
// buffer free of per-reader state: the cursor still moves only on Ack.
func (b *Buffer) PeekAfter(seen []*Entry) (*Entry, error) {
	b.mu.Lock()
	segID, off := b.cursorSeg, b.cursorOff
	b.mu.Unlock()

	if n := len(seen); n > 0 {
		segID, off = seen[n-1].segID, seen[n-1].endOff
	}
	return b.readFrom(segID, off)
}

// readFrom returns the record at a position, skipping to the next segment when
// the current one is exhausted.
func (b *Buffer) readFrom(segID uint64, off int64) (*Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		seg := b.findSegmentLocked(segID)
		if seg == nil {
			// The position is before the oldest surviving segment,
			// which happens after eviction. Resume from the oldest.
			if len(b.segments) > 0 && segID < b.segments[0].id {
				segID, off = b.segments[0].id, 0
				continue
			}
			return nil, nil
		}

		if off >= seg.bytes {
			next := b.nextSegmentLocked(seg.id)
			if next == nil {
				return nil, nil
			}
			segID, off = next.id, 0
			continue
		}

		rf, err := b.readHandleLocked(seg)
		if err != nil {
			return nil, err
		}

		entry, err := readFromFile(rf, off)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errCorrupt) {
				if b.opts.OnEvict != nil {
					b.opts.OnEvict("corrupt_record", 1, seg.bytes-off)
				}
				next := b.nextSegmentLocked(seg.id)
				if next == nil {
					return nil, nil
				}
				segID, off = next.id, 0
				continue
			}
			return nil, err
		}

		entry.segID = seg.id
		return entry, nil
	}
}

// readHandleLocked returns a cached read handle for a segment, opening it once.
func (b *Buffer) readHandleLocked(seg *segment) (*os.File, error) {
	if seg.rf != nil {
		return seg.rf, nil
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return nil, err
	}
	seg.rf = f
	return f, nil
}

// closeReadHandle releases a segment's cached reader, so a reclaimed or
// evicted segment does not hold a descriptor open.
func (seg *segment) closeReadHandle() {
	if seg.rf != nil {
		_ = seg.rf.Close()
		seg.rf = nil
	}
}

func (b *Buffer) findSegmentLocked(id uint64) *segment {
	for _, seg := range b.segments {
		if seg.id == id {
			return seg
		}
	}
	return nil
}
