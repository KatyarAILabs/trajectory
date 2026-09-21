// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Package buffer is the durable queue between the processors and a sink (F-8).
//
// It exists so that a sink outage or a process kill does not lose data that was
// acknowledged to a producer (G-4). Everything in it has already been redacted
// (F-5.3), so the on-disk buffer contains no unredacted payloads even if the
// disk is seized or snapshotted.
//
// The design is a segmented append-only log with a persisted read cursor:
//
//	buffer/
//	  0000000001.seg   append-only records, sealed when full
//	  0000000002.seg   the segment currently being written
//	  state.json       read cursor (segment + offset)
//
// Why not a library: the requirement is a queue bounded by **bytes and age**
// with an explicit, observable eviction policy (F-8.5). General-purpose WALs
// bound by record count or not at all, and the eviction policy is the part an
// operator has to reason about when the disk fills at 3am.
package buffer

import (
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Record framing. A torn write — the process died mid-append — is detected by
// the CRC and truncated on recovery, rather than being handed to a sink as
// corrupt data.
const (
	magic       = 0x54524a31    // "TRJ1"
	headerBytes = 4 + 4 + 4 + 8 // magic | length | crc32 | unix micros
)

// ErrFull is returned when the buffer is at its byte bound and the eviction
// policy is to reject rather than drop.
var ErrFull = fmt.Errorf("buffer is full")

// Options configure a Buffer.
type Options struct {
	Dir string
	// MaxBytes bounds total on-disk size (F-8.5).
	MaxBytes int64
	// MaxAge bounds how long a record may sit undelivered (F-8.5).
	MaxAge time.Duration
	// SegmentBytes is the size at which a segment is sealed and a new one
	// started. Smaller segments reclaim space sooner; larger ones mean
	// fewer files.
	SegmentBytes int64
	// BackpressureAt is the fraction of MaxBytes at which the buffer starts
	// asking sources to slow down (F-8.2).
	BackpressureAt float64
	// OnEvict is called for records dropped by the eviction policy, so the
	// loss is observable rather than silent (F-8.5).
	OnEvict func(reason string, n int, bytes int64)

	// EncryptionKey, when set, encrypts every record at rest (F-8.6).
	EncryptionKey []byte

	Now func() time.Time
}

// Buffer is a durable FIFO queue on disk.
type Buffer struct {
	opts Options

	mu       sync.Mutex
	segments []*segment
	active   *segment
	// cursor is the read position: which segment, and the offset within it.
	cursorSeg uint64
	cursorOff int64

	totalBytes int64
	closed     bool
	lock       *lockFile
	aead       cipher.AEAD
}

type segment struct {
	id   uint64
	path string
	// file is the append handle for the active segment.
	file *os.File
	// rf is a cached read handle. Reads are the hot path during delivery,
	// and opening the segment per record cost four syscalls each.
	rf    *os.File
	bytes int64
	// sealed segments are never appended to again, which is what makes
	// eviction a whole-file delete rather than a rewrite.
	sealed bool
	// oldest is the timestamp of the first record, for age-based eviction.
	oldest time.Time
}

type state struct {
	CursorSegment uint64 `json:"cursor_segment"`
	CursorOffset  int64  `json:"cursor_offset"`
}

// Open opens or recovers a buffer directory.
//
// Recovery is not optional and not a fast path: a collector that starts after a
// crash must not silently begin with an empty queue, and must not hand a
// half-written record to a sink.
func Open(opts Options) (*Buffer, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("buffer: dir is required")
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 8 << 30
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = 24 * time.Hour
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}
	if opts.BackpressureAt <= 0 || opts.BackpressureAt > 1 {
		opts.BackpressureAt = 0.8
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("buffer: create %s: %w", opts.Dir, err)
	}

	// Exclusive: a second writer on the same directory corrupts the log.
	lock, err := acquireLock(opts.Dir)
	if err != nil {
		return nil, err
	}

	b := &Buffer{opts: opts, lock: lock}

	fingerprint := "none"
	if len(opts.EncryptionKey) > 0 {
		aead, err := NewAEAD(opts.EncryptionKey)
		if err != nil {
			lock.release()
			return nil, err
		}
		b.aead = aead
		fingerprint = Fingerprint(opts.EncryptionKey)
	}

	if err := b.recover(); err != nil {
		lock.release()
		return nil, err
	}
	if err := checkKey(opts.Dir, fingerprint, b.PendingBytes() > 0); err != nil {
		lock.release()
		return nil, err
	}
	return b, nil
}

// recover rebuilds in-memory state from the directory and truncates any torn
// tail on the active segment.
func (b *Buffer) recover() error {
	entries, err := os.ReadDir(b.opts.Dir)
	if err != nil {
		return err
	}

	var ids []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".seg"), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if err := b.loadState(); err != nil {
		return err
	}

	for i, id := range ids {
		path := b.segPath(id)
		info, err := os.Stat(path)
		if err != nil {
			return err
		}

		seg := &segment{id: id, path: path, bytes: info.Size(), sealed: true}
		if ts, ok := firstTimestamp(path); ok {
			seg.oldest = ts
		}

		// Every segment is scanned, not only the last.
		//
		// In the normal case a kill can only tear the segment being
		// written, so the others verify in one pass and cost nothing.
		// But a buffer that was written by two processes, or copied
		// mid-write, can have damage anywhere, and skipping to the next
		// segment on a bad record discards everything after it. Finding
		// the tear precisely loses only what was actually damaged.
		{
			n, err := truncateTorn(path)
			if err != nil {
				return err
			}
			if lost := info.Size() - n; lost > 0 && b.opts.OnEvict != nil {
				b.opts.OnEvict("torn_or_corrupt_tail", 0, lost)
			}
			seg.bytes = n
		}

		if i == len(ids)-1 {
			n := seg.bytes
			seg.bytes = n
			seg.sealed = false
		}

		b.segments = append(b.segments, seg)
		b.totalBytes += seg.bytes
	}

	if len(b.segments) > 0 {
		b.active = b.segments[len(b.segments)-1]
		if b.active.bytes >= b.opts.SegmentBytes {
			b.active.sealed = true
			b.active = nil
		}
	}
	return nil
}

func (b *Buffer) segPath(id uint64) string {
	return filepath.Join(b.opts.Dir, fmt.Sprintf("%010d.seg", id))
}

func (b *Buffer) statePath() string { return filepath.Join(b.opts.Dir, "state.json") }

func (b *Buffer) loadState() error {
	data, err := os.ReadFile(b.statePath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var st state
	if err := json.Unmarshal(data, &st); err != nil {
		// A corrupt cursor means we do not know what was delivered.
		// Starting from the beginning re-delivers, which at-least-once
		// permits (F-8.4); starting from the end would lose data, which
		// it does not.
		return nil
	}
	b.cursorSeg = st.CursorSegment
	b.cursorOff = st.CursorOffset
	return nil
}

func (b *Buffer) saveStateLocked() error {
	data, err := json.Marshal(state{CursorSegment: b.cursorSeg, CursorOffset: b.cursorOff})
	if err != nil {
		return err
	}

	tmp := b.statePath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, b.statePath())
}
