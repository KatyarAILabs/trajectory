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

// errCorrupt means a record's framing or checksum did not verify. It is
// deliberately distinct from an I/O error: corruption is recoverable by
// truncating, an I/O error is not something to paper over.
var errCorrupt = errors.New("buffer: corrupt record")

// readAt reads one record at an offset, opening the file for the call.
//
// Used only where a single read is needed — recovery and scanning. The hot
// path goes through readFromFile with a cached handle: opening the segment per
// record cost four syscalls each, which a profile showed dominating delivery.
func readAt(path string, off int64) (*Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readFromFile(f, off)
}

// readFromFile reads one record from an already-open segment.
func readFromFile(f *os.File, off int64) (*Entry, error) {
	hdr := make([]byte, headerBytes)
	if _, err := f.ReadAt(hdr, off); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}

	if binary.BigEndian.Uint32(hdr[0:4]) != magic {
		return nil, errCorrupt
	}
	length := binary.BigEndian.Uint32(hdr[4:8])
	want := binary.BigEndian.Uint32(hdr[8:12])
	writtenAt := time.UnixMicro(int64(binary.BigEndian.Uint64(hdr[12:20])))

	// A length field is attacker-influenced only insofar as the buffer is
	// written by this process, but a corrupt one could otherwise ask for a
	// multi-gigabyte allocation.
	if length > maxRecordBytes {
		return nil, errCorrupt
	}

	payload := make([]byte, length)
	if _, err := f.ReadAt(payload, off+headerBytes); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}

	if crc32.ChecksumIEEE(payload) != want {
		return nil, errCorrupt
	}

	return &Entry{
		Payload:   payload,
		WrittenAt: writtenAt,
		endOff:    off + headerBytes + int64(length),
	}, nil
}

// maxRecordBytes caps one buffered record. A payload larger than this could
// not have been written by Append in the first place, so exceeding it means
// the header is corrupt.
const maxRecordBytes = 512 << 20

// truncateTorn scans a segment and truncates at the first unreadable record,
// returning the surviving length.
//
// This is the crash-recovery path. A process killed mid-append leaves a partial
// record; handing it to a sink would write corrupt data, and refusing to start
// would turn one bad write into an outage. Truncating loses only the record
// that was never completed, which by definition was never acknowledged.
func truncateTorn(path string) (int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()

	var off int64
	for off < size {
		e, err := readAt(path, off)
		if err != nil {
			break
		}
		off = e.endOff
	}

	if off < size {
		if err := f.Truncate(off); err != nil {
			return 0, fmt.Errorf("buffer: truncate torn segment %s: %w", path, err)
		}
		if err := f.Sync(); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// firstTimestamp reads the timestamp of a segment's first record, for
// age-based eviction.
func firstTimestamp(path string) (time.Time, bool) {
	e, err := readAt(path, 0)
	if err != nil {
		return time.Time{}, false
	}
	return e.WrittenAt, true
}
