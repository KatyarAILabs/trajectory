// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockFile is an advisory exclusive lock on the buffer directory.
//
// The buffer is a single-writer log: two processes appending to the same
// segment interleave partial records, and the reader then sees framing that
// does not verify and discards whole segments as corrupt. That failure mode was
// observed in practice — a collector restarted while its predecessor was still
// draining, and tens of megabytes of already-redacted trajectories were thrown
// away with only a warning.
//
// Holding a lock turns that silent data loss into a refusal to start, which an
// operator can see and fix. This matters most in exactly the situation where it
// is easiest to get wrong: a rolling restart, or a pod rescheduled onto a
// volume the old pod has not finished releasing.
type lockFile struct {
	f *os.File
}

func acquireLock(dir string) (*lockFile, error) {
	path := filepath.Join(dir, "LOCK")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("buffer: open lock %s: %w", path, err)
	}

	// Non-blocking: waiting would hang startup behind a process that may
	// never exit, and a clear error now beats a mysterious hang.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf(
			"buffer: %s is locked by another process. A buffer directory has exactly one "+
				"writer: two collectors sharing it interleave partial records and corrupt "+
				"the log. If the previous collector has exited, remove %s and retry",
			dir, path)
	}

	// Record who holds it, so an operator investigating has somewhere to
	// start. This is advisory information, not a lock mechanism.
	f.Truncate(0)
	fmt.Fprintf(f, "pid %d\n", os.Getpid())

	return &lockFile{f: f}, nil
}

func (l *lockFile) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	// The flock is released by closing the descriptor. The file is left
	// behind on purpose: its presence is harmless, and removing it races
	// with another process that may have just opened it.
	err := l.f.Close()
	l.f = nil
	return err
}
