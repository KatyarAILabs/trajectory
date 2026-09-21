// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package buffer

import "errors"

// permanentError marks a failure that retrying cannot fix.
//
// The distinction matters more than it looks. A sink that is down returns the
// same "write failed" as a record the sink will never accept, and treating them
// alike is wrong in both directions:
//
//   - Dead-lettering on a sink outage turns a recoverable situation into manual
//     recovery work. A bucket unreachable for an hour would empty the entire
//     buffer into a dead-letter directory that someone then has to replay by
//     hand, when simply waiting would have delivered everything.
//   - Retrying a permanently bad record forever blocks every record behind it,
//     which is what F-8.3's dead-letter rule exists to prevent.
//
// So: transient failures retry indefinitely, bounded by the buffer's own byte
// and age limits (F-8.5), which is where "down too long" is already handled and
// made observable. Permanent failures dead-letter immediately.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks an error as unretryable, sending the record straight to the
// dead-letter queue.
//
// Use it for a record a sink will never accept: undecodable bytes, a schema the
// destination rejects, a key that is structurally invalid. Never for a network
// error, a timeout, a 5xx, or a credential problem that an operator may be
// about to fix.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether an error was marked unretryable.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
