// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Deliverer drains a buffer into a sink with retry, backoff and dead-lettering
// (F-8.3).
type Deliverer struct {
	buf  *Buffer
	send func(context.Context, []byte) error

	opts DeliveryOptions

	mu    sync.Mutex
	stats DeliveryStats
}

// DeliveryOptions configure retry behaviour.
type DeliveryOptions struct {
	// MaxAttempts before a record is dead-lettered. Zero means the default.
	MaxAttempts int
	// BaseDelay is the first backoff interval; it doubles per attempt.
	BaseDelay time.Duration
	// MaxDelay caps the backoff.
	MaxDelay time.Duration
	// DeadLetterDir receives records that exhausted their attempts.
	DeadLetterDir string
	// OnError is called for each failed attempt, for metrics and logging.
	// It must never be given a payload.
	OnError func(attempt int, err error)
	// BatchSize is how many records are handed to the sink before it is
	// asked to commit. Batching matters: a commit per record made delivery
	// roughly thirty times slower than ingest under load, so the buffer
	// grew steadily even though nothing was wrong.
	BatchSize int
	// Commit is called after a batch has been handed to the sink, and must
	// make it durable. Records are acknowledged only once it returns, so a
	// crash between the sink write and the commit redelivers rather than
	// loses (F-8.4).
	Commit func(context.Context) error

	Now  func() time.Time
	Rand *rand.Rand
}

// DeliveryStats are the counters this stage contributes to §11.
type DeliveryStats struct {
	Delivered    int64
	Retries      int64
	DeadLettered int64
	// Stalled counts drains that stopped because a sink was unreachable.
	// It is the signal that data is accumulating rather than being lost,
	// and it is what distinguishes a sink outage from a poisoned record.
	Stalled int64
}

// NewDeliverer creates a delivery loop.
func NewDeliverer(buf *Buffer, send func(context.Context, []byte) error, opts DeliveryOptions) *Deliverer {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 8
	}
	if opts.BaseDelay <= 0 {
		opts.BaseDelay = 500 * time.Millisecond
	}
	if opts.MaxDelay <= 0 {
		opts.MaxDelay = 60 * time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Rand == nil {
		opts.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 256
	}
	return &Deliverer{buf: buf, send: send, opts: opts}
}

// Run drains the buffer until ctx is cancelled.
func (d *Deliverer) Run(ctx context.Context, idle time.Duration) {
	if idle <= 0 {
		idle = 200 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := d.DrainOnce(ctx)
		if err != nil && ctx.Err() != nil {
			return
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(idle):
			}
		}
	}
}

// DrainOnce delivers every record currently in the buffer, returning how many
// were delivered.
//
// Records are handed to the sink in batches and acknowledged only after the
// batch is committed. Acknowledging earlier would risk losing a record the sink
// had accepted but not yet made durable; acknowledging one at a time made
// delivery far slower than ingest, so the backlog grew under normal load.
func (d *Deliverer) DrainOnce(ctx context.Context) (int, error) {
	delivered := 0

	for {
		if ctx.Err() != nil {
			return delivered, ctx.Err()
		}

		batch, err := d.nextBatch()
		if err != nil {
			return delivered, err
		}
		if len(batch) == 0 {
			return delivered, nil
		}

		n, err := d.deliverBatch(ctx, batch)
		delivered += n
		if err != nil {
			if errors.Is(err, errStalled) {
				// Not a failure of the buffer: the destination
				// is unreachable and the records are still
				// queued. Stop draining and try again later.
				return delivered, nil
			}
			return delivered, err
		}
	}
}

// nextBatch peeks up to BatchSize records without acknowledging them.
func (d *Deliverer) nextBatch() ([]*Entry, error) {
	var batch []*Entry

	for len(batch) < d.opts.BatchSize {
		e, err := d.buf.PeekAfter(batch)
		if err != nil {
			return batch, err
		}
		if e == nil {
			break
		}
		batch = append(batch, e)
	}
	return batch, nil
}

// deliverBatch sends a batch, commits it, then acknowledges it.
//
// On any failure the whole batch stays unacknowledged. That can redeliver
// records the sink already took, which at-least-once permits and a reader
// deduplicates on episode_id (F-8.4); the alternative — acknowledging before
// the commit — would lose them outright, which it does not permit.
func (d *Deliverer) deliverBatch(ctx context.Context, batch []*Entry) (int, error) {
	// A dead-lettered record is acknowledged inside sendOne and must not be
	// counted as delivered or acknowledged twice.
	toAck := make([]*Entry, 0, len(batch))

	for _, e := range batch {
		handled, err := d.sendOne(ctx, e)
		if err != nil {
			return 0, err
		}
		if !handled {
			toAck = append(toAck, e)
		}
	}

	if d.opts.Commit != nil && len(toAck) > 0 {
		if err := d.commitWithRetry(ctx); err != nil {
			return 0, err
		}
	}

	for _, e := range toAck {
		if err := d.buf.Ack(e); err != nil {
			return 0, err
		}
	}

	d.mu.Lock()
	d.stats.Delivered += int64(len(toAck))
	d.mu.Unlock()

	return len(batch), nil
}

// sendOne hands one record to the sink, retrying transient failures.
//
// The bool reports whether the record was already dealt with — dead-lettered
// and acknowledged — so the caller neither acknowledges nor counts it again.
func (d *Deliverer) sendOne(ctx context.Context, e *Entry) (bool, error) {
	var lastErr error

	for attempt := 1; attempt <= d.opts.MaxAttempts; attempt++ {
		err := d.send(ctx, e.Payload)
		if err == nil {
			return false, nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if IsPermanent(err) {
			break
		}

		d.mu.Lock()
		d.stats.Retries++
		d.mu.Unlock()
		if d.opts.OnError != nil {
			d.opts.OnError(attempt, err)
		}

		if attempt == d.opts.MaxAttempts {
			d.mu.Lock()
			d.stats.Stalled++
			d.mu.Unlock()
			return false, errStalled
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(d.backoff(attempt)):
		}
	}

	// Permanent: dead-letter and acknowledge, so one poisonous record
	// cannot block every record behind it (F-8.3).
	if err := d.deadLetter(e, lastErr); err != nil {
		return false, err
	}
	d.mu.Lock()
	d.stats.DeadLettered++
	d.mu.Unlock()
	if err := d.buf.Ack(e); err != nil {
		return false, err
	}
	return true, nil
}

func (d *Deliverer) commitWithRetry(ctx context.Context) error {
	var lastErr error

	for attempt := 1; attempt <= d.opts.MaxAttempts; attempt++ {
		err := d.opts.Commit(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.mu.Lock()
		d.stats.Retries++
		d.mu.Unlock()
		if d.opts.OnError != nil {
			d.opts.OnError(attempt, err)
		}

		if attempt == d.opts.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.backoff(attempt)):
		}
	}

	d.mu.Lock()
	d.stats.Stalled++
	d.mu.Unlock()
	_ = lastErr
	return errStalled
}

// errStalled ends a drain because the destination is unreachable. The record is
// left in the buffer for the next cycle.
var errStalled = errors.New("buffer: sink unreachable, delivery stalled")

// deliver sends one record, retrying with exponential backoff and jitter.
//
// What happens when the attempts run out depends on why they failed, and the
// distinction is the difference between a self-healing outage and a pager at
// 3am. See permanentError.
func (d *Deliverer) deliver(ctx context.Context, e *Entry) error {
	var lastErr error

	for attempt := 1; attempt <= d.opts.MaxAttempts; attempt++ {
		err := d.send(ctx, e.Payload)
		if err == nil {
			d.mu.Lock()
			d.stats.Delivered++
			d.mu.Unlock()
			return d.buf.Ack(e)
		}
		lastErr = err

		if ctx.Err() != nil {
			// Shutting down. Do not ack: the record stays in the
			// buffer and is redelivered on the next start, which is
			// the whole point of the buffer surviving restart
			// (F-8.1).
			return ctx.Err()
		}

		// A record the sink will never accept must not consume the
		// retry budget, and must not block everything behind it.
		if IsPermanent(err) {
			break
		}

		d.mu.Lock()
		d.stats.Retries++
		d.mu.Unlock()

		if d.opts.OnError != nil {
			d.opts.OnError(attempt, err)
		}

		if attempt == d.opts.MaxAttempts {
			// Transient failures do not dead-letter. The sink is
			// down; waiting is the correct response, and the
			// buffer's byte and age bounds (F-8.5) are where "down
			// too long" is handled and made observable.
			d.mu.Lock()
			d.stats.Stalled++
			d.mu.Unlock()
			return errStalled
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.backoff(attempt)):
		}
	}

	// Permanent failure: dead-letter so one poisonous record cannot block
	// every record behind it (F-8.3).
	if err := d.deadLetter(e, lastErr); err != nil {
		return err
	}
	d.mu.Lock()
	d.stats.DeadLettered++
	d.mu.Unlock()

	return d.buf.Ack(e)
}

// backoff is exponential with full jitter.
//
// Jitter matters more than the curve: without it, every collector that lost the
// same sink retries in lockstep and hammers it the moment it recovers, turning
// one outage into a second one.
func (d *Deliverer) backoff(attempt int) time.Duration {
	exp := float64(d.opts.BaseDelay) * math.Pow(2, float64(attempt-1))
	if exp > float64(d.opts.MaxDelay) {
		exp = float64(d.opts.MaxDelay)
	}
	return time.Duration(d.opts.Rand.Float64() * exp)
}

// deadLetter writes a record aside with the reason (§8 quarantine prefix).
func (d *Deliverer) deadLetter(e *Entry, cause error) error {
	if d.opts.DeadLetterDir == "" {
		// Nowhere to put it. Dropping silently would be the worst
		// option, so the caller is told and the record stays put.
		return fmt.Errorf("buffer: record exhausted %d attempts and no dead_letter_dir is configured: %w",
			d.opts.MaxAttempts, cause)
	}

	dir := filepath.Join(d.opts.DeadLetterDir,
		"dt="+d.opts.Now().UTC().Format("2006-01-02"), "delivery_failed")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	// The envelope names the cause and the timing; the payload is the
	// already-redacted record, so the dead-letter file is no more sensitive
	// than the lake it was headed for.
	envelope := map[string]any{
		"reason":        cause.Error(),
		"attempts":      d.opts.MaxAttempts,
		"buffered_at":   e.WrittenAt.UTC().Format(time.RFC3339Nano),
		"dead_lettered": d.opts.Now().UTC().Format(time.RFC3339Nano),
		"payload":       json.RawMessage(e.Payload),
	}
	body, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		// The payload was not valid JSON, which should be impossible,
		// but losing the record over a formatting problem would be
		// worse than writing it raw.
		body = e.Payload
	}

	name := fmt.Sprintf("%d-%d.json", d.opts.Now().UnixNano(), d.stats.DeadLettered)
	return os.WriteFile(filepath.Join(dir, name), body, 0o600)
}

// Stats returns a snapshot.
func (d *Deliverer) Stats() DeliveryStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}
