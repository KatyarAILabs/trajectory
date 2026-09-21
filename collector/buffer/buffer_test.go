// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func newClock() *clock               { return &clock{t: time.Unix(1_757_000_000, 0).UTC()} }

func open(t *testing.T, dir string, tweak ...func(*Options)) *Buffer {
	t.Helper()
	o := Options{Dir: dir, MaxBytes: 1 << 20, MaxAge: time.Hour, SegmentBytes: 4096}
	for _, f := range tweak {
		f(&o)
	}
	if o.Now == nil {
		o.Now = newClock().now
	}
	b, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return b
}

func TestAppendAndRead(t *testing.T) {
	b := open(t, t.TempDir())
	defer b.Close()

	want := []string{"one", "two", "three"}
	for _, s := range want {
		if err := b.Append([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	for _, w := range want {
		e, err := b.Next()
		if err != nil {
			t.Fatal(err)
		}
		if e == nil {
			t.Fatalf("buffer empty, wanted %q", w)
		}
		if string(e.Payload) != w {
			t.Errorf("got %q, want %q", e.Payload, w)
		}
		if err := b.Ack(e); err != nil {
			t.Fatal(err)
		}
	}

	e, err := b.Next()
	if err != nil {
		t.Fatal(err)
	}
	if e != nil {
		t.Errorf("expected empty buffer, got %q", e.Payload)
	}
}

// G-4 / F-8.1: no acknowledged data is lost across a restart. This is the
// reason the package exists.
func TestSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	b := open(t, dir)
	for i := 0; i < 20; i++ {
		if err := b.Append([]byte(fmt.Sprintf("record-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Consume the first five, as a running collector would.
	for i := 0; i < 5; i++ {
		e, _ := b.Next()
		if e == nil {
			t.Fatalf("buffer emptied early at %d", i)
		}
		if err := b.Ack(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart.
	b2 := open(t, dir)
	defer b2.Close()

	for i := 5; i < 20; i++ {
		e, err := b2.Next()
		if err != nil {
			t.Fatal(err)
		}
		if e == nil {
			t.Fatalf("record %d was lost across restart", i)
		}
		if got, want := string(e.Payload), fmt.Sprintf("record-%02d", i); got != want {
			t.Fatalf("after restart got %q, want %q", got, want)
		}
		if err := b2.Ack(e); err != nil {
			t.Fatal(err)
		}
	}
}

// A process killed mid-append leaves a partial record. It must be truncated,
// not handed to a sink as corrupt data, and the records before it must survive.
func TestTornWriteIsTruncatedNotFatal(t *testing.T) {
	dir := t.TempDir()

	b := open(t, dir)
	for i := 0; i < 5; i++ {
		if err := b.Append([]byte(fmt.Sprintf("good-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	b.Close()

	// Simulate the kill: append a partial header to the active segment.
	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) == 0 {
		t.Fatal("no segment written")
	}
	f, err := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x54, 0x52, 0x4a, 0x31, 0x00, 0x00, 0x10}) // truncated header
	f.Close()

	b2 := open(t, dir)
	defer b2.Close()

	for i := 0; i < 5; i++ {
		e, err := b2.Next()
		if err != nil {
			t.Fatalf("recovery failed on record %d: %v", i, err)
		}
		if e == nil {
			t.Fatalf("record %d lost to a torn write that followed it", i)
		}
		if got, want := string(e.Payload), fmt.Sprintf("good-%d", i); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		b2.Ack(e)
	}

	if e, _ := b2.Next(); e != nil {
		t.Errorf("the torn record was returned as if valid: %q", e.Payload)
	}
}

// A flipped byte must be caught by the checksum rather than delivered.
func TestCorruptRecordIsDetected(t *testing.T) {
	dir := t.TempDir()

	b := open(t, dir)
	b.Append([]byte("this payload will be corrupted"))
	b.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	data, _ := os.ReadFile(segs[0])
	data[headerBytes+3] ^= 0xFF // flip a bit inside the payload
	os.WriteFile(segs[0], data, 0o600)

	var evictions []string
	b2 := open(t, dir, func(o *Options) {
		o.OnEvict = func(reason string, _ int, _ int64) { evictions = append(evictions, reason) }
	})
	defer b2.Close()

	e, err := b2.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e != nil {
		t.Errorf("corrupt record was delivered: %q", e.Payload)
	}
	// Corruption must be reported, whether it is caught at recovery (which
	// truncates the tail) or at read time. A silently shortened buffer is
	// the failure mode this guards against.
	if len(evictions) == 0 {
		t.Error("corruption was not reported at all")
	}
}

// F-8.5: the buffer is bounded by bytes, and going over is visible.
func TestBoundedByBytes(t *testing.T) {
	dir := t.TempDir()
	var evicted []string

	b := open(t, dir, func(o *Options) {
		o.MaxBytes = 8192
		o.SegmentBytes = 1024
		o.OnEvict = func(reason string, _ int, _ int64) { evicted = append(evicted, reason) }
	})
	defer b.Close()

	payload := make([]byte, 400)
	for i := 0; i < 200; i++ {
		if err := b.Append(payload); err != nil && err != ErrFull {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if got := b.Bytes(); got > 8192 {
		t.Errorf("buffer grew to %d bytes, bound is 8192", got)
	}
	if len(evicted) == 0 {
		t.Error("nothing was reported as evicted despite exceeding the bound")
	}
}

// F-8.5: bounded by age too.
func TestBoundedByAge(t *testing.T) {
	dir := t.TempDir()
	clk := newClock()
	var evicted []string

	b := open(t, dir, func(o *Options) {
		o.MaxAge = time.Hour
		o.SegmentBytes = 256
		o.Now = clk.now
		o.OnEvict = func(reason string, _ int, _ int64) { evicted = append(evicted, reason) }
	})
	defer b.Close()

	for i := 0; i < 10; i++ {
		b.Append([]byte(strings.Repeat("x", 100)))
	}

	before := b.Bytes()
	clk.add(2 * time.Hour)
	b.EvictExpired()

	if b.Bytes() >= before {
		t.Errorf("nothing expired: %d bytes before, %d after", before, b.Bytes())
	}
	if len(evicted) == 0 || evicted[0] != "max_age" {
		t.Errorf("age eviction not reported: %v", evicted)
	}
}

// F-8.2: backpressure engages before the buffer is full, not at the cliff.
func TestBackpressureThreshold(t *testing.T) {
	b := open(t, t.TempDir(), func(o *Options) {
		o.MaxBytes = 10000
		o.BackpressureAt = 0.5
		o.SegmentBytes = 100000
	})
	defer b.Close()

	if b.UnderBackpressure() {
		t.Error("backpressure on an empty buffer")
	}

	// 10 records of 500 bytes plus framing clears the 5000-byte threshold.
	payload := make([]byte, 500)
	for i := 0; i < 10; i++ {
		b.Append(payload)
	}
	if !b.UnderBackpressure() {
		t.Errorf("no backpressure at %d/%d bytes with threshold 0.5", b.Bytes(), 10000)
	}
}

// Age is observable for the §11 gauge.
func TestOldestAge(t *testing.T) {
	clk := newClock()
	b := open(t, t.TempDir(), func(o *Options) { o.Now = clk.now })
	defer b.Close()

	if b.OldestAge() != 0 {
		t.Error("non-zero age on an empty buffer")
	}

	b.Append([]byte("x"))
	clk.add(90 * time.Second)

	if got := b.OldestAge(); got != 90*time.Second {
		t.Errorf("OldestAge = %v, want 90s", got)
	}
}

// F-8.3: a record that cannot be delivered is dead-lettered rather than
// blocking every record behind it forever.
func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	dir := t.TempDir()
	dlq := filepath.Join(dir, "dlq")

	b := open(t, dir)
	defer b.Close()
	b.Append([]byte(`{"poison":true}`))
	b.Append([]byte(`{"fine":true}`))

	attempts := 0
	send := func(_ context.Context, p []byte) error {
		if strings.Contains(string(p), "poison") {
			attempts++
			// Permanent: this record will never be accepted, as
			// distinct from the sink being down.
			return Permanent(fmt.Errorf("sink rejects this record"))
		}
		return nil
	}

	d := NewDeliverer(b, send, DeliveryOptions{
		MaxAttempts:   3,
		BaseDelay:     time.Microsecond,
		MaxDelay:      time.Microsecond,
		DeadLetterDir: dlq,
		Rand:          rand.New(rand.NewSource(1)),
	})

	n, err := d.DrainOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// A permanent failure does not consume the retry budget: retrying a
	// record the sink will never accept just delays everything behind it.
	if attempts != 1 {
		t.Errorf("tried %d times, want 1 — a permanent failure must not be retried", attempts)
	}
	if n != 2 {
		t.Errorf("delivered %d records, want 2 (the poison one counts as handled)", n)
	}

	// The good record behind the poison one must have got through.
	if st := d.Stats(); st.Delivered != 1 || st.DeadLettered != 1 {
		t.Errorf("stats = %+v, want 1 delivered and 1 dead-lettered", st)
	}

	var files []string
	filepath.Walk(dlq, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if len(files) != 1 {
		t.Fatalf("got %d dead-letter files, want 1", len(files))
	}
	body, _ := os.ReadFile(files[0])
	if !strings.Contains(string(body), "sink rejects this record") {
		t.Error("the dead-letter file does not record why delivery failed")
	}
}

// A record is acked only after the sink accepted it, so a crash mid-delivery
// redelivers rather than losing (F-8.4, at-least-once).
func TestNotAckedUntilDelivered(t *testing.T) {
	dir := t.TempDir()

	b := open(t, dir)
	b.Append([]byte("important"))

	failing := func(context.Context, []byte) error { return fmt.Errorf("sink down") }
	d := NewDeliverer(b, failing, DeliveryOptions{
		MaxAttempts: 2, BaseDelay: time.Microsecond, MaxDelay: time.Microsecond,
		DeadLetterDir: filepath.Join(dir, "dlq"),
		Rand:          rand.New(rand.NewSource(1)),
	})

	// The sink is merely down, so the record must stay queued rather than
	// being dead-lettered or dropped.
	if _, err := d.DrainOnce(context.Background()); err != nil {
		t.Fatalf("a transient outage should stall, not error: %v", err)
	}
	if st := d.Stats(); st.Stalled != 1 || st.DeadLettered != 0 {
		t.Errorf("stats = %+v, want Stalled=1 DeadLettered=0", st)
	}
	b.Close()

	b2 := open(t, dir)
	defer b2.Close()
	e, _ := b2.Next()
	if e == nil {
		t.Fatal("the undeliverable record was lost instead of being retained for retry")
	}
	if string(e.Payload) != "important" {
		t.Errorf("got %q", e.Payload)
	}
}

// Backoff must be jittered, or every collector that lost the same sink retries
// in lockstep and turns one outage into a second one on recovery.
func TestBackoffIsJittered(t *testing.T) {
	d := NewDeliverer(nil, nil, DeliveryOptions{
		BaseDelay: time.Second, MaxDelay: time.Minute,
		Rand: rand.New(rand.NewSource(42)),
	})

	seen := map[time.Duration]bool{}
	for i := 0; i < 20; i++ {
		seen[d.backoff(3)] = true
	}
	if len(seen) < 10 {
		t.Errorf("backoff produced only %d distinct values across 20 calls; it is not jittered", len(seen))
	}

	// Still bounded by the cap.
	for i := 0; i < 100; i++ {
		if got := d.backoff(50); got > time.Minute {
			t.Fatalf("backoff %v exceeds MaxDelay", got)
		}
	}
}

// Delivery must survive a restart mid-drain without duplicating acknowledged
// records or losing unacknowledged ones.
func TestDeliveryResumesAfterRestart(t *testing.T) {
	dir := t.TempDir()

	b := open(t, dir)
	for i := 0; i < 10; i++ {
		b.Append([]byte(fmt.Sprintf("rec-%d", i)))
	}

	var got []string
	failAfter := 4
	send := func(_ context.Context, p []byte) error {
		if len(got) >= failAfter {
			return fmt.Errorf("sink down")
		}
		got = append(got, string(p))
		return nil
	}

	d := NewDeliverer(b, send, DeliveryOptions{
		MaxAttempts: 1, BaseDelay: time.Microsecond, MaxDelay: time.Microsecond,
		Rand: rand.New(rand.NewSource(1)),
	})
	_, _ = d.DrainOnce(context.Background())
	b.Close()

	// Restart with a working sink.
	b2 := open(t, dir)
	defer b2.Close()
	send2 := func(_ context.Context, p []byte) error {
		got = append(got, string(p))
		return nil
	}
	d2 := NewDeliverer(b2, send2, DeliveryOptions{Rand: rand.New(rand.NewSource(1))})
	if _, err := d2.DrainOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Delivery is at-least-once (F-8.4), not exactly-once. A batch that was
	// partly sent and never acknowledged is redelivered on restart, which is
	// correct: the alternative is acknowledging before the sink committed,
	// which loses data. Readers deduplicate on episode_id.
	seen := map[string]bool{}
	for _, g := range got {
		seen[g] = true
	}
	for i := 0; i < 10; i++ {
		want := fmt.Sprintf("rec-%d", i)
		if !seen[want] {
			t.Errorf("%s was lost across the restart", want)
		}
	}
	if len(got) < 10 {
		t.Errorf("delivered %d records, want at least 10: %v", len(got), got)
	}
}

// A sink that is merely down must not cause records to be dead-lettered.
//
// This is the difference between an outage that heals itself and one that
// leaves an operator replaying a dead-letter directory by hand. A bucket
// unreachable for an hour would otherwise empty the whole buffer into the DLQ.
func TestTransientOutageStallsInsteadOfDeadLettering(t *testing.T) {
	dir := t.TempDir()
	dlq := filepath.Join(dir, "dlq")

	b := open(t, dir)
	defer b.Close()
	for i := 0; i < 20; i++ {
		b.Append([]byte(fmt.Sprintf("rec-%d", i)))
	}

	down := true
	send := func(context.Context, []byte) error {
		if down {
			return fmt.Errorf("connection refused")
		}
		return nil
	}

	d := NewDeliverer(b, send, DeliveryOptions{
		MaxAttempts: 3, BaseDelay: time.Microsecond, MaxDelay: time.Microsecond,
		DeadLetterDir: dlq, Rand: rand.New(rand.NewSource(1)),
	})

	// Several drain cycles while the sink is down.
	for i := 0; i < 5; i++ {
		if _, err := d.DrainOnce(context.Background()); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}

	if st := d.Stats(); st.DeadLettered != 0 {
		t.Errorf("%d records dead-lettered during a transient outage; they should have waited",
			st.DeadLettered)
	}
	if _, err := os.Stat(dlq); err == nil {
		t.Error("a dead-letter directory was created during a transient outage")
	}

	// The sink comes back. Everything must still be there.
	down = false
	n, err := d.DrainOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Errorf("delivered %d records after recovery, want 20", n)
	}
}

// A permanent failure must not block the records behind it.
func TestPermanentFailureDoesNotBlockTheQueue(t *testing.T) {
	dir := t.TempDir()

	b := open(t, dir)
	defer b.Close()
	b.Append([]byte("poison"))
	for i := 0; i < 5; i++ {
		b.Append([]byte(fmt.Sprintf("good-%d", i)))
	}

	var delivered []string
	send := func(_ context.Context, p []byte) error {
		if string(p) == "poison" {
			return Permanent(fmt.Errorf("malformed record"))
		}
		delivered = append(delivered, string(p))
		return nil
	}

	d := NewDeliverer(b, send, DeliveryOptions{
		MaxAttempts: 5, BaseDelay: time.Microsecond, MaxDelay: time.Microsecond,
		DeadLetterDir: filepath.Join(dir, "dlq"), Rand: rand.New(rand.NewSource(1)),
	})

	if _, err := d.DrainOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 5 {
		t.Errorf("delivered %d good records, want 5; the poison record blocked the queue", len(delivered))
	}
	if st := d.Stats(); st.DeadLettered != 1 {
		t.Errorf("DeadLettered = %d, want 1", st.DeadLettered)
	}
}

func TestIsPermanent(t *testing.T) {
	plain := fmt.Errorf("connection refused")
	if IsPermanent(plain) {
		t.Error("a plain error was treated as permanent")
	}
	if !IsPermanent(Permanent(plain)) {
		t.Error("a marked error was not treated as permanent")
	}
	// Wrapping must not lose the marker; errors travel through layers.
	if !IsPermanent(fmt.Errorf("while writing: %w", Permanent(plain))) {
		t.Error("the permanent marker was lost through a wrap")
	}
	if IsPermanent(nil) {
		t.Error("nil was treated as permanent")
	}
}

// A second writer on the same directory corrupts the log: two processes
// appending to one segment interleave partial records, and the reader then
// discards whole segments as unreadable.
//
// This was observed in practice during load testing — a collector restarted
// while its predecessor was still draining threw away tens of megabytes of
// already-redacted trajectories with only a warning. Refusing to start turns
// silent data loss into something an operator can see and fix.
func TestSecondWriterIsRefused(t *testing.T) {
	dir := t.TempDir()

	first := open(t, dir)
	defer first.Close()

	second, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: 4096, MaxAge: time.Hour})
	if err == nil {
		second.Close()
		t.Fatal("a second buffer opened the same directory; the log would be corrupted")
	}
	if !strings.Contains(err.Error(), "locked by another process") {
		t.Errorf("error does not explain the problem: %v", err)
	}

	// After the first releases, a new writer must be able to take over —
	// otherwise a restart would need manual intervention.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: 4096, MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("could not reopen after a clean close: %v", err)
	}
	third.Close()
}

// Damage anywhere in the log must cost only the damaged records, not
// everything after them.
func TestCorruptionInAnEarlierSegmentIsIsolated(t *testing.T) {
	dir := t.TempDir()

	b := open(t, dir, func(o *Options) { o.SegmentBytes = 256 })
	for i := 0; i < 40; i++ {
		if err := b.Append([]byte(fmt.Sprintf("record-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	b.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	if len(segs) < 3 {
		t.Fatalf("expected several segments, got %d", len(segs))
	}

	// Damage a segment in the middle, as a second writer or a bad disk
	// would.
	target := segs[len(segs)/2]
	data, _ := os.ReadFile(target)
	if len(data) > headerBytes+4 {
		data[headerBytes+2] ^= 0xFF
		os.WriteFile(target, data, 0o600)
	}

	b2 := open(t, dir, func(o *Options) { o.SegmentBytes = 256 })
	defer b2.Close()

	// Records from the undamaged segments must still be readable.
	got := 0
	for {
		e, err := b2.Next()
		if err != nil {
			t.Fatalf("read after corruption: %v", err)
		}
		if e == nil {
			break
		}
		got++
		if err := b2.Ack(e); err != nil {
			t.Fatal(err)
		}
	}

	// Some loss is expected in the damaged segment; losing everything is
	// not.
	if got == 0 {
		t.Fatal("corruption in one segment destroyed the whole buffer")
	}
	if got < 20 {
		t.Errorf("recovered only %d of 40 records; damage to one segment should not "+
			"discard the others", got)
	}
}

var testBufKey = []byte("0123456789abcdef0123456789abcdef") // 32 bytes

// F-8.6: with a key configured, nothing on disk is readable as plaintext.
func TestEncryptedAtRest(t *testing.T) {
	dir := t.TempDir()
	b := open(t, dir, func(o *Options) { o.EncryptionKey = testBufKey })
	b.Append([]byte(`{"prompt":"a distinctive secret phrase"}`))
	b.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, "*.seg"))
	for _, s := range segs {
		data, _ := os.ReadFile(s)
		if strings.Contains(string(data), "distinctive secret phrase") {
			t.Fatalf("plaintext found in %s despite encryption", filepath.Base(s))
		}
	}

	b2 := open(t, dir, func(o *Options) { o.EncryptionKey = testBufKey })
	defer b2.Close()
	e, err := b2.Next()
	if err != nil || e == nil {
		t.Fatalf("could not read back: %v", err)
	}
	if !strings.Contains(string(e.Payload), "distinctive secret phrase") {
		t.Errorf("decrypted payload wrong: %q", e.Payload)
	}
}

// Changing the key with undelivered data would make every record unreadable,
// and an unreadable record is discarded. Refuse instead.
func TestKeyChangeWithPendingDataRefused(t *testing.T) {
	dir := t.TempDir()
	b := open(t, dir, func(o *Options) { o.EncryptionKey = testBufKey })
	b.Append([]byte("pending"))
	b.Close()

	other := []byte("ffffffffffffffffffffffffffffffff")
	_, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: 4096,
		MaxAge: time.Hour, EncryptionKey: other})
	if err == nil {
		t.Fatal("opened with a different key while undelivered records remain; they would be discarded")
	}
	if !strings.Contains(err.Error(), "drain") {
		t.Errorf("error does not say how to recover: %v", err)
	}

	// Turning encryption off is the same hazard.
	if _, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: 4096, MaxAge: time.Hour}); err == nil {
		t.Fatal("opened without encryption over an encrypted buffer with pending data")
	}
}

// Once drained, the key can change freely.
func TestKeyChangeAfterDrainAllowed(t *testing.T) {
	dir := t.TempDir()
	b := open(t, dir, func(o *Options) { o.EncryptionKey = testBufKey })
	b.Append([]byte("x"))
	e, _ := b.Next()
	b.Ack(e)
	b.Close()

	other := []byte("ffffffffffffffffffffffffffffffff")
	b2, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, SegmentBytes: 4096,
		MaxAge: time.Hour, EncryptionKey: other})
	if err != nil {
		t.Fatalf("drained buffer refused a new key: %v", err)
	}
	b2.Close()
}

func TestHexKeyAccepted(t *testing.T) {
	hexKey := []byte(strings.Repeat("ab", 32))
	if _, err := NewAEAD(hexKey); err != nil {
		t.Errorf("64-char hex key rejected: %v", err)
	}
	if _, err := NewAEAD([]byte("short")); err == nil {
		t.Error("a short key was accepted")
	}
}
