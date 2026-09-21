// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// s3Under returns a store against a real S3-compatible endpoint, or skips.
//
// The S3 sink is the production sink, and until this test existed it had only
// ever been compiled. Mocking the SDK would test the mock; this talks to a real
// S3 API (MinIO in CI) so request signing, path-style addressing, HEAD-based
// existence checks and error mapping are all exercised for real.
//
//	TRAJECTORY_S3_ENDPOINT=http://127.0.0.1:19000 \
//	TRAJECTORY_S3_BUCKET=lake \
//	AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... go test ./collector/sink/objstore/
func s3Under(t *testing.T, prefix string) *S3 {
	t.Helper()
	endpoint := os.Getenv("TRAJECTORY_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("TRAJECTORY_S3_ENDPOINT not set; skipping the real-S3 integration test")
	}
	s, err := NewS3(context.Background(), S3Options{
		Bucket:          os.Getenv("TRAJECTORY_S3_BUCKET"),
		Prefix:          prefix,
		Region:          "us-east-1",
		Endpoint:        endpoint,
		UsePathStyle:    true,
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestS3RoundTrip(t *testing.T) {
	s := s3Under(t, fmt.Sprintf("it/%s-%d", t.Name(), time.Now().UnixNano()))
	ctx := context.Background()

	body := []byte("parquet bytes would go here")
	if err := s.Put(ctx, "episodes/dt=2026-09-21/part-1.parquet", body); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := s.Get(ctx, "episodes/dt=2026-09-21/part-1.parquet")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("round trip changed the object")
	}
}

// Blob deduplication depends on Exists telling "absent" apart from "error".
// Getting that wrong either rewrites every blob or, worse, skips writing one.
func TestS3ExistsDistinguishesAbsent(t *testing.T) {
	s := s3Under(t, fmt.Sprintf("it/%s-%d", t.Name(), time.Now().UnixNano()))
	ctx := context.Background()

	ok, err := s.Exists(ctx, "blobs/sha256/aa/bb/never-written")
	if err != nil {
		t.Fatalf("absent key returned an error instead of false: %v", err)
	}
	if ok {
		t.Fatal("absent key reported as present")
	}

	if err := s.Put(ctx, "blobs/sha256/aa/bb/written", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Exists(ctx, "blobs/sha256/aa/bb/written"); err != nil || !ok {
		t.Fatalf("present key: ok=%v err=%v", ok, err)
	}
}

func TestS3GetMissingIsNotFound(t *testing.T) {
	s := s3Under(t, fmt.Sprintf("it/%s-%d", t.Name(), time.Now().UnixNano()))
	if _, err := s.Get(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing object: got %v, want ErrNotFound", err)
	}
}

// Bad credentials must fail loudly, and the error must not echo a secret.
func TestS3BadCredentialsFailWithoutLeaking(t *testing.T) {
	if os.Getenv("TRAJECTORY_S3_ENDPOINT") == "" {
		t.Skip("no S3 endpoint")
	}
	s, err := NewS3(context.Background(), S3Options{
		Bucket: os.Getenv("TRAJECTORY_S3_BUCKET"), Region: "us-east-1",
		Endpoint: os.Getenv("TRAJECTORY_S3_ENDPOINT"), UsePathStyle: true,
		AccessKeyID: "wrong", SecretAccessKey: "definitely-not-the-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Put(context.Background(), "x", []byte("y"))
	if err == nil {
		t.Fatal("a write with bad credentials succeeded")
	}
	if bytes.Contains([]byte(err.Error()), []byte("definitely-not-the-secret")) {
		t.Errorf("error leaked the secret: %v", err)
	}
}
