// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/sink/lake"
	"github.com/trajectory-project/trajectory/pkg/record"
)

// F-9.1 end to end: the production sink, against a real S3 API.
//
// The layout, manifest ordering and blob fan-out are shared with the
// filesystem sink and tested there. What only a real S3 endpoint can show is
// that they survive the trip: keys under the prefix, objects that parse as
// Parquet when downloaded, a manifest listing files that exist, and no seeded
// secret in any object.
func TestS3SinkEndToEnd(t *testing.T) {
	endpoint := os.Getenv("TRAJECTORY_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("TRAJECTORY_S3_ENDPOINT not set; skipping the real-S3 end-to-end test")
	}
	bucket := os.Getenv("TRAJECTORY_S3_BUCKET")
	// Unique per run: a shared bucket keeps objects from earlier runs, and a
	// test that lists a reused prefix would be judging someone else's data.
	prefix := fmt.Sprintf("e2e/%s-%d", strings.ToLower(t.Name()), time.Now().UnixNano())

	t.Setenv("CC_HMAC_KEY", "k")
	cfg := testConfig(filepath.Join(t.TempDir(), "unused"))
	cfg.Sinks = []config.Sink{{
		Name: "lake", Type: "s3", Bucket: bucket, Prefix: prefix,
		Region: "us-east-1", Endpoint: endpoint, PathStyle: true,
		AccessKeyID:        os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		PartitionBy:        []string{"dt", "tenant", "task_type"},
		BlobThresholdBytes: 8192, MaxPayloadBytes: 8 << 20,
		TargetFileBytes: 1, Compression: "zstd",
	}}

	_, stop := runService(t, cfg)
	waitForListener(t, cfg.Sources[0].HTTP.Listen)

	req := ptraceotlp.NewExportRequestFromTraces(OpenInferenceFixture())
	body, _ := req.MarshalProto()
	resp, err := http.Post("http://"+cfg.Sources[0].HTTP.Listen+"/v1/traces",
		"application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	stop()

	client := s3Client(t, endpoint)
	ctx := context.Background()

	keys := map[string]bool{}
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/"),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, o := range page.Contents {
			keys[strings.TrimPrefix(*o.Key, prefix+"/")] = true
		}
	}

	get := func(key string) []byte {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(prefix + "/" + key)})
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		defer out.Body.Close()
		b, _ := io.ReadAll(out.Body)
		return b
	}

	var manifestKey, stepsKey string
	blobs := 0
	for k := range keys {
		switch {
		case strings.HasPrefix(k, "manifests/"):
			manifestKey = k
		case strings.HasPrefix(k, "steps/"):
			stepsKey = k
		case strings.HasPrefix(k, "blobs/sha256/"):
			blobs++
		}
		if bytes.Contains(get(k), []byte("alice@example.com")) {
			t.Errorf("SEEDED SECRET LEAKED into s3://%s/%s/%s", bucket, prefix, k)
		}
	}
	if manifestKey == "" || stepsKey == "" {
		t.Fatalf("expected a manifest and a steps file under the prefix; got %v", keys)
	}
	if blobs == 0 {
		t.Error("the 12 KB prompt was not externalised to a blob in S3")
	}

	var m lake.Manifest
	if err := json.Unmarshal(get(manifestKey), &m); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	for _, f := range m.Files {
		if !keys[f.Path] {
			t.Errorf("manifest lists %s, which is not in the bucket", f.Path)
		}
	}

	stepBytes := get(stepsKey)
	steps, err := parquet.Read[record.Step](bytes.NewReader(stepBytes), int64(len(stepBytes)))
	if err != nil {
		t.Fatalf("a downloaded steps object does not parse as Parquet: %v", err)
	}
	if len(steps) != 4 {
		t.Errorf("got %d steps from S3, want 4", len(steps))
	}
}

func s3Client(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")))
	if err != nil {
		t.Fatal(err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}
