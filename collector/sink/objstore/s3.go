// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Options configure an S3-compatible store.
type S3Options struct {
	Bucket string
	Prefix string
	Region string
	// Endpoint targets an S3-compatible service: MinIO, R2, or GCS's S3
	// interoperability endpoint. Empty means AWS.
	Endpoint string
	// UsePathStyle is required by MinIO and most self-hosted gateways,
	// which do not do virtual-host addressing.
	UsePathStyle bool

	// AccessKeyID and SecretAccessKey come from the environment via config
	// interpolation, never inline (F-11.1). Empty means the default
	// credential chain: instance role, web identity, shared config.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// SSEType is "", "AES256" or "aws:kms". Customer-managed keys are a
	// requirement, not a nicety: a security reviewer rejects a collector
	// that cannot write into a CMK-encrypted bucket (F-12.3).
	SSEType  string
	SSEKeyID string
}

// S3 writes objects to any S3-compatible store (F-9.1).
type S3 struct {
	client *s3.Client
	opts   S3Options
}

// NewS3 builds a client.
func NewS3(ctx context.Context, opts S3Options) (*S3, error) {
	if opts.Bucket == "" {
		return nil, fmt.Errorf("objstore: bucket is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if opts.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opts.Region))
	}
	if opts.AccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				opts.AccessKeyID, opts.SecretAccessKey, opts.SessionToken)))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("objstore: load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
		o.UsePathStyle = opts.UsePathStyle
	})

	return &S3{client: client, opts: opts}, nil
}

// Put writes an object.
//
// A single PutObject is atomic in S3 and in every compatible implementation: a
// reader either sees the complete object or the previous one, never a partial
// body (F-9.4). That is why the sink writes whole files rather than streaming,
// and why no multipart upload appears here — multipart would reintroduce the
// partial-visibility problem the manifest ordering exists to avoid.
func (s *S3) Put(ctx context.Context, key string, body []byte) error {
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(s.key(key)),
		Body:   bytes.NewReader(body),
	}

	switch s.opts.SSEType {
	case "AES256":
		in.ServerSideEncryption = types.ServerSideEncryptionAes256
	case "aws:kms":
		in.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		if s.opts.SSEKeyID != "" {
			in.SSEKMSKeyId = aws.String(s.opts.SSEKeyID)
		}
	}

	if _, err := s.client.PutObject(ctx, in); err != nil {
		return fmt.Errorf("objstore: put %s: %w", key, redactAWSError(err))
	}
	return nil
}

func (s *S3) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(s.key(key)),
	})
	if err == nil {
		return true, nil
	}

	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey") {
		return false, nil
	}
	return false, fmt.Errorf("objstore: head %s: %w", key, redactAWSError(err))
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.opts.Bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("objstore: get %s: %w", key, redactAWSError(err))
	}
	defer out.Body.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(out.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Describe names the destination without revealing a credential (F-12.2).
func (s *S3) Describe() string {
	dest := "s3://" + s.opts.Bucket
	if s.opts.Prefix != "" {
		dest += "/" + strings.Trim(s.opts.Prefix, "/")
	}
	if s.opts.Endpoint != "" {
		dest += " via " + s.opts.Endpoint
	}
	if s.opts.SSEType != "" {
		dest += " (sse " + s.opts.SSEType + ")"
	}
	return dest
}

func (s *S3) Close() error { return nil }

func (s *S3) key(k string) string {
	p := strings.Trim(s.opts.Prefix, "/")
	k = strings.TrimPrefix(k, "/")
	if p == "" {
		return k
	}
	return p + "/" + k
}

// redactAWSError strips anything that could carry a credential out of an error
// before it reaches a log (F-12.2).
//
// Signing failures in particular can echo the request, and a presigned URL in a
// log line is a leaked credential with a long tail.
func redactAWSError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()

	for _, marker := range []string{"X-Amz-Signature", "x-amz-security-token", "Authorization:"} {
		if i := strings.Index(msg, marker); i >= 0 {
			msg = msg[:i] + "[redacted]"
			break
		}
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return fmt.Errorf("%s: %s", apiErr.ErrorCode(), truncateErr(apiErr.ErrorMessage()))
	}
	return fmt.Errorf("%s", truncateErr(msg))
}

func truncateErr(s string) string {
	const max = 300
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
