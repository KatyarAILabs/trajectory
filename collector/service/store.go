// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"fmt"

	"github.com/trajectory-project/trajectory/collector/config"
	"github.com/trajectory-project/trajectory/collector/sink/objstore"
)

// newStore builds the object store a sink writes to.
//
// The choice is the only thing that differs between a laptop and production:
// everything downstream — layout, blob fan-out, manifest ordering — is the same
// code either way, which is what makes local testing meaningful.
func newStore(cfg config.Sink) (objstore.Store, error) {
	switch cfg.Type {
	case "fs":
		return objstore.NewFS(cfg.Dir)

	case "s3":
		return objstore.NewS3(context.Background(), objstore.S3Options{
			Bucket:          cfg.Bucket,
			Prefix:          cfg.Prefix,
			Region:          cfg.Region,
			Endpoint:        cfg.Endpoint,
			UsePathStyle:    cfg.PathStyle,
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
			SessionToken:    cfg.SessionToken,
			SSEType:         cfg.SSE.Type,
			SSEKeyID:        cfg.SSE.KeyID,
		})

	default:
		return nil, fmt.Errorf("service: unsupported sink type %q", cfg.Type)
	}
}
