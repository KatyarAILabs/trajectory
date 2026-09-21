// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package otlp

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/trajectory-project/trajectory/collector/pipeline"
)

// traceService implements the OTLP trace export service over gRPC (F-1.1).
//
// It shares dispatch with the HTTP path: the transport differs, the mapping
// does not. A second copy of the flattening logic would drift, and a fidelity
// bug that reproduced on only one transport is exactly the kind of thing that
// takes days to find.
type traceService struct {
	ptraceotlp.UnimplementedGRPCServer
	r    *Receiver
	next pipeline.Next
}

func (s *traceService) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	n, err := s.r.dispatch(ctx, req.Traces(), s.next)
	if err != nil {
		s.r.rejected.Add(1)
		// Unavailable is retryable in the OTLP spec, which is what we
		// want: the data was not accepted and the producer should send
		// it again rather than treating it as delivered.
		return ptraceotlp.NewExportResponse(), status.Error(codes.Unavailable,
			"collector cannot accept data")
	}

	s.r.accepted.Add(1)
	s.r.spans.Add(int64(n))
	return ptraceotlp.NewExportResponse(), nil
}

// startGRPC serves OTLP/gRPC until ctx is cancelled.
func (r *Receiver) startGRPC(ctx context.Context, next pipeline.Next) error {
	serverOpts := []grpc.ServerOption{
		// Bound the message size for the same reason the HTTP path
		// bounds the body: an oversized request must be rejected, not
		// buffered (F-1.7).
		grpc.MaxRecvMsgSize(int(r.opts.MaxRequestBytes)),
	}
	if r.opts.GRPCTLSConfig != nil {
		serverOpts = append(serverOpts,
			grpc.Creds(credentials.NewTLS(r.opts.GRPCTLSConfig)))
	}

	srv := grpc.NewServer(serverOpts...)
	ptraceotlp.RegisterGRPCServer(srv, &traceService{r: r, next: next})

	ln, err := net.Listen("tcp", r.opts.GRPCListen)
	if err != nil {
		return fmt.Errorf("otlp source %q: grpc listen %s: %w", r.opts.Name, r.opts.GRPCListen, err)
	}

	go func() {
		<-ctx.Done()
		// GracefulStop lets in-flight exports finish, so a shutdown
		// does not turn acknowledged-in-progress data into a loss.
		srv.GracefulStop()
	}()

	r.grpcSrv = srv
	return srv.Serve(ln)
}
