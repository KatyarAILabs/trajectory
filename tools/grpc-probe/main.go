// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Command grpc-probe sends the acceptance fixture over OTLP/gRPC, so the gRPC
// transport can be exercised from a shell the way the HTTP one can with curl.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/trajectory-project/trajectory/collector/acceptance"
)

func main() {
	conn, err := grpc.NewClient("127.0.0.1:44317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()

	c := ptraceotlp.NewGRPCClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := ptraceotlp.NewExportRequestFromTraces(acceptance.OpenInferenceFixture())
	if _, err := c.Export(ctx, req); err != nil {
		fmt.Fprintln(os.Stderr, "export failed:", err)
		os.Exit(1)
	}
	fmt.Println("gRPC export accepted")
}
