// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Command gen-traffic posts the recorded OpenInference fixture to a running
// collector.
//
// This is the "generate traffic" step of UC-5: a contributor runs the
// collector against a local filesystem sink, runs this, and inspects the
// result with DuckDB — no cloud account, no credentials, no Kubernetes.
//
// Run: go run ./tools/gen-traffic -endpoint http://127.0.0.1:4318/v1/traces
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/trajectory-project/trajectory/collector/acceptance"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:4318/v1/traces", "OTLP/HTTP traces endpoint")
	count := flag.Int("count", 1, "how many copies of the fixture to send")
	flag.Parse()

	req := ptraceotlp.NewExportRequestFromTraces(acceptance.OpenInferenceFixture())
	body, err := req.MarshalProto()
	if err != nil {
		fail(err)
	}

	client := &http.Client{Timeout: 10 * time.Second}

	for i := 0; i < *count; i++ {
		resp, err := client.Post(*endpoint, "application/x-protobuf", bytes.NewReader(body))
		if err != nil {
			fail(fmt.Errorf("post to %s: %w", *endpoint, err))
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			fail(fmt.Errorf("post to %s: status %d", *endpoint, resp.StatusCode))
		}
	}

	fmt.Printf("sent %d fixture episode(s) (4 spans each) to %s\n", *count, *endpoint)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "gen-traffic:", err)
	os.Exit(1)
}
