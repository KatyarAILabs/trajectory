// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

// Command loadtest measures the §13 non-functional targets against a running
// collector, so they are published numbers rather than assertions.
//
// The spec asks for throughput "measured by a load harness in CI, published per
// release", and for memory to be shown bounded by a soak rather than claimed.
// This is that harness. It deliberately reports what it measured and what the
// target was, and exits non-zero on a miss, so a regression fails a build
// instead of being noticed a release later.
//
// Run:
//
//	cc run -config examples/local.yaml &
//	go run ./tools/loadtest -endpoint http://127.0.0.1:4318/v1/traces -duration 60s
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/KatyarAILabs/trajectory/collector/acceptance"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:4318/v1/traces", "OTLP/HTTP traces endpoint")
	duration := flag.Duration("duration", 30*time.Second, "how long to sustain load")
	workers := flag.Int("workers", 8, "concurrent senders")
	targetSpans := flag.Float64("target-spans", 5000, "spans/s target from §13")
	metricsURL := flag.String("metrics", "http://127.0.0.1:9464/metrics", "collector metrics endpoint, for RSS")
	flag.Parse()

	req := ptraceotlp.NewExportRequestFromTraces(acceptance.OpenInferenceFixture())
	body, err := req.MarshalProto()
	if err != nil {
		fail(err)
	}
	const spansPerRequest = 4

	var (
		sent     atomic.Int64
		failed   atomic.Int64
		rejected atomic.Int64
	)

	latencies := make([][]time.Duration, *workers)
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	fmt.Printf("load: %d workers against %s for %s\n", *workers, *endpoint, *duration)
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()

			client := &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 4,
				},
			}
			var local []time.Duration

			for ctx.Err() == nil {
				t0 := time.Now()
				resp, err := client.Post(*endpoint, "application/x-protobuf", bytes.NewReader(body))
				if err != nil {
					failed.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				switch {
				case resp.StatusCode == http.StatusOK:
					sent.Add(1)
					local = append(local, time.Since(t0))
				case resp.StatusCode == http.StatusServiceUnavailable:
					// Backpressure is correct behaviour under
					// load, not a failure. Counting it
					// separately is what distinguishes "the
					// collector shed load" from "the
					// collector broke".
					rejected.Add(1)
					time.Sleep(50 * time.Millisecond)
				default:
					failed.Add(1)
				}
			}
			latencies[w] = local
		}(w)
	}
	wg.Wait()

	elapsed := time.Since(start)
	ok := sent.Load()
	spans := float64(ok*spansPerRequest) / elapsed.Seconds()

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	fmt.Printf("\nresults over %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  requests accepted   %d\n", ok)
	fmt.Printf("  shed (backpressure) %d\n", rejected.Load())
	fmt.Printf("  failed              %d\n", failed.Load())
	fmt.Printf("  spans/s             %.0f   (target %.0f)\n", spans, *targetSpans)
	if len(all) > 0 {
		fmt.Printf("  latency p50         %s\n", all[len(all)*50/100].Round(time.Microsecond))
		fmt.Printf("  latency p95         %s\n", all[len(all)*95/100].Round(time.Microsecond))
		fmt.Printf("  latency p99         %s\n", all[min(len(all)*99/100, len(all)-1)].Round(time.Microsecond))
	}

	fmt.Printf("\nharness process\n")
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("  harness heap        %.1f MiB (the collector's own RSS is separate)\n",
		float64(m.HeapAlloc)/(1<<20))

	if rss, err := collectorRSS(*metricsURL); err == nil {
		fmt.Printf("  collector RSS       %.1f MiB (target < 100 MiB idle, bounded under load)\n", rss)
	}

	exit := 0
	if failed.Load() > 0 {
		fmt.Fprintf(os.Stderr, "\nFAIL: %d requests failed outright\n", failed.Load())
		exit = 1
	}
	if spans < *targetSpans {
		fmt.Fprintf(os.Stderr,
			"\nFAIL: %.0f spans/s is below the §13 target of %.0f\n", spans, *targetSpans)
		exit = 1
	}
	if exit == 0 {
		fmt.Printf("\nPASS: met the §13 throughput target\n")
	}
	os.Exit(exit)
}

// collectorRSS scrapes process_resident_memory_bytes, which the Prometheus Go
// collector exports, so the number is the collector's own and not the harness's.
func collectorRSS(url string) (float64, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	for _, line := range bytes.Split(body, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("process_resident_memory_bytes ")) {
			var v float64
			if _, err := fmt.Sscanf(string(line), "process_resident_memory_bytes %g", &v); err == nil {
				return v / (1 << 20), nil
			}
		}
	}
	return 0, fmt.Errorf("process_resident_memory_bytes not found")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "loadtest:", err)
	os.Exit(1)
}
