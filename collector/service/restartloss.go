// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// Assembly state is best-effort in v1 (Q-8): episodes still open in memory when
// the process dies are lost. F-3.8 allows that only if the loss is measured, so
// the service writes a small marker recording how many episodes it holds, and
// the next process reads it.
//
// A clean shutdown drains assembly first and marks the file clean, so only an
// unclean stop is counted. The number is an upper bound on what was lost — an
// episode open at the last write may have closed and been buffered before the
// crash — which is the right direction for a loss metric to err in.
type assemblyMarker struct {
	InFlight  int       `json:"in_flight"`
	Clean     bool      `json:"clean"`
	UpdatedAt time.Time `json:"updated_at"`
	PID       int       `json:"pid"`
}

// markerPath uses the buffer directory captured at construction. The buffer is
// structural and never changes on reload, and reading it from s.cfg would race
// with Reload replacing that pointer.
func (s *Service) markerPath() string {
	return filepath.Join(s.bufferDir, "assembly.json")
}

// recordInFlight persists the in-flight count. Called on a timer.
func (s *Service) recordInFlight(clean bool) {
	m := assemblyMarker{
		InFlight:  s.assembler.InFlight(),
		Clean:     clean,
		UpdatedAt: time.Now().UTC(),
		PID:       os.Getpid(),
	}
	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	tmp := s.markerPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, s.markerPath())
}

// reportPreviousLoss reads the previous process's marker at startup.
func (s *Service) reportPreviousLoss() {
	b, err := os.ReadFile(s.markerPath())
	if err != nil {
		return
	}
	var m assemblyMarker
	if err := json.Unmarshal(b, &m); err != nil {
		return
	}
	if m.Clean || m.InFlight == 0 {
		return
	}

	s.metrics.LostOnRestart.Add(float64(m.InFlight))
	s.log.Warn("previous process stopped without draining assembly; "+
		"episodes it held in memory were lost (assembly state is best-effort, F-3.8)",
		"episodes_lost_upper_bound", m.InFlight,
		"last_recorded", m.UpdatedAt.Format(time.RFC3339),
		"previous_pid", m.PID)
}
