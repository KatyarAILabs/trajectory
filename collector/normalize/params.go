// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package normalize

import (
	"encoding/json"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// invocationParams mirrors the JSON object OpenInference puts in
// llm.invocation_parameters. Every field is a pointer so that an absent key
// and a zero value stay distinguishable all the way into the record: a step
// with temperature 0 and a step whose source never reported temperature are
// different things to a replayer (F-4.2).
type invocationParams struct {
	Temperature *float64 `json:"temperature"`
	TopP        *float64 `json:"top_p"`
	MaxTokens   *int32   `json:"max_tokens"`
	Seed        *int64   `json:"seed"`
	Stop        []string `json:"stop"`
}

// parseInvocationParams returns nil if the attribute is not a JSON object or
// carries none of the parameters we model. Returning nil rather than an empty
// struct keeps has_params in the fidelity flags honest (F-4.6).
func parseInvocationParams(raw string) *record.Params {
	var ip invocationParams
	if err := json.Unmarshal([]byte(raw), &ip); err != nil {
		return nil
	}

	if ip.Temperature == nil && ip.TopP == nil && ip.MaxTokens == nil &&
		ip.Seed == nil && len(ip.Stop) == 0 {
		return nil
	}

	return &record.Params{
		Temperature: ip.Temperature,
		TopP:        ip.TopP,
		MaxTokens:   ip.MaxTokens,
		Seed:        ip.Seed,
		Stop:        ip.Stop,
	}
}
