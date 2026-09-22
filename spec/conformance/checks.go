// Copyright The Trajectory Authors.
// SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/KatyarAILabs/trajectory/pkg/record"
)

// Check runs every conformance check over a loaded dataset.
func Check(d *Dataset) Report {
	r := Report{Root: d.Root}

	for _, fn := range []func(*Dataset) Result{
		checkFilesReadable,
		checkSchemaVersionStamped,
		checkSchemaVersionConsistent,
		checkEpisodeRequiredFields,
		checkStatusVocabulary,
		checkKindVocabulary,
		checkTrainableVocabulary,
		checkStepsBelongToEpisodes,
		checkStepCountMatches,
		checkPayloadPlacement,
		checkBlobsResolve,
		checkBlobHashesVerify,
		checkParentIndicesValid,
		checkTimestampsSane,
		checkManifestsCoverFiles,
		checkOutcomesJoinable,
		checkRewardsIdentified,
		checkReservedTablesEmpty,
	} {
		r.Results = append(r.Results, fn(d))
	}
	return r
}

func pass(check, ref string) Result { return Result{Check: check, Ref: ref, Passed: true} }
func skip(check, ref, why string) Result {
	return Result{Check: check, Ref: ref, Skipped: true, Detail: why}
}
func fail(check, ref, format string, args ...any) Result {
	return Result{Check: check, Ref: ref, Detail: fmt.Sprintf(format, args...)}
}

// Every file in the layout must be readable. An unreadable file is data a
// consumer silently will not see, which is worse than an absent one.
func checkFilesReadable(d *Dataset) Result {
	const check, ref = "every file is readable", "F-9.4"

	if len(d.Unreadable) == 0 {
		return pass(check, ref)
	}
	detail := strings.Join(d.Unreadable, "; ")
	if len(detail) > 300 {
		detail = detail[:300] + "..."
	}
	return fail(check, ref, "%d unreadable file(s): %s", len(d.Unreadable), detail)
}

// F-10.2: every file carries schema_version in its metadata, so a reader can
// check compatibility without decoding any data.
func checkSchemaVersionStamped(d *Dataset) Result {
	const check, ref = "schema_version in file metadata", "F-10.2"

	if len(d.SchemaVersions) == 0 {
		return skip(check, ref, "no Parquet files found")
	}
	if n := d.SchemaVersions[""]; n > 0 {
		return fail(check, ref, "%d file(s) carry no schema_version in metadata", n)
	}
	return pass(check, ref)
}

// A dataset spanning two major versions cannot be read as one thing.
func checkSchemaVersionConsistent(d *Dataset) Result {
	const check, ref = "schema versions are compatible", "F-10.1, F-10.4"

	versions := sortedKeys(d.SchemaVersions)
	if len(versions) <= 1 {
		return pass(check, ref)
	}

	majors := map[string]bool{}
	for _, v := range versions {
		if v == "" {
			continue
		}
		majors[strings.SplitN(v, ".", 2)[0]] = true
	}
	if len(majors) > 1 {
		return fail(check, ref,
			"files span incompatible major versions %v; a reader cannot read them as one dataset",
			versions)
	}
	// Several minor versions is explicitly allowed: additive-only within a
	// major version is what F-10.4 promises.
	return pass(check, ref)
}

// §7.1: the columns marked "Null: no" must be present on every episode.
func checkEpisodeRequiredFields(d *Dataset) Result {
	const check, ref = "episode required fields present", "§7.1"

	if len(d.Episodes) == 0 {
		return skip(check, ref, "no episodes")
	}
	for i, e := range d.Episodes {
		switch {
		case e.EpisodeID == "":
			return fail(check, ref, "episode %d has no episode_id", i)
		case e.Tenant == "":
			return fail(check, ref, "episode %s has no tenant", e.EpisodeID)
		case e.Source == "":
			return fail(check, ref, "episode %s has no source (F-1.6)", e.EpisodeID)
		case e.Status == "":
			return fail(check, ref, "episode %s has no status", e.EpisodeID)
		case e.SchemaVersion == "":
			return fail(check, ref, "episode %s has no schema_version", e.EpisodeID)
		case e.StartedAt == 0:
			return fail(check, ref, "episode %s has no started_at", e.EpisodeID)
		}
	}
	return pass(check, ref)
}

func checkStatusVocabulary(d *Dataset) Result {
	const check, ref = "status vocabulary", "§7.1"

	valid := map[string]bool{
		record.StatusComplete: true, record.StatusTimedOut: true,
		record.StatusEvicted: true, record.StatusPatched: true,
	}
	for _, e := range d.Episodes {
		if !valid[e.Status] {
			return fail(check, ref,
				"episode %s has status %q; the vocabulary is complete|timed_out|evicted|patched",
				e.EpisodeID, e.Status)
		}
	}
	if len(d.Episodes) == 0 {
		return skip(check, ref, "no episodes")
	}
	return pass(check, ref)
}

func checkKindVocabulary(d *Dataset) Result {
	const check, ref = "step kind vocabulary", "§7.2, F-2.5"

	valid := map[string]bool{
		record.KindLLM: true, record.KindTool: true, record.KindRetrieval: true,
		record.KindHuman: true, record.KindOther: true,
	}
	for _, s := range d.Steps {
		if !valid[s.Kind] {
			return fail(check, ref,
				"step %d of %s has kind %q; unrecognised kinds must normalise to \"other\", not be invented",
				s.StepIdx, s.EpisodeID, s.Kind)
		}
	}
	if len(d.Steps) == 0 {
		return skip(check, ref, "no steps")
	}
	return pass(check, ref)
}

// F-4.4: trainable is three-valued. A producer that cannot say must write
// "unknown", never "false" — a trainer acting on a coerced false would silently
// train on the wrong tokens.
func checkTrainableVocabulary(d *Dataset) Result {
	const check, ref = "trainable is three-valued", "F-4.4"

	valid := map[string]bool{
		record.TrainableTrue: true, record.TrainableFalse: true, record.TrainableUnknown: true,
	}
	for _, s := range d.Steps {
		if !valid[s.Trainable] {
			return fail(check, ref, "step %d of %s has trainable %q", s.StepIdx, s.EpisodeID, s.Trainable)
		}
		for _, ts := range s.TokenSpans {
			if !valid[ts.Trainable] {
				return fail(check, ref, "a token span of %s has trainable %q", s.EpisodeID, ts.Trainable)
			}
		}
	}
	if len(d.Steps) == 0 {
		return skip(check, ref, "no steps")
	}
	return pass(check, ref)
}

func checkStepsBelongToEpisodes(d *Dataset) Result {
	const check, ref = "steps reference a known episode", "§7.2"

	if len(d.Steps) == 0 {
		return skip(check, ref, "no steps")
	}
	known := map[string]bool{}
	for _, e := range d.Episodes {
		known[e.EpisodeID] = true
	}
	for _, s := range d.Steps {
		if s.EpisodeID == "" {
			return fail(check, ref, "a step has no episode_id")
		}
		if !known[s.EpisodeID] {
			return fail(check, ref, "step %d references unknown episode %s", s.StepIdx, s.EpisodeID)
		}
	}
	return pass(check, ref)
}

// step_count must agree with the steps actually present, or a reader cannot
// tell a truncated read from a complete one.
func checkStepCountMatches(d *Dataset) Result {
	const check, ref = "step_count matches the steps table", "§7.1"

	if len(d.Episodes) == 0 {
		return skip(check, ref, "no episodes")
	}

	actual := map[string]int32{}
	for _, s := range d.Steps {
		actual[s.EpisodeID]++
	}
	for _, e := range d.Episodes {
		// A patch record describes only its own steps (F-3.5), so it is
		// counted on its own terms.
		if e.Status == record.StatusPatched {
			continue
		}
		if got := actual[e.EpisodeID]; got != e.StepCount {
			return fail(check, ref,
				"episode %s declares step_count %d but %d steps are present",
				e.EpisodeID, e.StepCount, got)
		}
	}
	return pass(check, ref)
}

// F-9.3: exactly one of content_ref and content_inline carries the payload.
func checkPayloadPlacement(d *Dataset) Result {
	const check, ref = "payload is in exactly one place", "§7.2, F-9.3"

	if len(d.Steps) == 0 {
		return skip(check, ref, "no steps")
	}
	for _, s := range d.Steps {
		if s.ContentRef != nil && s.ContentInline != nil {
			return fail(check, ref,
				"step %d of %s has both content_ref and content_inline",
				s.StepIdx, s.EpisodeID)
		}
	}
	return pass(check, ref)
}

// A content_ref that points at nothing is a dangling payload: the trajectory
// looks complete and is not.
func checkBlobsResolve(d *Dataset) Result {
	const check, ref = "content_ref resolves to a blob", "F-9.3"

	refs := 0
	for _, s := range d.Steps {
		if s.ContentRef == nil {
			continue
		}
		refs++
		h := *s.ContentRef
		if len(h) < 4 {
			return fail(check, ref, "step %d of %s has a malformed content_ref %q",
				s.StepIdx, s.EpisodeID, h)
		}
		path := filepath.Join(d.Root, record.TableBlobs, "sha256", h[0:2], h[2:4], h)
		if _, err := os.Stat(path); err != nil {
			return fail(check, ref,
				"step %d of %s references blob %s, which is not present",
				s.StepIdx, s.EpisodeID, h[:12])
		}
	}
	if refs == 0 {
		return skip(check, ref, "no externalised payloads")
	}
	return pass(check, ref)
}

// A blob is addressed by the sha256 of its content, so that is checkable
// rather than something to take on trust.
func checkBlobHashesVerify(d *Dataset) Result {
	const check, ref = "blob contents match their hash", "§7.3, F-9.3"

	checked := 0
	err := filepath.Walk(filepath.Join(d.Root, record.TableBlobs, "sha256"),
		func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(b)
			if hex.EncodeToString(sum[:]) != filepath.Base(path) {
				return fmt.Errorf("blob %s does not hash to its own name", filepath.Base(path)[:12])
			}
			checked++
			return nil
		})
	if err != nil {
		return fail(check, ref, "%v", err)
	}
	if checked == 0 {
		return skip(check, ref, "no blobs")
	}
	return pass(check, ref)
}

// F-3.4: the tree must be intact. A parent index pointing outside the episode
// would make the retry structure unreadable.
func checkParentIndicesValid(d *Dataset) Result {
	const check, ref = "parent_idx references a real step", "F-3.4"

	byEpisode := map[string]map[int32]bool{}
	for _, s := range d.Steps {
		if byEpisode[s.EpisodeID] == nil {
			byEpisode[s.EpisodeID] = map[int32]bool{}
		}
		byEpisode[s.EpisodeID][s.StepIdx] = true
	}

	checked := 0
	for _, s := range d.Steps {
		if s.ParentIdx == nil {
			continue
		}
		checked++
		if *s.ParentIdx == s.StepIdx {
			return fail(check, ref, "step %d of %s is its own parent", s.StepIdx, s.EpisodeID)
		}
		if !byEpisode[s.EpisodeID][*s.ParentIdx] {
			return fail(check, ref,
				"step %d of %s has parent_idx %d, which is not a step in that episode",
				s.StepIdx, s.EpisodeID, *s.ParentIdx)
		}
	}
	if checked == 0 {
		return skip(check, ref, "no steps have a parent")
	}
	return pass(check, ref)
}

// Timestamps are microseconds since the Unix epoch. A value in the wrong unit
// is a common and silent producer bug: seconds or milliseconds land decades in
// the past, which nothing else catches.
func checkTimestampsSane(d *Dataset) Result {
	const check, ref = "timestamps are microseconds", "§7.1"

	// 2001-09-09 in microseconds. Anything below is almost certainly
	// seconds or milliseconds misread as microseconds.
	const floor = int64(1_000_000_000_000_000)
	// Year 2100, above which a value is probably nanoseconds.
	const ceiling = int64(4_102_444_800_000_000)

	for _, e := range d.Episodes {
		if e.StartedAt < floor || e.StartedAt > ceiling {
			return fail(check, ref,
				"episode %s has started_at %d, which is not plausibly microseconds "+
					"since the epoch; check the producer's time unit",
				e.EpisodeID, e.StartedAt)
		}
		if e.EndedAt != nil && *e.EndedAt < e.StartedAt {
			return fail(check, ref, "episode %s ended before it started", e.EpisodeID)
		}
	}
	if len(d.Episodes) == 0 {
		return skip(check, ref, "no episodes")
	}
	return pass(check, ref)
}

// F-9.4/F-9.6: the manifest is what makes a batch visible, and a reader that
// ignores unmanifested files must still see all the data.
func checkManifestsCoverFiles(d *Dataset) Result {
	const check, ref = "manifests account for the data", "F-9.4, F-9.6"

	if len(d.Manifests) == 0 {
		if len(d.Episodes) == 0 {
			return skip(check, ref, "no data")
		}
		return fail(check, ref,
			"%d episodes are present but no manifest was written; a conforming reader "+
				"ignores unmanifested files and would see nothing", len(d.Episodes))
	}

	var declared int64
	for _, m := range d.Manifests {
		if m.SchemaVersion == "" {
			return fail(check, ref, "manifest %s has no schema_version", m.BatchID)
		}
		declared += m.Counts.Episodes
		for _, f := range m.Files {
			if _, err := os.Stat(filepath.Join(d.Root, f.Path)); err != nil {
				return fail(check, ref,
					"manifest %s lists %s, which does not exist", m.BatchID, f.Path)
			}
		}
	}

	if declared != int64(len(d.Episodes)) {
		return fail(check, ref,
			"manifests declare %d episodes but %d are present; a reader following the "+
				"manifests would see the wrong data", declared, len(d.Episodes))
	}
	return pass(check, ref)
}

// labels is still reserved (§2.3). outcomes and rewards were reopened in v0.2.
func checkReservedTablesEmpty(d *Dataset) Result {
	const check, ref = "labels table is empty", "§2.3, §7.4"

	entries, err := os.ReadDir(filepath.Join(d.Root, record.TableLabels))
	if err != nil {
		return pass(check, ref) // absent is correct
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".parquet") {
			return fail(check, ref, "labels contains data; it is reserved surface and not yet written by any tool")
		}
	}
	return pass(check, ref)
}

// An outcome must carry what the join needs, or it is a row that can never
// match anything.
func checkOutcomesJoinable(d *Dataset) Result {
	const check, ref = "outcomes carry join fields", "§9.4"

	if len(d.Outcomes) == 0 {
		return skip(check, ref, "no outcomes")
	}
	const floor = int64(1_000_000_000_000_000)
	for i, o := range d.Outcomes {
		switch {
		case o.EntityName == nil || *o.EntityName == "":
			return fail(check, ref, "outcome %d has no entity_name; it cannot be told apart from another key type", i)
		case o.EntityKey == "":
			return fail(check, ref, "outcome %d has no entity_key", i)
		case o.Kind == "":
			return fail(check, ref, "outcome %d has no kind", i)
		case o.OccurredAt < floor:
			return fail(check, ref, "outcome %d occurred_at %d is not plausibly microseconds", i, o.OccurredAt)
		case o.ObservedAt < floor:
			return fail(check, ref, "outcome %d observed_at %d is not plausibly microseconds; as-of joins depend on it", i, o.ObservedAt)
		}
	}
	return pass(check, ref)
}

// A reward must say which verifier and version produced it, and must refer to
// an episode that exists.
func checkRewardsIdentified(d *Dataset) Result {
	const check, ref = "rewards identify verifier and episode", "F-13.2"

	if len(d.Rewards) == 0 {
		return skip(check, ref, "no rewards")
	}
	known := map[string]bool{}
	for _, e := range d.Episodes {
		known[e.EpisodeID] = true
	}
	for i, r := range d.Rewards {
		switch {
		case r.VerifierID == "" || r.VerifierVersion == "":
			return fail(check, ref, "reward %d lacks verifier_id or verifier_version", i)
		case !known[r.EpisodeID]:
			return fail(check, ref, "reward %d refers to episode %s, which is not in this lake", i, r.EpisodeID)
		}
	}
	return pass(check, ref)
}
