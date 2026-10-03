package backup

import (
	"errors"
	"fmt"
	"io"
	"sort"

	"incbackup/internal/repo"
)

// errDiffInterrupted is the controlled stop of the diff failpoint
// (Fail.DiffStopAfterChecks), simulating a crash mid-generation. The report
// stays "running" with its progress persisted; the next CreateDiff for the
// same pair resumes it.
var errDiffInterrupted = errors.New("diff generation interrupted by failpoint")

// ErrDiffRejected is returned when a snapshot pair cannot be compared, e.g.
// one side is not committed. No report row is created for rejected pairs.
type ErrDiffRejected struct{ Message string }

func (e *ErrDiffRejected) Error() string { return e.Message }

// CreateDiff compares two committed snapshots and returns the persisted
// report. The (base, target) pair is unique: a repeated request returns the
// same report without recomputing; a report left "running" by an
// interruption resumes from its persisted phase. Generation only reads the
// snapshots — their status and restorability are never modified.
func (e *Engine) CreateDiff(baseID, targetID int64) (repo.DiffReport, bool, error) {
	if baseID == targetID {
		return repo.DiffReport{}, false, fmt.Errorf("base and target must be two different snapshots")
	}
	base, err := e.Manifest.GetSnapshot(baseID)
	if err != nil {
		return repo.DiffReport{}, false, err
	}
	target, err := e.Manifest.GetSnapshot(targetID)
	if err != nil {
		return repo.DiffReport{}, false, err
	}
	if base.Status != repo.StatusCommitted || target.Status != repo.StatusCommitted {
		return repo.DiffReport{}, false, &ErrDiffRejected{Message: fmt.Sprintf(
			"diff requires two committed snapshots: snapshot %d is %s, snapshot %d is %s",
			baseID, base.Status, targetID, target.Status)}
	}

	e.diffMu.Lock()
	defer e.diffMu.Unlock()

	rep, created, err := e.Manifest.GetOrCreateDiffReport(baseID, targetID)
	if err != nil {
		return repo.DiffReport{}, false, err
	}
	if rep.Status == repo.DiffDone {
		return rep, created, nil // same request -> same traceable report
	}
	if err := e.generateDiff(rep.ID); err != nil {
		if !errors.Is(err, errDiffInterrupted) {
			_ = e.Manifest.FailDiff(rep.ID, err.Error())
			return repo.DiffReport{}, created, err
		}
	}
	final, err := e.Manifest.GetDiffReport(rep.ID)
	if err != nil {
		return repo.DiffReport{}, created, err
	}
	return final, created, nil
}

// ResumeInterruptedDiffs continues every report whose generation was cut off
// (process killed mid-run). Called at service startup; snapshot rows are
// only read, never modified.
func (e *Engine) ResumeInterruptedDiffs() ([]repo.DiffReport, error) {
	reps, err := e.Manifest.InterruptedDiffReports()
	if err != nil {
		return nil, err
	}
	var out []repo.DiffReport
	for _, r := range reps {
		final, _, err := e.CreateDiff(r.BaseID, r.TargetID)
		if err != nil {
			continue // failed report stays inspectable; resume the others
		}
		out = append(out, final)
	}
	return out, nil
}

// generateDiff runs the persisted phase machine to completion (or to the
// failpoint stop). Each phase is idempotent, so re-entering after a crash is
// safe.
func (e *Engine) generateDiff(id int64) error {
	if err := e.Manifest.MarkDiffRunning(id); err != nil {
		return err
	}
	for {
		rep, err := e.Manifest.GetDiffReport(id)
		if err != nil {
			return err
		}
		switch rep.Phase {
		case repo.DiffPhaseClassify:
			if err := e.classifyDiff(rep); err != nil {
				return err
			}
		case repo.DiffPhaseVerify:
			if err := e.verifyDiffChunks(rep); err != nil {
				return err
			}
		case repo.DiffPhaseFinalize:
			return e.finalizeDiff(rep)
		case repo.DiffPhaseDone:
			return nil
		default:
			return fmt.Errorf("unknown diff phase %q", rep.Phase)
		}
	}
}

// classifyDiff compares the two manifests and stores the classified items,
// counts and storage-level chunk statistics in one transaction.
func (e *Engine) classifyDiff(rep repo.DiffReport) error {
	baseEntries, err := e.Manifest.EntriesOf(rep.BaseID)
	if err != nil {
		return err
	}
	targetEntries, err := e.Manifest.EntriesOf(rep.TargetID)
	if err != nil {
		return err
	}
	baseSet := chunkSetOf(baseEntries)
	targetSet := chunkSetOf(targetEntries)

	items, counts := classifyEntries(baseEntries, targetEntries, baseSet)

	// Storage-level reuse: distinct chunks the target references, split by
	// whether the base already referenced them.
	var reused, newChunks int64
	union := map[string]bool{}
	for d := range baseSet {
		union[d] = true
	}
	for d := range targetSet {
		union[d] = true
		if baseSet[d] {
			reused++
		} else {
			newChunks++
		}
	}
	return e.Manifest.ReplaceDiffItems(rep.ID, items, counts, reused, newChunks, int64(len(union)))
}

// verifyDiffChunks checks every distinct chunk referenced by either
// snapshot: catalog row present, blob present with the declared length, and
// blob content hashing back to the chunk digest. Verdicts are persisted per
// chunk, so an interruption resumes after the last recorded check.
func (e *Engine) verifyDiffChunks(rep repo.DiffReport) error {
	done, err := e.Manifest.DiffChunkCheckDigests(rep.ID)
	if err != nil {
		return err
	}
	chunks, err := e.Manifest.DiffChunksToVerify(rep.BaseID, rep.TargetID)
	if err != nil {
		return err
	}
	checked := 0
	for _, c := range chunks {
		if done[string(c.Digest)] {
			continue
		}
		ok, reason := e.checkDiffChunk(c.Digest, c.Length)
		if err := e.Manifest.SaveDiffChunkCheck(rep.ID, c.Digest, c.Length, ok, reason); err != nil {
			return err
		}
		checked++
		if e.Fail.DiffStopAfterChecks > 0 && checked >= e.Fail.DiffStopAfterChecks {
			return errDiffInterrupted
		}
	}
	return e.Manifest.SetDiffPhase(rep.ID, repo.DiffPhaseFinalize)
}

// checkDiffChunk verifies one chunk against the live content store. Any
// problem is reported as a failed check (incomplete report), never as a
// generation error.
func (e *Engine) checkDiffChunk(digest []byte, length int64) (bool, string) {
	if length < 0 {
		return false, "chunk missing from catalog (commit interrupted)"
	}
	ok, err := e.Store.Has(digest, length)
	if err != nil {
		return false, "stat blob: " + err.Error()
	}
	if !ok {
		return false, "chunk blob missing or length mismatch in content store"
	}
	rc, err := e.Store.Open(digest) // streams with digest verification
	if err != nil {
		return false, "open blob: " + err.Error()
	}
	_, copyErr := io.Copy(io.Discard, rc)
	closeErr := rc.Close()
	if copyErr != nil {
		return false, "blob digest mismatch: " + copyErr.Error()
	}
	if closeErr != nil {
		return false, "close blob: " + closeErr.Error()
	}
	return true, ""
}

// finalizeDiff derives the missing-path list and stamps the integrity
// verdict. A report with any failed chunk check is incomplete.
func (e *Engine) finalizeDiff(rep repo.DiffReport) error {
	failed, err := e.Manifest.CountFailedDiffChecks(rep.ID)
	if err != nil {
		return err
	}
	integrity := repo.IntegrityComplete
	if failed > 0 {
		integrity = repo.IntegrityIncomplete
	}
	return e.Manifest.FinalizeDiff(rep.ID, rep.BaseID, rep.TargetID, integrity)
}

// chunkSetOf returns the distinct chunk digests referenced by entries.
func chunkSetOf(entries []repo.StoredEntry) map[string]bool {
	set := map[string]bool{}
	for _, e := range entries {
		for _, d := range e.ChunkDigests {
			set[string(d)] = true
		}
	}
	return set
}

// classifyEntries compares two entry lists path by path and classifies every
// difference. Rename detection uses whole-file digests and only pairs a
// deleted and an added file when the digest is unique on both sides; any
// digest with multiple candidates is reported as ambiguous on every side,
// never as an invented pairing.
func classifyEntries(base, target []repo.StoredEntry, baseSet map[string]bool) ([]repo.DiffItem, repo.DiffCounts) {
	var items []repo.DiffItem
	var counts repo.DiffCounts

	targetByPath := map[string]repo.StoredEntry{}
	for _, t := range target {
		targetByPath[t.RelPath] = t
	}
	baseByPath := map[string]repo.StoredEntry{}
	for _, b := range base {
		baseByPath[b.RelPath] = b
	}

	// Paths present on both sides.
	for _, b := range base {
		t, ok := targetByPath[b.RelPath]
		if !ok {
			continue
		}
		if b.Kind != t.Kind {
			// A type change is a delete plus a create, not a modification.
			items = append(items, deletedItem(b), addedItem(t, baseSet))
			counts.Deleted++
			counts.Added++
			continue
		}
		switch b.Kind {
		case repo.KindFile:
			if !equalBytes(b.FileDigest, t.FileDigest) {
				items = append(items, contentChangedItem(b, t, baseSet))
				counts.ContentChanged++
			} else if fields := metaDiff(b, t); len(fields) > 0 {
				items = append(items, metaChangedItem(b, t, fields))
				counts.MetaChanged++
			} else {
				counts.Unchanged++
			}
		case repo.KindDir:
			if fields := metaDiff(b, t); len(fields) > 0 {
				items = append(items, metaChangedItem(b, t, fields))
				counts.MetaChanged++
			} else {
				counts.Unchanged++
			}
		case repo.KindSymlink:
			// The link target is the symlink's payload: a target change is a
			// content change; mode/mtime alone are metadata changes. The
			// stored target strings are compared, links are never followed.
			if b.LinkTarget != t.LinkTarget {
				items = append(items, symlinkChangedItem(b, t))
				counts.ContentChanged++
			} else if fields := metaDiff(b, t); len(fields) > 0 {
				items = append(items, metaChangedItem(b, t, fields))
				counts.MetaChanged++
			} else {
				counts.Unchanged++
			}
		}
	}

	// One-sided paths. Only regular files carry digests, so only they take
	// part in rename detection; dirs and symlinks are plain added/deleted.
	var deletedFiles, addedFiles []repo.StoredEntry
	for _, b := range base {
		if _, ok := targetByPath[b.RelPath]; ok {
			continue
		}
		if b.Kind == repo.KindFile && len(b.FileDigest) > 0 {
			deletedFiles = append(deletedFiles, b)
		} else {
			items = append(items, deletedItem(b))
			counts.Deleted++
		}
	}
	for _, t := range target {
		if _, ok := baseByPath[t.RelPath]; ok {
			continue
		}
		if t.Kind == repo.KindFile && len(t.FileDigest) > 0 {
			addedFiles = append(addedFiles, t)
		} else {
			items = append(items, addedItem(t, baseSet))
			counts.Added++
		}
	}

	delByDigest := groupByDigest(deletedFiles)
	addByDigest := groupByDigest(addedFiles)
	pairedDel := map[string]bool{}
	pairedAdd := map[string]bool{}
	for digest, dels := range delByDigest {
		adds, ok := addByDigest[digest]
		if !ok {
			continue
		}
		if len(dels) == 1 && len(adds) == 1 {
			// Exactly one candidate on each side: a real rename.
			items = append(items, renameItem(dels[0], adds[0]))
			counts.Renamed++
		} else {
			// Multiple same-content candidates: report ambiguity on every
			// involved path instead of inventing a migration.
			addPaths := pathsOf(adds)
			delPaths := pathsOf(dels)
			for _, d := range dels {
				items = append(items, ambiguousItem(d, true, addPaths))
				counts.Ambiguous++
			}
			for _, a := range adds {
				items = append(items, ambiguousItem(a, false, delPaths))
				counts.Ambiguous++
			}
		}
		for _, d := range dels {
			pairedDel[d.RelPath] = true
		}
		for _, a := range adds {
			pairedAdd[a.RelPath] = true
		}
	}
	for _, d := range deletedFiles {
		if !pairedDel[d.RelPath] {
			items = append(items, deletedItem(d))
			counts.Deleted++
		}
	}
	for _, a := range addedFiles {
		if !pairedAdd[a.RelPath] {
			items = append(items, addedItem(a, baseSet))
			counts.Added++
		}
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].RelPath != items[j].RelPath {
			return items[i].RelPath < items[j].RelPath
		}
		if items[i].OldRelPath != items[j].OldRelPath {
			return items[i].OldRelPath < items[j].OldRelPath
		}
		return items[i].ChangeType < items[j].ChangeType
	})
	return items, counts
}

// metaDiff lists the metadata fields that differ between two stored entries.
func metaDiff(b, t repo.StoredEntry) []string {
	var fields []string
	if b.Mode != t.Mode {
		fields = append(fields, "mode")
	}
	if b.UID != t.UID {
		fields = append(fields, "uid")
	}
	if b.GID != t.GID {
		fields = append(fields, "gid")
	}
	if !b.ModTime.Equal(t.ModTime) {
		fields = append(fields, "mtime")
	}
	return fields
}

func groupByDigest(entries []repo.StoredEntry) map[string][]repo.StoredEntry {
	out := map[string][]repo.StoredEntry{}
	for _, e := range entries {
		key := string(e.FileDigest)
		out[key] = append(out[key], e)
	}
	return out
}

func pathsOf(entries []repo.StoredEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.RelPath)
	}
	sort.Strings(out)
	return out
}

// chunkStats splits the new side's chunk sequence into chunks the base
// snapshot already referenced (reused) and chunks it did not (new).
func chunkStats(t repo.StoredEntry, baseSet map[string]bool) (reused, newChunks int64) {
	for _, d := range t.ChunkDigests {
		if baseSet[string(d)] {
			reused++
		} else {
			newChunks++
		}
	}
	return reused, newChunks
}

func newDiffItem(changeType, kind, rel string) repo.DiffItem {
	return repo.DiffItem{
		ChangeType: changeType,
		Kind:       kind,
		RelPath:    rel,
		OldSize:    -1,
		NewSize:    -1,
		OldMode:    -1,
		NewMode:    -1,
	}
}

func fillOldSide(it *repo.DiffItem, e repo.StoredEntry) {
	it.OldSize = e.Size
	it.OldMode = int64(e.Mode)
	it.OldMtimeNS = e.ModTime.UnixNano()
	it.OldDigest = e.FileDigest
	it.OldLinkTarget = e.LinkTarget
}

func fillNewSide(it *repo.DiffItem, e repo.StoredEntry) {
	it.NewSize = e.Size
	it.NewMode = int64(e.Mode)
	it.NewMtimeNS = e.ModTime.UnixNano()
	it.NewDigest = e.FileDigest
	it.NewLinkTarget = e.LinkTarget
}

func deletedItem(b repo.StoredEntry) repo.DiffItem {
	it := newDiffItem(repo.ChangeDeleted, b.Kind, b.RelPath)
	fillOldSide(&it, b)
	return it
}

func addedItem(t repo.StoredEntry, baseSet map[string]bool) repo.DiffItem {
	it := newDiffItem(repo.ChangeAdded, t.Kind, t.RelPath)
	fillNewSide(&it, t)
	it.ChunksReused, it.ChunksNew = chunkStats(t, baseSet)
	return it
}

func contentChangedItem(b, t repo.StoredEntry, baseSet map[string]bool) repo.DiffItem {
	it := newDiffItem(repo.ChangeContentChanged, t.Kind, t.RelPath)
	fillOldSide(&it, b)
	fillNewSide(&it, t)
	it.ChangedFields = metaDiff(b, t)
	it.ChunksReused, it.ChunksNew = chunkStats(t, baseSet)
	return it
}

func metaChangedItem(b, t repo.StoredEntry, fields []string) repo.DiffItem {
	it := newDiffItem(repo.ChangeMetaChanged, t.Kind, t.RelPath)
	fillOldSide(&it, b)
	fillNewSide(&it, t)
	it.ChangedFields = fields
	return it
}

func symlinkChangedItem(b, t repo.StoredEntry) repo.DiffItem {
	it := newDiffItem(repo.ChangeContentChanged, t.Kind, t.RelPath)
	fillOldSide(&it, b)
	fillNewSide(&it, t)
	it.ChangedFields = metaDiff(b, t)
	return it
}

func renameItem(b, t repo.StoredEntry) repo.DiffItem {
	it := newDiffItem(repo.ChangeRenamed, t.Kind, t.RelPath)
	it.OldRelPath = b.RelPath
	fillOldSide(&it, b)
	fillNewSide(&it, t)
	it.ChangedFields = metaDiff(b, t)
	it.ChunksReused = int64(len(t.ChunkDigests)) // identical content: all reused
	return it
}

// ambiguousItem marks one side of an unresolvable same-digest group.
// deletedSide selects which side of the item is filled; candidates are the
// possible counterpart paths on the other side.
func ambiguousItem(e repo.StoredEntry, deletedSide bool, candidates []string) repo.DiffItem {
	it := newDiffItem(repo.ChangeAmbiguous, e.Kind, e.RelPath)
	if deletedSide {
		fillOldSide(&it, e)
	} else {
		fillNewSide(&it, e)
	}
	it.Candidates = candidates
	return it
}
