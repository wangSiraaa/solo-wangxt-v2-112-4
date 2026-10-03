package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"incbackup/internal/repo"
)

// DiffFailpoints are test hooks for the comparison pipeline.
type DiffFailpoints struct {
	// CrashAfterChunks, when > 0, makes the worker stop (as if the process had
	// been killed) right after persisting that many new chunk verdicts. The
	// job stays "running" and resumes on the next Ensure/Recover call without
	// re-verifying those blobs.
	CrashAfterChunks int
}

// DiffService computes and persists snapshot comparisons. It never mutates the
// two input snapshots; all its state lives in the diff_* tables.
type DiffService struct {
	engine *Engine

	mu     sync.Mutex
	active map[int64]struct{} // jobs currently running in this process

	Fail DiffFailpoints
}

// NewDiffService wires a diff service to an engine.
func NewDiffService(e *Engine) *DiffService {
	return &DiffService{engine: e, active: map[int64]struct{}{}}
}

// ErrDiffNotCommitted rejects comparisons against non-committed snapshots.
type ErrDiffNotCommitted struct {
	SnapshotID int64
	Status     string
}

func (e *ErrDiffNotCommitted) Error() string {
	return fmt.Sprintf("snapshot %d is %s, only committed snapshots can be compared",
		e.SnapshotID, e.Status)
}

// EnsureDiff returns the persisted job for a snapshot pair, creating one if
// needed. The same (base, target) pair always returns the same traceable
// report. Processing runs asynchronously; use GetDiff/Details for progress.
func (d *DiffService) EnsureDiff(baseID, targetID int64) (repo.DiffJob, error) {
	if baseID == targetID {
		return repo.DiffJob{}, errors.New("base and target snapshot must differ")
	}
	for _, id := range []int64{baseID, targetID} {
		si, err := d.engine.Manifest.GetSnapshot(id)
		if errors.Is(err, repo.ErrNotFound) {
			return repo.DiffJob{}, fmt.Errorf("%w: snapshot %d", repo.ErrNotFound, id)
		}
		if err != nil {
			return repo.DiffJob{}, err
		}
		if si.Status != repo.StatusCommitted {
			return repo.DiffJob{}, &ErrDiffNotCommitted{SnapshotID: id, Status: si.Status}
		}
	}

	if existing, ok, err := d.engine.Manifest.FindDiffJob(baseID, targetID); err != nil {
		return repo.DiffJob{}, err
	} else if ok {
		if existing.Status == repo.DiffQueued || existing.Status == repo.DiffRunning {
			d.kick(existing.ID)
		}
		return existing, nil
	}

	id, err := d.engine.Manifest.CreateDiffJob(baseID, targetID)
	if err != nil {
		// A concurrent identical request lost the unique-constraint race:
		// return the existing report instead of failing.
		if existing, ok2, ferr := d.engine.Manifest.FindDiffJob(baseID, targetID); ferr == nil && ok2 {
			d.kick(existing.ID)
			return existing, nil
		}
		return repo.DiffJob{}, err
	}
	job, err := d.engine.Manifest.GetDiffJob(id)
	if err != nil {
		return repo.DiffJob{}, err
	}
	d.kick(id)
	return job, nil
}

// GetDiff returns the persisted job state.
func (d *DiffService) GetDiff(id int64) (repo.DiffJob, error) {
	return d.engine.Manifest.GetDiffJob(id)
}

// kick starts the worker goroutine unless this process is already running it.
func (d *DiffService) kick(id int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, busy := d.active[id]; busy {
		return
	}
	d.active[id] = struct{}{}
	go func() {
		defer func() {
			d.mu.Lock()
			delete(d.active, id)
			d.mu.Unlock()
		}()
		if err := d.runJob(id); err != nil {
			if errors.Is(err, errDiffCrashed) {
				// Simulated kill: leave the job running so recovery resumes it.
				return
			}
			_ = d.engine.Manifest.MarkDiffFailed(id, err.Error())
		}
	}()
}

// RecoverInterrupted resumes every queued/running job (e.g. on startup).
func (d *DiffService) RecoverInterrupted() ([]int64, error) {
	jobs, err := d.engine.Manifest.RunningDiffJobs()
	if err != nil {
		return nil, err
	}
	var ids []int64
	for _, j := range jobs {
		ids = append(ids, j.ID)
		d.kick(j.ID)
	}
	return ids, nil
}

// errDiffCrashed is the synthetic error of the crash failpoint. It is not
// persisted as failed: the worker exits with status still running.
var errDiffCrashed = errors.New("diff worker interrupted (failpoint)")

type blobVerdict struct {
	state  string // ok | missing | digest_mismatch
	actual string // live hex digest when mismatched
	length int64  // catalog-declared length (-1 if no catalog row)
}

// runJob performs the comparison in resumable phases:
//  1. verify every distinct blob referenced by either snapshot (cached),
//  2. attribute bad references to the files that use them,
//  3. classify every entry, then persist the whole report transactionally.
func (d *DiffService) runJob(jobID int64) error {
	if err := d.engine.Manifest.MarkDiffRunning(jobID); err != nil {
		return err
	}
	job, err := d.engine.Manifest.GetDiffJob(jobID)
	if err != nil {
		return err
	}
	baseEntries, err := d.engine.Manifest.EntriesOf(job.BaseSnapshotID)
	if err != nil {
		return err
	}
	targetEntries, err := d.engine.Manifest.EntriesOf(job.TargetSnapshot)
	if err != nil {
		return err
	}

	// ---- phase 1: verify every distinct referenced blob ------------------
	type ref struct {
		digest []byte
	}
	distinct := map[string][]byte{}
	collect := func(entries []repo.StoredEntry) {
		for _, e := range entries {
			if e.Kind != repo.KindFile {
				continue
			}
			for _, c := range e.ChunkDigests {
				k := hex.EncodeToString(c)
				if _, ok := distinct[k]; !ok {
					distinct[k] = append([]byte(nil), c...)
				}
			}
		}
	}
	collect(baseEntries)
	collect(targetEntries)
	keys := make([]string, 0, len(distinct))
	for k := range distinct {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if err := d.engine.Manifest.UpdateDiffProgress(jobID, repo.DiffProgress{
		ChunksTotal: int64(len(keys)),
	}); err != nil {
		return err
	}

	verdicts := map[string]blobVerdict{}
	var checked int64
	for _, k := range keys {
		if state, ok, err := d.engine.Manifest.ChunkCheckState(jobID, distinct[k]); err != nil {
			return err
		} else if ok {
			verdicts[k] = blobVerdict{state: state}
			checked++
			continue
		}
		v, err := d.verifyBlob(distinct[k])
		if err != nil {
			return err // I/O-level failure: retried on next kick/restart
		}
		if err := d.engine.Manifest.PutChunkCheck(jobID, distinct[k], v.state); err != nil {
			return err
		}
		verdicts[k] = v
		checked++
		if checked%32 == 0 {
			if err := d.engine.Manifest.UpdateDiffProgress(jobID, repo.DiffProgress{
				ChunksTotal: int64(len(keys)), ChunksChecked: checked,
			}); err != nil {
				return err
			}
		}
		if d.Fail.CrashAfterChunks > 0 && checked == int64(d.Fail.CrashAfterChunks) {
			_ = d.engine.Manifest.UpdateDiffProgress(jobID, repo.DiffProgress{
				ChunksTotal: int64(len(keys)), ChunksChecked: checked,
			})
			return errDiffCrashed
		}
	}
	if err := d.engine.Manifest.UpdateDiffProgress(jobID, repo.DiffProgress{
		ChunksTotal: int64(len(keys)), ChunksChecked: checked,
	}); err != nil {
		return err
	}

	// ---- phase 2: attribute bad references to files ----------------------
	affected := map[int64]map[string]bool{
		job.BaseSnapshotID: {},
		job.TargetSnapshot: {},
	}
	problems := map[string]repo.DiffProblemInput{} // dedup per snapshot+path+chunk
	markEntries := func(snapID int64, entries []repo.StoredEntry) {
		for _, e := range entries {
			if e.Kind != repo.KindFile {
				continue
			}
			for _, c := range e.ChunkDigests {
				k := hex.EncodeToString(c)
				v, known := verdicts[k]
				if !known || v.state == "ok" {
					continue
				}
				affected[snapID][e.RelPath] = true
				pkey := fmt.Sprintf("%d|%s|%s", snapID, e.RelPath, k)
				if _, dup := problems[pkey]; dup {
					continue
				}
				p := repo.DiffProblemInput{
					SnapshotID:  snapID,
					RelPath:     e.RelPath,
					ChunkDigest: k,
				}
				switch v.state {
				case repo.DiffProblemMissing:
					p.Kind = repo.DiffProblemMissing
					p.DeclaredLength = v.length
					p.Reason = "chunk blob missing, length mismatch, or chunk absent from catalog"
				case repo.DiffProblemMismatch:
					p.Kind = repo.DiffProblemMismatch
					p.DeclaredLength = v.length
					p.ActualDigest = v.actual
					p.Reason = "stored blob SHA-256 does not match catalog digest"
				}
				problems[pkey] = p
			}
		}
	}
	markEntries(job.BaseSnapshotID, baseEntries)
	markEntries(job.TargetSnapshot, targetEntries)

	// ---- phase 3: classify + aggregate ------------------------------------
	items, counts := classifyEntries(job, baseEntries, targetEntries, affected)
	for _, v := range verdicts {
		switch v.state {
		case repo.DiffProblemMissing:
			counts.Missing++
		case repo.DiffProblemMismatch:
			counts.Mismatch++
		}
	}
	counts.AffectedFiles = int64(len(affected[job.BaseSnapshotID]) + len(affected[job.TargetSnapshot]))

	problemList := make([]repo.DiffProblemInput, 0, len(problems))
	for _, p := range problems {
		problemList = append(problemList, p)
	}
	sort.Slice(problemList, func(i, j int) bool {
		if problemList[i].SnapshotID != problemList[j].SnapshotID {
			return problemList[i].SnapshotID < problemList[j].SnapshotID
		}
		if problemList[i].RelPath != problemList[j].RelPath {
			return problemList[i].RelPath < problemList[j].RelPath
		}
		return problemList[i].ChunkDigest < problemList[j].ChunkDigest
	})

	return d.engine.Manifest.SaveDiffResult(jobID, items, problemList, counts)
}

// verifyBlob checks one content-addressed blob: the catalog row must exist,
// the on-disk file must be a regular file of the declared length, and its
// streamed SHA-256 must equal its name.
func (d *DiffService) verifyBlob(digest []byte) (blobVerdict, error) {
	length, hasRow, err := d.engine.Manifest.ChunkLength(digest)
	if err != nil {
		return blobVerdict{}, err
	}
	if !hasRow {
		return blobVerdict{state: repo.DiffProblemMissing, length: -1}, nil
	}
	ok, err := d.engine.Store.Has(digest, length)
	if err != nil {
		return blobVerdict{}, err
	}
	if !ok {
		return blobVerdict{state: repo.DiffProblemMissing, length: length}, nil
	}
	matches, actual, err := d.engine.Store.VerifyDigest(digest)
	if err != nil {
		return blobVerdict{}, err
	}
	if !matches {
		return blobVerdict{
			state:  repo.DiffProblemMismatch,
			actual: hex.EncodeToString(actual),
			length: length,
		}, nil
	}
	return blobVerdict{state: "ok", length: length}, nil
}

// classifyEntries turns two entry sets into one DiffItem per entry.
func classifyEntries(job repo.DiffJob, base, target []repo.StoredEntry,
	affected map[int64]map[string]bool) ([]repo.DiffItem, repo.DiffResultCounts) {

	baseByPath := map[string]repo.StoredEntry{}
	targetByPath := map[string]repo.StoredEntry{}
	baseByDigest := map[string][]repo.StoredEntry{} // whole-file digest -> files
	targetByDigest := map[string][]repo.StoredEntry{}
	baseChunkSet := map[string]bool{}
	targetChunkSet := map[string]bool{}

	index := func(entries []repo.StoredEntry, byPath map[string]repo.StoredEntry,
		byDigest map[string][]repo.StoredEntry, chunkSet map[string]bool) {
		for _, e := range entries {
			byPath[e.RelPath] = e
			if e.Kind == repo.KindFile {
				if len(e.FileDigest) == sha256.Size {
					k := hex.EncodeToString(e.FileDigest)
					byDigest[k] = append(byDigest[k], e)
				}
				for _, c := range e.ChunkDigests {
					chunkSet[hex.EncodeToString(c)] = true
				}
			}
		}
	}
	index(base, baseByPath, baseByDigest, baseChunkSet)
	index(target, targetByPath, targetByDigest, targetChunkSet)

	pathSet := map[string]bool{}
	for p := range baseByPath {
		pathSet[p] = true
	}
	for p := range targetByPath {
		pathSet[p] = true
	}
	paths := make([]string, 0, len(pathSet))
	for p := range pathSet {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	usedBase := map[string]bool{}
	usedTarget := map[string]bool{}
	var items []repo.DiffItem
	reusedSet := map[string]bool{}
	newSet := map[string]bool{}
	affectedBase := affected[job.BaseSnapshotID]
	affectedTarget := affected[job.TargetSnapshot]

	// Pass 1: same-path pairs plus one-sided non-files.
	for _, p := range paths {
		b, inBase := baseByPath[p]
		t, inTarget := targetByPath[p]
		switch {
		case inBase && inTarget:
			items = append(items, classifyPair(b, t, affectedBase[p], affectedTarget[p],
				baseChunkSet, reusedSet, newSet))
			usedBase[p] = true
			usedTarget[p] = true
		case inBase && b.Kind != repo.KindFile:
			items = append(items, oneSided(b, true, affectedBase[p]))
			usedBase[p] = true
		case inTarget && t.Kind != repo.KindFile:
			items = append(items, oneSided(t, false, affectedTarget[p]))
			usedTarget[p] = true
		}
	}

	// Pass 2: moved files. A rename requires EXACTLY one file with that
	// whole-file digest in EACH snapshot (global one-to-one). Multiple
	// candidates on either side are "ambiguous" — no migration is invented.
	digestSet := map[string]bool{}
	for dg := range baseByDigest {
		digestSet[dg] = true
	}
	for dg := range targetByDigest {
		digestSet[dg] = true
	}
	digests := make([]string, 0, len(digestSet))
	for dg := range digestSet {
		digests = append(digests, dg)
	}
	sort.Strings(digests)

	for _, dg := range digests {
		var unmatchedB, unmatchedT []repo.StoredEntry
		for _, e := range baseByDigest[dg] {
			if !usedBase[e.RelPath] {
				unmatchedB = append(unmatchedB, e)
			}
		}
		for _, e := range targetByDigest[dg] {
			if !usedTarget[e.RelPath] {
				unmatchedT = append(unmatchedT, e)
			}
		}
		if len(unmatchedB) == 0 && len(unmatchedT) == 0 {
			continue // e.g. an unchanged same-path file already classified
		}
		sortEntries(unmatchedB)
		sortEntries(unmatchedT)

		switch {
		case len(unmatchedB) == 1 && len(unmatchedT) == 1 &&
			len(baseByDigest[dg]) == 1 && len(targetByDigest[dg]) == 1:
			b, t := unmatchedB[0], unmatchedT[0]
			items = append(items, renameItem(b, t,
				affectedBase[b.RelPath], affectedTarget[t.RelPath],
				baseChunkSet, reusedSet, newSet))
			usedBase[b.RelPath] = true
			usedTarget[t.RelPath] = true
		case len(unmatchedB) > 0 && len(unmatchedT) > 0:
			// Candidates exist on both sides but the pairing is not unique.
			markAmbiguous(dg, pathsOf(baseByDigest[dg]), pathsOf(targetByDigest[dg]),
				unmatchedB, unmatchedT, affectedBase, affectedTarget, &items, usedBase, usedTarget)
		case len(unmatchedT) > 0:
			if len(baseByDigest[dg]) == 0 {
				// Genuinely new content: no base file anywhere has this digest.
				for _, e := range unmatchedT {
					items = append(items, oneSidedWithStats(e, false, affectedTarget[e.RelPath],
						baseChunkSet, reusedSet, newSet))
					usedTarget[e.RelPath] = true
				}
			} else {
				// The digest exists in the base (possibly already consumed by
				// an unchanged same-path file), but the groups are not 1:1, so
				// naming the new copy a rename would be invented history.
				markAmbiguous(dg, pathsOf(baseByDigest[dg]), pathsOf(targetByDigest[dg]),
					unmatchedB, unmatchedT, affectedBase, affectedTarget, &items, usedBase, usedTarget)
			}
		default:
			if len(targetByDigest[dg]) == 0 {
				// Genuinely deleted: no target file anywhere has this digest.
				for _, e := range unmatchedB {
					items = append(items, oneSided(e, true, affectedBase[e.RelPath]))
					usedBase[e.RelPath] = true
				}
			} else {
				markAmbiguous(dg, pathsOf(baseByDigest[dg]), pathsOf(targetByDigest[dg]),
					unmatchedB, unmatchedT, affectedBase, affectedTarget, &items, usedBase, usedTarget)
			}
		}
	}

	counts := repo.DiffResultCounts{
		Reused: int64(len(reusedSet)),
		New:    int64(len(newSet)),
	}
	return items, counts
}

// markAmbiguous emits one "ambiguous" item per unmatched same-digest entry on
// both sides. baseAll/targetAll list every file with the digest in each
// snapshot (including entries already matched at their own path), so the
// report explains precisely why a unique rename cannot be asserted; no
// migration is invented.
func markAmbiguous(dg string, baseAll, targetAll []string,
	unmatchedB, unmatchedT []repo.StoredEntry,
	affectedBase, affectedTarget map[string]bool, items *[]repo.DiffItem,
	usedBase, usedTarget map[string]bool) {
	detail := diffDetail(dg, baseAll, targetAll)
	for _, e := range unmatchedB {
		it := oneSided(e, true, affectedBase[e.RelPath])
		it.ChangeType = repo.ChangeAmbiguous
		it.Detail = detail
		*items = append(*items, it)
		usedBase[e.RelPath] = true
	}
	for _, e := range unmatchedT {
		it := oneSided(e, false, affectedTarget[e.RelPath])
		it.ChangeType = repo.ChangeAmbiguous
		it.Detail = detail
		*items = append(*items, it)
		usedTarget[e.RelPath] = true
	}
}

// classifyPair handles an entry present at the same path in both snapshots.
func classifyPair(b, t repo.StoredEntry, baseBad, targetBad bool,
	baseChunkSet map[string]bool, reusedSet, newSet map[string]bool) repo.DiffItem {

	it := repo.DiffItem{
		BasePath:   b.RelPath,
		TargetPath: t.RelPath,
		EntryKind:  t.Kind,
	}
	var meta []string
	contentChanged := false

	if b.Kind != t.Kind {
		contentChanged = true
		meta = append(meta, "kind")
	}

	switch {
	case t.Kind == repo.KindFile && b.Kind == repo.KindFile:
		bd, td := hex.EncodeToString(b.FileDigest), hex.EncodeToString(t.FileDigest)
		it.FileDigest = td
		if bd != td {
			contentChanged = true
			meta = append(meta, "content")
		} else if !sameSequence(b.ChunkDigests, t.ChunkDigests) {
			// Identical whole-file digest but a different block sequence: the
			// representation changed; report content rather than guessing.
			contentChanged = true
			meta = append(meta, "chunk_sequence")
		}
	case t.Kind == repo.KindSymlink && b.Kind == repo.KindSymlink:
		if b.LinkTarget != t.LinkTarget {
			// Symlink targets classify as metadata; the link is never followed.
			meta = append(meta, "link_target")
		}
	}

	if b.Mode != t.Mode {
		meta = append(meta, "mode")
	}
	if b.UID != t.UID || b.GID != t.GID {
		meta = append(meta, "owner")
	}
	if !b.ModTime.Equal(t.ModTime) {
		meta = append(meta, "mtime")
	}

	switch {
	case contentChanged:
		it.ChangeType = repo.ChangeContent
	case len(meta) > 0:
		it.ChangeType = repo.ChangeMetadata
	default:
		it.ChangeType = repo.ChangeUnchanged
	}
	it.ChangedFields = strings.Join(meta, ",")

	if t.Kind == repo.KindFile {
		it.ChunksReused, it.ChunksNew = itemChunkStats(t, baseChunkSet, reusedSet, newSet)
	}

	if baseBad || targetBad {
		it.Integrity = "affected"
		// A bad block reference forbids claiming "unchanged"/"metadata only".
		if it.ChangeType == repo.ChangeUnchanged || it.ChangeType == repo.ChangeMetadata {
			it.ChangeType = repo.ChangeUnverified
		}
	}
	return it
}

// oneSided builds a plain added/deleted row for an entry without a same-digest
// counterpart anywhere in the other snapshot.
func oneSided(e repo.StoredEntry, baseSide, bad bool) repo.DiffItem {
	it := repo.DiffItem{EntryKind: e.Kind}
	if baseSide {
		it.BasePath = e.RelPath
		it.ChangeType = repo.ChangeDeleted
	} else {
		it.TargetPath = e.RelPath
		it.ChangeType = repo.ChangeAdded
	}
	if e.Kind == repo.KindFile {
		it.FileDigest = hex.EncodeToString(e.FileDigest)
	}
	if bad {
		it.Integrity = "affected"
		it.ChangeType = repo.ChangeUnverified
	}
	return it
}

// oneSidedWithStats is oneSided plus per-item block reuse stats (used for
// added files, whose blocks may already exist from the base snapshot).
func oneSidedWithStats(e repo.StoredEntry, baseSide, bad bool,
	baseChunkSet map[string]bool, reusedSet, newSet map[string]bool) repo.DiffItem {
	it := oneSided(e, baseSide, bad)
	if e.Kind == repo.KindFile && !baseSide {
		it.ChunksReused, it.ChunksNew = itemChunkStats(e, baseChunkSet, reusedSet, newSet)
	}
	if bad {
		// itemChunkStats still describes manifest-level reuse, useful info,
		// but the classification must not assert the content is intact/new.
		it.ChangeType = repo.ChangeUnverified
	}
	return it
}

func renameItem(b, t repo.StoredEntry, baseBad, targetBad bool,
	baseChunkSet map[string]bool, reusedSet, newSet map[string]bool) repo.DiffItem {
	var meta []string
	if b.Mode != t.Mode {
		meta = append(meta, "mode")
	}
	if b.UID != t.UID || b.GID != t.GID {
		meta = append(meta, "owner")
	}
	if !b.ModTime.Equal(t.ModTime) {
		meta = append(meta, "mtime")
	}
	it := repo.DiffItem{
		BasePath:      b.RelPath,
		TargetPath:    t.RelPath,
		EntryKind:     repo.KindFile,
		ChangeType:    repo.ChangeRenamed,
		ChangedFields: strings.Join(meta, ","),
		FileDigest:    hex.EncodeToString(t.FileDigest),
		Detail:        mustJSON(map[string]any{"renamed_from": b.RelPath}),
	}
	it.ChunksReused, it.ChunksNew = itemChunkStats(t, baseChunkSet, reusedSet, newSet)
	if baseBad || targetBad {
		it.Integrity = "affected"
		it.ChangeType = repo.ChangeUnverified
		it.Detail = mustJSON(map[string]any{
			"renamed_from": b.RelPath,
			"reason":       "referenced blob missing or digest mismatch; rename not verifiable",
		})
	}
	return it
}

// itemChunkStats counts a target file's blocks reused from the base snapshot.
func itemChunkStats(t repo.StoredEntry, baseChunkSet map[string]bool,
	reusedSet, newSet map[string]bool) (reused, n int64) {
	for _, c := range t.ChunkDigests {
		k := hex.EncodeToString(c)
		if baseChunkSet[k] {
			reused++
			reusedSet[k] = true
		} else {
			n++
			newSet[k] = true
		}
	}
	return
}

func sameSequence(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if hex.EncodeToString(a[i]) != hex.EncodeToString(b[i]) {
			return false
		}
	}
	return true
}

func sortEntries(es []repo.StoredEntry) {
	sort.Slice(es, func(i, j int) bool { return es[i].RelPath < es[j].RelPath })
}

func pathsOf(es []repo.StoredEntry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.RelPath)
	}
	return out
}

func diffDetail(digest string, baseCandidates, targetCandidates []string) string {
	sort.Strings(baseCandidates)
	sort.Strings(targetCandidates)
	return mustJSON(map[string]any{
		"digest":            digest,
		"base_candidates":   baseCandidates,
		"target_candidates": targetCandidates,
		"reason":            "multiple files share this digest; rename cannot be determined uniquely",
	})
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
