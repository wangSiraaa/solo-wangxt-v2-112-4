package backup_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// waitForDiff blocks until the persisted job reaches a terminal state.
func waitForDiff(t *testing.T, svc *backup.DiffService, id int64) repo.DiffJob {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		j, err := svc.GetDiff(id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == repo.DiffComplete || j.Status == repo.DiffFailed {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("diff %d stuck in %s", id, j.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func diffItemMap(t *testing.T, e *backup.Engine, jobID int64) map[string]repo.DiffItem {
	t.Helper()
	items, _, err := e.Manifest.ListDiffItems(jobID, repo.DiffItemFilter{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]repo.DiffItem{}
	for _, it := range items {
		if it.BasePath != "" {
			m["base:"+it.BasePath] = it
		}
		if it.TargetPath != "" {
			m["target:"+it.TargetPath] = it
		}
	}
	return m
}

// Acceptance ①: a mid-file byte edit is a content change, and the report
// shows block-level reuse rather than "everything is new".
func TestDiffLargeFileSmallEditShowsContentChangeAndReuse(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))

	big := strings.Repeat("0123456789ABCDEF\n", 12000) // ~180KiB, many chunks
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), []byte(big), 0o644))
	r1, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)

	buf := []byte(big)
	copy(buf[90*1024:], []byte("PATCHED IN THE MIDDLE"))
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), buf, 0o644))
	r2, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)

	job, err := svc.EnsureDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	job = waitForDiff(t, svc, job.ID)
	if job.Status != repo.DiffComplete || !job.Complete {
		t.Fatalf("job status=%s complete=%v err=%s", job.Status, job.Complete, job.Error)
	}
	if job.ChunksMissing != 0 || job.ChunksMismatch != 0 {
		t.Fatalf("unexpected integrity problems: missing=%d mismatch=%d",
			job.ChunksMissing, job.ChunksMismatch)
	}
	// almost every block is reused; the mid-file edit produces exactly 1 new
	if job.ChunksNew != 1 {
		t.Fatalf("want 1 new block, got %d (reused=%d total=%d)",
			job.ChunksNew, job.ChunksReused, job.ChunksTotal)
	}
	if job.ChunksReused == 0 {
		t.Fatal("report must count reused blocks")
	}

	items := diffItemMap(t, e, job.ID)
	it := items["target:big.bin"]
	if it.ChangeType != repo.ChangeContent {
		t.Fatalf("big.bin change=%q want changed", it.ChangeType)
	}
	if it.ChunksReused == 0 || it.ChunksNew != 1 {
		t.Fatalf("per-item reuse=%d new=%d", it.ChunksReused, it.ChunksNew)
	}
}

// Acceptance ②: a single renamed file is recognized by its whole-file digest,
// but two same-content files produce an explicit ambiguity, never an invented
// migration.
func TestDiffRenameUniqueVersusAmbiguousCandidates(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))

	// --- unique rename ---
	content := strings.Repeat("rename me\n", 500)
	must(t, os.WriteFile(filepath.Join(src, "old-name.txt"), []byte(content), 0o644))
	r1, err := e.CreateSnapshot(src, "before-rename", true)
	must(t, err)
	must(t, os.Rename(filepath.Join(src, "old-name.txt"), filepath.Join(src, "new-name.txt")))
	r2, err := e.CreateSnapshot(src, "after-rename", true)
	must(t, err)

	job, err := svc.EnsureDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	job = waitForDiff(t, svc, job.ID)
	if job.Status != repo.DiffComplete {
		t.Fatalf("status=%s err=%s", job.Status, job.Error)
	}
	items, _, err := e.Manifest.ListDiffItems(job.ID, repo.DiffItemFilter{}, 0, 0)
	must(t, err)
	var foundRenamed bool
	for _, it := range items {
		if it.ChangeType == repo.ChangeRenamed {
			foundRenamed = true
			if it.BasePath != "old-name.txt" || it.TargetPath != "new-name.txt" {
				t.Fatalf("rename pair = %s -> %s", it.BasePath, it.TargetPath)
			}
		}
	}
	if !foundRenamed {
		t.Fatalf("expected a renamed item, got %+v", items)
	}

	// --- same digest, two files: must be ambiguous, not a guessed rename ---
	src2 := filepath.Join(dir, "src2")
	must(t, os.MkdirAll(src2, 0o755))
	dup := strings.Repeat("identical content\n", 400)
	must(t, os.WriteFile(filepath.Join(src2, "a.bin"), []byte(dup), 0o644))
	r3, err := e.CreateSnapshot(src2, "dup-base", true)
	must(t, err)
	// base has one file "a.bin"; target has two copies (a.bin stays + b.bin)
	must(t, os.WriteFile(filepath.Join(src2, "b.bin"), []byte(dup), 0o644))
	r4, err := e.CreateSnapshot(src2, "dup-two", true)
	must(t, err)

	job2, err := svc.EnsureDiff(r3.SnapshotID, r4.SnapshotID)
	must(t, err)
	job2 = waitForDiff(t, svc, job2.ID)
	if job2.Status != repo.DiffComplete {
		t.Fatalf("status=%s err=%s", job2.Status, job2.Error)
	}
	items2, _, err := e.Manifest.ListDiffItems(job2.ID, repo.DiffItemFilter{}, 0, 0)
	must(t, err)
	amb := 0
	renamed := 0
	for _, it := range items2 {
		if it.ChangeType == repo.ChangeAmbiguous {
			amb++
			if !strings.Contains(it.Detail, "target_candidates") {
				t.Fatalf("ambiguous detail missing candidates: %s", it.Detail)
			}
		}
		if it.ChangeType == repo.ChangeRenamed {
			renamed++
		}
	}
	if renamed != 0 {
		t.Fatalf("must not invent rename with duplicate digests, got %d renames", renamed)
	}
	if amb == 0 {
		t.Fatalf("expected ambiguous classification for duplicate-content files, got %+v", items2)
	}

	// reverse direction: two base files, one target file -> still ambiguous
	job3, err := svc.EnsureDiff(r4.SnapshotID, r3.SnapshotID)
	must(t, err)
	job3 = waitForDiff(t, svc, job3.ID)
	items3, _, err := e.Manifest.ListDiffItems(job3.ID, repo.DiffItemFilter{}, 0, 0)
	must(t, err)
	amb3 := 0
	for _, it := range items3 {
		if it.ChangeType == repo.ChangeAmbiguous {
			amb3++
		}
		if it.ChangeType == repo.ChangeRenamed {
			t.Fatal("reverse direction must not invent a rename")
		}
	}
	if amb3 == 0 {
		t.Fatalf("reverse comparison must also be ambiguous, got %+v", items3)
	}
}

// Acceptance ③: permission/mtime only changes are metadata; a symlink target
// string change is metadata and never follows the link.
func TestDiffMetadataClassificationAndNoSymlinkFollow(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))

	must(t, os.WriteFile(filepath.Join(src, "f.txt"), []byte("unchanged bytes\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "target-a"), []byte("A\n"), 0o600))
	must(t, os.WriteFile(filepath.Join(src, "target-b"), []byte("B\n"), 0o600))
	must(t, os.Symlink("target-a", filepath.Join(src, "link")))
	r1, err := e.CreateSnapshot(src, "meta-base", true)
	must(t, err)

	// chmod only
	must(t, os.Chmod(filepath.Join(src, "f.txt"), 0o600))
	// mtime only on the directory (content unchanged)
	newTime := time.Now().Add(2 * time.Hour)
	must(t, os.Chtimes(src, newTime, newTime))
	// retarget the symlink: target string changes, link itself is not followed
	must(t, os.Remove(filepath.Join(src, "link")))
	must(t, os.Symlink("target-b", filepath.Join(src, "link")))

	r2, err := e.CreateSnapshot(src, "meta-only", true)
	must(t, err)
	job, err := svc.EnsureDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	job = waitForDiff(t, svc, job.ID)
	if job.Status != repo.DiffComplete {
		t.Fatalf("status=%s err=%s", job.Status, job.Error)
	}

	items := diffItemMap(t, e, job.ID)
	f := items["target:f.txt"]
	if f.ChangeType != repo.ChangeMetadata {
		t.Fatalf("chmod-only file classified as %q, want metadata_changed", f.ChangeType)
	}
	if !strings.Contains(f.ChangedFields, "mode") {
		t.Fatalf("changed_fields=%q missing mode", f.ChangedFields)
	}
	root := items["target:"+"."]
	if root.ChangeType != repo.ChangeMetadata || !strings.Contains(root.ChangedFields, "mtime") {
		t.Fatalf("root dir mtime change: %+v", root)
	}
	ln := items["target:link"]
	if ln.ChangeType != repo.ChangeMetadata {
		t.Fatalf("symlink retarget classified as %q, want metadata_changed", ln.ChangeType)
	}
	if !strings.Contains(ln.ChangedFields, "link_target") {
		t.Fatalf("changed_fields=%q missing link_target", ln.ChangedFields)
	}

	// The pointed-to files must not be reported changed (link was not followed).
	ta := items["target:target-a"]
	tb := items["target:target-b"]
	if ta.ChangeType != repo.ChangeUnchanged || tb.ChangeType != repo.ChangeUnchanged {
		t.Fatalf("link targets must stay unchanged (no follow): a=%s b=%s",
			ta.ChangeType, tb.ChangeType)
	}
}

// content-only change still reports content, never downgraded to metadata.
func TestDiffContentChangeBeatsMetadata(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "x"), []byte("one\n"), 0o644))
	r1, err := e.CreateSnapshot(src, "a", true)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(src, "x"), []byte("two-different\n"), 0o600))
	r2, err := e.CreateSnapshot(src, "b", true)
	must(t, err)
	job := waitForDiff(t, svc, mustEnsure(t, svc, r1.SnapshotID, r2.SnapshotID))
	items := diffItemMap(t, e, job.ID)
	it := items["target:x"]
	if it.ChangeType != repo.ChangeContent {
		t.Fatalf("want changed, got %q fields=%s", it.ChangeType, it.ChangedFields)
	}
}

// empty files all share the e3b0… digest: same-path empties stay unchanged,
// and a single empty file that moves can only be a rename when 1:1.
func TestDiffEmptyFilesAndDirectoryLifecycle(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(filepath.Join(src, "d1"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "e1"), nil, 0o644))
	must(t, os.WriteFile(filepath.Join(src, "e2"), nil, 0o644))
	r1, err := e.CreateSnapshot(src, "base", true)
	must(t, err)

	// two same-path empties stay unchanged, but a third copy shares their
	// digest and CANNOT be called an addition-from-nowhere with certainty:
	// it is ambiguous (might be a copy/migration of e1 or e2); a new
	// directory appears and d1 is removed.
	must(t, os.WriteFile(filepath.Join(src, "e3"), nil, 0o644))
	must(t, os.MkdirAll(filepath.Join(src, "d2"), 0o750))
	must(t, os.RemoveAll(filepath.Join(src, "d1")))
	r2, err := e.CreateSnapshot(src, "target", true)
	must(t, err)

	job := waitForDiff(t, svc, mustEnsure(t, svc, r1.SnapshotID, r2.SnapshotID))
	if job.Status != repo.DiffComplete {
		t.Fatalf("status=%s err=%s", job.Status, job.Error)
	}
	items := diffItemMap(t, e, job.ID)
	if items["target:e1"].ChangeType != repo.ChangeUnchanged ||
		items["target:e2"].ChangeType != repo.ChangeUnchanged {
		t.Fatalf("same-path empty files must stay unchanged: e1=%s e2=%s",
			items["target:e1"].ChangeType, items["target:e2"].ChangeType)
	}
	if items["target:e3"].ChangeType != repo.ChangeAmbiguous {
		t.Fatalf("new empty copy with same digest must be ambiguous, got %s",
			items["target:e3"].ChangeType)
	}
	if items["base:d1"].ChangeType != repo.ChangeDeleted ||
		items["target:d2"].ChangeType != repo.ChangeAdded {
		t.Fatalf("directory lifecycle: d1=%s d2=%s",
			items["base:d1"].ChangeType, items["target:d2"].ChangeType)
	}

	// a genuinely new empty file in a tree with no other empty file is added
	src0 := filepath.Join(dir, "src0")
	must(t, os.MkdirAll(src0, 0o755))
	must(t, os.WriteFile(filepath.Join(src0, "nonempty"), []byte("x\n"), 0o644))
	c0, err := e.CreateSnapshot(src0, "p0", true)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(src0, "fresh-empty"), nil, 0o644))
	c1, err := e.CreateSnapshot(src0, "p1", true)
	must(t, err)
	j0 := waitForDiff(t, svc, mustEnsure(t, svc, c0.SnapshotID, c1.SnapshotID))
	i0 := diffItemMap(t, e, j0.ID)
	if i0["target:fresh-empty"].ChangeType != repo.ChangeAdded {
		t.Fatalf("first empty file in tree must be added, got %s",
			i0["target:fresh-empty"].ChangeType)
	}

	// a lone empty file moving 1:1 is a valid rename (digest unique per side)
	src2 := filepath.Join(dir, "src2")
	must(t, os.MkdirAll(src2, 0o755))
	must(t, os.WriteFile(filepath.Join(src2, "only-empty"), nil, 0o644))
	a, err := e.CreateSnapshot(src2, "ea", true)
	must(t, err)
	must(t, os.Rename(filepath.Join(src2, "only-empty"), filepath.Join(src2, "moved-empty")))
	b, err := e.CreateSnapshot(src2, "eb", true)
	must(t, err)
	j := waitForDiff(t, svc, mustEnsure(t, svc, a.SnapshotID, b.SnapshotID))
	its, _, err := e.Manifest.ListDiffItems(j.ID, repo.DiffItemFilter{}, 0, 0)
	must(t, err)
	saw := false
	for _, it := range its {
		if it.ChangeType == repo.ChangeRenamed &&
			it.BasePath == "only-empty" && it.TargetPath == "moved-empty" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("single empty file rename not recognized: %+v", its)
	}
}

func mustEnsure(t *testing.T, svc *backup.DiffService, a, b int64) int64 {
	t.Helper()
	j, err := svc.EnsureDiff(a, b)
	if err != nil {
		t.Fatal(err)
	}
	return j.ID
}

// Acceptance ④a: pending/failed snapshots are rejected.
func TestDiffRejectsNonCommittedSnapshots(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "ok.txt"), []byte("hi\n"), 0o644))

	good, err := e.CreateSnapshot(src, "good", true)
	must(t, err)

	// pending via finish:false
	pend, err := e.CreateSnapshot(src, "pending", false)
	must(t, err)
	if pend.Status != repo.StatusPending {
		t.Fatalf("want pending, got %s", pend.Status)
	}
	if _, err := svc.EnsureDiff(good.SnapshotID, pend.SnapshotID); err == nil {
		t.Fatal("comparison against pending must fail")
	} else {
		var nc *backup.ErrDiffNotCommitted
		if !errors.As(err, &nc) {
			t.Fatalf("want ErrDiffNotCommitted, got %v", err)
		}
		if nc.Status != repo.StatusPending {
			t.Fatalf("error status=%s", nc.Status)
		}
	}

	// failed via lose_chunks
	e.Fail.LoseChunkCount = 1
	must(t, os.WriteFile(filepath.Join(src, "grow"), []byte(strings.Repeat("q", 9000)), 0o644))
	failRes, err := e.CreateSnapshot(src, "broken", true)
	e.Fail.LoseChunkCount = 0
	if err == nil {
		t.Fatal("expected injected failure")
	}
	if _, err := svc.EnsureDiff(failRes.SnapshotID, good.SnapshotID); err == nil {
		t.Fatal("comparison against failed must fail")
	}
}

// Acceptance ④b: a corrupted blob discovered during comparison yields an
// incomplete report that names the paths, while both input snapshots keep
// their committed status and the intact one stays fully restorable. The diff
// itself never mutates snapshots — it only diagnoses pre-existing storage rot.
func TestDiffCorruptedBlobProducesIncompleteReport(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "stable.txt"), []byte("never touched\n"), 0o644))
	// a multi-chunk file unique to snapshot 2 so its corruption does not
	// physically affect snapshot 1
	fresh := strings.Repeat("fresh-block-content\n", 1200)
	r1, err := e.CreateSnapshot(src, "s1", true)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(src, "new.bin"), []byte(fresh), 0o644))
	r2, err := e.CreateSnapshot(src, "s2", true)
	must(t, err)

	// Find a chunk of new.bin referenced ONLY by snapshot 2 (singleton), and
	// tamper its blob in place with same-length bytes after the diff inputs
	// are both committed.
	singletons, err := e.Manifest.SingletonChunks(r2.SnapshotID, 64)
	must(t, err)
	var victim []byte
	for _, c := range singletons {
		// pick a chunk actually used by new.bin
		ens, err := e.Manifest.EntriesOf(r2.SnapshotID)
		must(t, err)
		for _, en := range ens {
			if en.RelPath != "new.bin" {
				continue
			}
			for _, cd := range en.ChunkDigests {
				if hexEq(cd, c.Digest) {
					victim = append([]byte(nil), c.Digest...)
				}
			}
		}
		if victim != nil {
			break
		}
	}
	if victim == nil {
		t.Fatal("no singleton chunk of new.bin found")
	}
	p, err := e.Store.Path(victim)
	must(t, err)
	must(t, os.Chmod(p, 0o644))
	orig, err := os.ReadFile(p)
	must(t, err)
	tampered := make([]byte, len(orig))
	copy(tampered, orig)
	for i := range tampered {
		tampered[i] = ^tampered[i] // same length, different bytes
	}
	must(t, os.WriteFile(p, tampered, 0o644))

	job := waitForDiff(t, svc, mustEnsure(t, svc, r1.SnapshotID, r2.SnapshotID))
	if job.Status != repo.DiffComplete || !job.Complete {
		t.Fatalf("corrupted-blob comparison should still complete with a report, status=%s err=%s",
			job.Status, job.Error)
	}
	if job.ChunksMismatch == 0 {
		t.Fatalf("expected digest_mismatch counters, got missing=%d mismatch=%d",
			job.ChunksMissing, job.ChunksMismatch)
	}
	if job.AffectedFiles == 0 {
		t.Fatal("affected file count must be > 0")
	}
	probs, err := e.Manifest.ListDiffProblems(job.ID)
	must(t, err)
	if len(probs) == 0 {
		t.Fatal("problems list must name corrupted references")
	}
	sawPath := false
	for _, pr := range probs {
		if pr.RelPath == "new.bin" && pr.Kind == repo.DiffProblemMismatch {
			sawPath = true
			if pr.ActualDigest == "" {
				t.Fatal("mismatch problem should record actual on-disk digest")
			}
		}
	}
	if !sawPath {
		t.Fatalf("new.bin not listed in problems: %+v", probs)
	}
	items, _, err := e.Manifest.ListDiffItems(job.ID, repo.DiffItemFilter{}, 0, 0)
	must(t, err)
	for _, it := range items {
		if it.TargetPath == "new.bin" || it.BasePath == "new.bin" {
			if it.Integrity != "affected" || it.ChangeType != repo.ChangeUnverified {
				t.Fatalf("corrupted new.bin must be unverified, got %s/%s",
					it.ChangeType, it.Integrity)
			}
		}
	}

	// diffing must not rewrite snapshot state: both stay committed
	for _, id := range []int64{r1.SnapshotID, r2.SnapshotID} {
		si, err := e.Manifest.GetSnapshot(id)
		must(t, err)
		if si.Status != repo.StatusCommitted {
			t.Fatalf("snapshot %d status changed to %s by diffing", id, si.Status)
		}
	}
	// the intact snapshot (whose blobs were never touched) stays restorable
	if _, err := e.Restore(r1.SnapshotID, filepath.Join(dir, "restore-1")); err != nil {
		t.Fatalf("intact base snapshot restore capability changed: %v", err)
	}
	// the snapshot whose blob physically rotted cannot restore — that is the
	// storage rot, not anything the diff did.
	if _, err := e.Restore(r2.SnapshotID, filepath.Join(dir, "restore-2")); err == nil {
		t.Fatal("restore of a snapshot whose blob is corrupt must fail verification")
	}
}

func hexEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// interrupted diff resumes without re-verifying cached blobs and does not
// rewrite input snapshots.
func TestDiffResumesAfterInterruption(t *testing.T) {
	e, dir := openEngine(t)
	svc := backup.NewDiffService(e)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "a"), []byte(strings.Repeat("a", 8000)), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "b"), []byte(strings.Repeat("b", 8000)), 0o644))
	r1, err := e.CreateSnapshot(src, "a", true)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(src, "c"), []byte(strings.Repeat("c", 8000)), 0o644))
	r2, err := e.CreateSnapshot(src, "b", true)
	must(t, err)

	totalRefs := r2.RefChunks

	svc.Fail.CrashAfterChunks = 1
	job, err := svc.EnsureDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	// worker "dies" after the first cached verdict
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, _ := svc.GetDiff(job.ID)
		if j.ChunksChecked >= 1 && j.Status == repo.DiffRunning {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	stuck, err := svc.GetDiff(job.ID)
	must(t, err)
	if stuck.ChunksChecked == 0 || stuck.Status != repo.DiffRunning {
		t.Fatalf("failpoint did not leave interrupted job: %+v", stuck)
	}

	// resume (simulating restart recovery); cached verdict is skipped
	svc.Fail.CrashAfterChunks = 0
	ids, err := svc.RecoverInterrupted()
	must(t, err)
	if len(ids) != 1 {
		t.Fatalf("expected 1 resumable job, got %v", ids)
	}
	finished := waitForDiff(t, svc, job.ID)
	if finished.Status != repo.DiffComplete {
		t.Fatalf("resumed job status=%s err=%s", finished.Status, finished.Error)
	}
	if finished.ChunksTotal != totalRefs {
		t.Fatalf("total=%d want %d", finished.ChunksTotal, totalRefs)
	}
	if finished.ChunksChecked != totalRefs {
		t.Fatalf("checked=%d want %d", finished.ChunksChecked, totalRefs)
	}

	// same request again returns the same traceable report, no recomputation
	again, err := svc.EnsureDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	if again.ID != job.ID || again.Status != repo.DiffComplete {
		t.Fatalf("duplicate request must return same report: id %d vs %d", again.ID, job.ID)
	}
}
