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

// itemByPath finds the diff items for one path.
func itemsByPath(items []repo.DiffItem, rel string) []repo.DiffItem {
	var out []repo.DiffItem
	for _, it := range items {
		if it.RelPath == rel || it.OldRelPath == rel {
			out = append(out, it)
		}
	}
	return out
}

func mustOneItem(t *testing.T, items []repo.DiffItem, rel, changeType string) repo.DiffItem {
	t.Helper()
	var found []repo.DiffItem
	for _, it := range items {
		if it.RelPath == rel && it.ChangeType == changeType {
			found = append(found, it)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly 1 %s item for %s, got %d (all: %v)", changeType, rel, len(found), items)
	}
	return found[0]
}

// Acceptance ①: a few bytes changed in the middle of a large file must be
// reported as a content change that reuses most of its chunks.
func TestDiffSmallEditShowsContentChangeAndReuse(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	big := strings.Repeat("0123456789ABCDEF\n", 12000) // ~180KiB
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), []byte(big), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "small.txt"), []byte("untouched\n"), 0o644))

	r1, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)

	buf := []byte(big)
	copy(buf[90*1024:], []byte("PATCHED!"))
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), buf, 0o644))
	r2, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)
	if r2.NewChunks != 1 {
		t.Fatalf("sanity: expected 1 new chunk for the edit, got %d", r2.NewChunks)
	}

	rep, created, err := e.CreateDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	if !created {
		t.Fatal("first request must create the report")
	}
	if rep.Status != repo.DiffDone || rep.Integrity != repo.IntegrityComplete {
		t.Fatalf("status=%s integrity=%s", rep.Status, rep.Integrity)
	}
	if rep.ContentChanged != 1 {
		t.Fatalf("content_changed=%d want 1", rep.ContentChanged)
	}
	// storage-level: exactly one distinct new chunk; every other chunk the
	// target references was already in the base (the replaced old chunk is
	// no longer referenced, hence RefChunks-1)
	if rep.ChunksNew != 1 {
		t.Fatalf("report chunks_new=%d want 1", rep.ChunksNew)
	}
	if rep.ChunksReused != r1.RefChunks-1 {
		t.Fatalf("report chunks_reused=%d want %d",
			rep.ChunksReused, r1.RefChunks-1)
	}

	items, err := e.Manifest.DiffItemsOf(rep.ID, "")
	must(t, err)
	it := mustOneItem(t, items, "big.bin", repo.ChangeContentChanged)
	if it.ChunksNew != 1 {
		t.Fatalf("big.bin chunks_new=%d want 1", it.ChunksNew)
	}
	if it.ChunksReused != r1.RefChunks-2 { // big file chunks minus the replaced one
		t.Fatalf("big.bin chunks_reused=%d want %d", it.ChunksReused, r1.RefChunks-2)
	}
	if it.OldSize != it.NewSize || it.OldSize != int64(len(big)) {
		t.Fatalf("sizes old=%d new=%d", it.OldSize, it.NewSize)
	}
	if string(it.OldDigest) == string(it.NewDigest) {
		t.Fatal("digests must differ after the edit")
	}
	// the untouched file must not appear as changed
	if got := itemsByPath(items, "small.txt"); len(got) != 0 {
		t.Fatalf("small.txt must be unchanged, got %+v", got)
	}

	// repeated request: same traceable report, no recompute
	rep2, created2, err := e.CreateDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	if created2 || rep2.ID != rep.ID {
		t.Fatalf("idempotency: created=%v id=%d want %d", created2, rep2.ID, rep.ID)
	}
	if rep2.FinishedAt == nil || rep2.RequestedAt.IsZero() {
		t.Fatal("report must carry request/finish timestamps")
	}
}

// Acceptance ②: a single renamed file is detected; two files with identical
// content on either side make the mapping ambiguous and must not be paired.
func TestDiffRenameAndAmbiguous(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "solo.txt"), []byte("i will be renamed\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "dup1.txt"), []byte("same content\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "dup2.txt"), []byte("same content\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "keep.txt"), []byte("keep\n"), 0o644))

	r1, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)

	must(t, os.Rename(filepath.Join(src, "solo.txt"), filepath.Join(src, "solo-renamed.txt")))
	must(t, os.Remove(filepath.Join(src, "dup1.txt")))
	must(t, os.Remove(filepath.Join(src, "dup2.txt")))
	must(t, os.WriteFile(filepath.Join(src, "dup3.txt"), []byte("same content\n"), 0o644))

	r2, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)

	rep, _, err := e.CreateDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	if rep.Integrity != repo.IntegrityComplete {
		t.Fatalf("integrity=%s", rep.Integrity)
	}
	items, err := e.Manifest.DiffItemsOf(rep.ID, "")
	must(t, err)

	// 1:1 rename detected
	ren := mustOneItem(t, items, "solo-renamed.txt", repo.ChangeRenamed)
	if ren.OldRelPath != "solo.txt" {
		t.Fatalf("rename old path = %q", ren.OldRelPath)
	}
	if string(ren.OldDigest) != string(ren.NewDigest) {
		t.Fatal("rename must keep the digest")
	}
	if ren.ChunksNew != 0 || ren.ChunksReused != 1 {
		t.Fatalf("rename chunks reused=%d new=%d, want 1/0", ren.ChunksReused, ren.ChunksNew)
	}

	// identical-content group: ambiguous on every side, no invented pairing
	if got := itemsByPath(items, "dup1.txt"); len(got) != 1 || got[0].ChangeType != repo.ChangeAmbiguous {
		t.Fatalf("dup1.txt must be ambiguous, got %+v", got)
	}
	if got := itemsByPath(items, "dup2.txt"); len(got) != 1 || got[0].ChangeType != repo.ChangeAmbiguous {
		t.Fatalf("dup2.txt must be ambiguous, got %+v", got)
	}
	d3 := itemsByPath(items, "dup3.txt")
	if len(d3) != 1 || d3[0].ChangeType != repo.ChangeAmbiguous {
		t.Fatalf("dup3.txt must be ambiguous, got %+v", d3)
	}
	if len(d3[0].Candidates) != 2 {
		t.Fatalf("dup3 candidates=%v want [dup1.txt dup2.txt]", d3[0].Candidates)
	}
	// no item may claim a rename/migration for the duplicate group
	for _, it := range items {
		if it.ChangeType == repo.ChangeRenamed &&
			(strings.HasPrefix(it.RelPath, "dup") || strings.HasPrefix(it.OldRelPath, "dup")) {
			t.Fatalf("invented rename for duplicate content: %+v", it)
		}
	}
	if rep.Renamed != 1 || rep.Ambiguous != 3 {
		t.Fatalf("renamed=%d ambiguous=%d, want 1/3", rep.Renamed, rep.Ambiguous)
	}
	// ambiguous content still exists in both snapshots: no new chunks at all
	if rep.ChunksNew != 0 {
		t.Fatalf("chunks_new=%d want 0", rep.ChunksNew)
	}
}

// Acceptance ③: permission-only, mtime-only and link-target-only changes are
// classified correctly; symlink targets are compared as stored strings
// (links are never followed).
func TestDiffMetaOnlyChanges(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "script.sh"), []byte("#!/bin/sh\n"), 0o750))
	must(t, os.WriteFile(filepath.Join(src, "data.txt"), []byte("data\n"), 0o644))
	must(t, os.Symlink("data.txt", filepath.Join(src, "link")))

	r1, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)

	must(t, os.Chmod(filepath.Join(src, "script.sh"), 0o700))     // mode only
	old := time.Now().Add(-2 * time.Hour)                         //
	must(t, os.Chtimes(filepath.Join(src, "data.txt"), old, old)) // mtime only
	must(t, os.Remove(filepath.Join(src, "link")))                //
	must(t, os.Symlink("script.sh", filepath.Join(src, "link")))  // target only

	r2, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)

	rep, _, err := e.CreateDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	items, err := e.Manifest.DiffItemsOf(rep.ID, "")
	must(t, err)

	mode := mustOneItem(t, items, "script.sh", repo.ChangeMetaChanged)
	if len(mode.ChangedFields) != 1 || mode.ChangedFields[0] != "mode" {
		t.Fatalf("script.sh fields=%v want [mode]", mode.ChangedFields)
	}
	if mode.OldMode != 0o750 || mode.NewMode != 0o700 {
		t.Fatalf("script.sh modes %o -> %o", mode.OldMode, mode.NewMode)
	}
	if string(mode.OldDigest) != string(mode.NewDigest) {
		t.Fatal("meta-only change must keep the digest")
	}

	mt := mustOneItem(t, items, "data.txt", repo.ChangeMetaChanged)
	if len(mt.ChangedFields) != 1 || mt.ChangedFields[0] != "mtime" {
		t.Fatalf("data.txt fields=%v want [mtime]", mt.ChangedFields)
	}

	// link target change = content change of the symlink; target strings are
	// compared as stored, never resolved
	link := mustOneItem(t, items, "link", repo.ChangeContentChanged)
	if link.Kind != repo.KindSymlink {
		t.Fatalf("link kind=%s", link.Kind)
	}
	if link.OldLinkTarget != "data.txt" || link.NewLinkTarget != "script.sh" {
		t.Fatalf("link targets %q -> %q", link.OldLinkTarget, link.NewLinkTarget)
	}
	// the target file itself is untouched by the link change
	if got := itemsByPath(items, "script.sh"); len(got) != 1 || got[0].ChangeType != repo.ChangeMetaChanged {
		t.Fatalf("script.sh must only have its mode change, got %+v", got)
	}
	if rep.ContentChanged != 1 {
		t.Fatalf("content_changed=%d want 1 (the symlink)", rep.ContentChanged)
	}
}

// Acceptance ④: pending/failed snapshots are rejected; a corrupted chunk
// found during compare yields an incomplete report naming the path, while
// both snapshots keep their status and restorability.
func TestDiffRejectsNonCommittedAndCorruptionIsReported(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte(strings.Repeat("a", 9000)), 0o644))
	must(t, os.WriteFile(filepath.Join(src, "b.txt"), []byte("b contents\n"), 0o644))

	r1, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	pend, err := e.CreateSnapshot(src, "pending", false)
	must(t, err)
	if pend.Status != repo.StatusPending {
		t.Fatalf("want pending, got %s", pend.Status)
	}

	// pending on either side is rejected, and no report row is created
	for _, pair := range [][2]int64{
		{r1.SnapshotID, pend.SnapshotID},
		{pend.SnapshotID, r1.SnapshotID},
	} {
		_, _, err := e.CreateDiff(pair[0], pair[1])
		var rej *backup.ErrDiffRejected
		if !errors.As(err, &rej) {
			t.Fatalf("pair %v: want ErrDiffRejected, got %v", pair, err)
		}
		if _, err := e.Manifest.DiffReportByPair(pair[0], pair[1]); !errors.Is(err, repo.ErrDiffNotFound) {
			t.Fatalf("pair %v: rejected request must not persist a report", pair)
		}
	}
	if _, _, err := e.CreateDiff(r1.SnapshotID, r1.SnapshotID); err == nil {
		t.Fatal("self-diff must be rejected")
	}

	// finalize the pending one, then make a failed one
	if _, err := e.VerifyAndFinalize(pend.SnapshotID); err != nil {
		t.Fatal(err)
	}
	must(t, os.WriteFile(filepath.Join(src, "b.txt"), []byte("b changed\n"), 0o644))
	e.Fail.LoseChunkCount = 1
	failed, ferr := e.CreateSnapshot(src, "interrupted", true)
	e.Fail.LoseChunkCount = 0
	if ferr == nil {
		t.Fatal("want rejection for lost chunk")
	}
	if _, _, err := e.CreateDiff(r1.SnapshotID, failed.SnapshotID); err == nil {
		t.Fatal("failed snapshot must be rejected for diff")
	}

	// a committed pair diffs cleanly
	must(t, os.WriteFile(filepath.Join(src, "c.txt"), []byte("new file\n"), 0o644))
	r2, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)

	// corrupt the blob of a.txt's chunk (a.txt is unchanged between v1/v2)
	entries, err := e.Manifest.EntriesOf(r1.SnapshotID)
	must(t, err)
	var digest []byte
	for _, se := range entries {
		if se.RelPath == "a.txt" {
			digest = se.ChunkDigests[0]
		}
	}
	if digest == nil {
		t.Fatal("a.txt chunk not found")
	}
	blobPath, err := e.Store.Path(digest)
	must(t, err)
	orig, err := os.ReadFile(blobPath)
	must(t, err)
	must(t, os.Chmod(blobPath, 0o644))
	bad := make([]byte, len(orig))
	for i := range bad {
		bad[i] = 0xFF
	}
	must(t, os.WriteFile(blobPath, bad, 0o444))

	rep, _, err := e.CreateDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	if rep.Status != repo.DiffDone {
		t.Fatalf("status=%s", rep.Status)
	}
	if rep.Integrity != repo.IntegrityIncomplete {
		t.Fatalf("integrity=%s want incomplete", rep.Integrity)
	}
	// the corrupt file is unchanged between the snapshots, yet must be named
	// in the missing list rather than reported as safely unchanged
	items, err := e.Manifest.DiffItemsOf(rep.ID, "")
	must(t, err)
	if got := itemsByPath(items, "a.txt"); len(got) != 0 {
		t.Fatalf("a.txt has no tree change, got %+v", got)
	}
	missing, err := e.Manifest.DiffMissingOf(rep.ID)
	must(t, err)
	var namedA, namedBothSnapshots int
	for _, m := range missing {
		if m.RelPath == "a.txt" {
			namedA++
			if m.SnapshotID == r1.SnapshotID || m.SnapshotID == r2.SnapshotID {
				namedBothSnapshots++
			}
			if !strings.Contains(m.Reason, "digest") {
				t.Fatalf("reason=%q must mention the digest mismatch", m.Reason)
			}
		}
	}
	if namedA == 0 || namedBothSnapshots != 2 {
		t.Fatalf("a.txt must be named for both snapshots, got %+v", missing)
	}

	// the diff run must not have touched the snapshots themselves
	for _, id := range []int64{r1.SnapshotID, r2.SnapshotID} {
		si, err := e.Manifest.GetSnapshot(id)
		must(t, err)
		if si.Status != repo.StatusCommitted {
			t.Fatalf("snapshot %d status changed to %s", id, si.Status)
		}
	}
	all, err := e.Manifest.ListSnapshots()
	must(t, err)
	if len(all) != 4 { // v1, pending, failed, v2 — no new rows from diffing
		t.Fatalf("snapshot count=%d want 4", len(all))
	}

	// restore capability is intact (repair the externally corrupted blob first)
	must(t, os.Chmod(blobPath, 0o644))
	must(t, os.WriteFile(blobPath, orig, 0o444))
	if _, err := e.Restore(r1.SnapshotID, filepath.Join(dir, "out1")); err != nil {
		t.Fatalf("restore v1 after diff: %v", err)
	}
	if _, err := e.Restore(r2.SnapshotID, filepath.Join(dir, "out2")); err != nil {
		t.Fatalf("restore v2 after diff: %v", err)
	}
}

// An interrupted generation resumes from its persisted progress and repeated
// requests converge to the same finished report.
func TestDiffResumeAfterInterruption(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	for _, f := range []string{"f1", "f2", "f3", "f4"} {
		must(t, os.WriteFile(filepath.Join(src, f), []byte(strings.Repeat(f, 3000)), 0o644))
	}
	r1, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(src, "f5"), []byte("brand new\n"), 0o644))
	r2, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)

	// crash after the first chunk check
	e.Fail.DiffStopAfterChecks = 1
	rep, created, err := e.CreateDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	e.Fail.DiffStopAfterChecks = 0
	if !created || rep.Status != repo.DiffRunning {
		t.Fatalf("created=%v status=%s want running", created, rep.Status)
	}
	if rep.ProgressDone != 1 || rep.ProgressTotal < 4 {
		t.Fatalf("progress=%d/%d", rep.ProgressDone, rep.ProgressTotal)
	}

	// same request resumes the same report instead of starting over
	rep2, created2, err := e.CreateDiff(r1.SnapshotID, r2.SnapshotID)
	must(t, err)
	if created2 || rep2.ID != rep.ID {
		t.Fatalf("resume created=%v id=%d want %d", created2, rep2.ID, rep.ID)
	}
	if rep2.Status != repo.DiffDone || rep2.Integrity != repo.IntegrityComplete {
		t.Fatalf("after resume: status=%s integrity=%s", rep2.Status, rep2.Integrity)
	}
	if rep2.ProgressDone != rep2.ProgressTotal {
		t.Fatalf("progress=%d/%d", rep2.ProgressDone, rep2.ProgressTotal)
	}

	// startup-style recovery also finishes interrupted reports
	e.Fail.DiffStopAfterChecks = 1
	r3, err := e.CreateSnapshot(src, "v3", true)
	must(t, err)
	rep3, _, err := e.CreateDiff(r2.SnapshotID, r3.SnapshotID)
	must(t, err)
	e.Fail.DiffStopAfterChecks = 0
	if rep3.Status != repo.DiffRunning {
		t.Fatalf("status=%s want running", rep3.Status)
	}
	resumed, err := e.ResumeInterruptedDiffs()
	must(t, err)
	if len(resumed) != 1 || resumed[0].ID != rep3.ID || resumed[0].Status != repo.DiffDone {
		t.Fatalf("resumed=%+v", resumed)
	}
}
