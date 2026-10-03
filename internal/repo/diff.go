package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Diff job status values.
const (
	DiffQueued   = "queued"
	DiffRunning  = "running"
	DiffComplete = "complete"
	DiffFailed   = "failed"
)

// Integrity problem kinds recorded for a diff job.
const (
	DiffProblemMissing  = "missing"         // catalog row or on-disk blob absent, or length wrong
	DiffProblemMismatch = "digest_mismatch" // blob present, live SHA-256 disagrees
)

// Change classifications emitted in diff reports.
const (
	ChangeAdded      = "added"            // only in target
	ChangeDeleted    = "deleted"          // only in base
	ChangeContent    = "changed"          // same path, content/kind/link target changed
	ChangeMetadata   = "metadata_changed" // same identity, only mode/owner/mtime changed
	ChangeUnchanged  = "unchanged"
	ChangeRenamed    = "renamed"    // one-to-one moved file, identical digest
	ChangeAmbiguous  = "ambiguous"  // several same-digest candidates, no safe pairing
	ChangeUnverified = "unverified" // structurally classified, blob integrity unverifiable
)

// DiffJob is the persisted request/state of one snapshot comparison.
type DiffJob struct {
	ID             int64
	BaseSnapshotID int64
	TargetSnapshot int64
	Status         string
	Complete       bool
	Error          string
	ChunksTotal    int64
	ChunksChecked  int64
	ChunksReused   int64
	ChunksNew      int64
	ChunksMissing  int64
	ChunksMismatch int64
	AffectedFiles  int64
	CreatedAt      time.Time
	StartedAt      *time.Time
	CompleteAt     *time.Time
	UpdatedAt      time.Time
}

const diffJobCols = `id, base_snapshot_id, target_snapshot_id, status, complete,
	COALESCE(error,''), chunks_total, chunks_checked, chunks_reused, chunks_new,
	chunks_missing, chunks_mismatch, affected_files, created_at, started_at,
	complete_at, updated_at`

func scanDiffJob(row interface{ Scan(...any) error }) (DiffJob, error) {
	var j DiffJob
	var created, started, completed, updated sql.NullString
	var done int
	if err := row.Scan(&j.ID, &j.BaseSnapshotID, &j.TargetSnapshot, &j.Status, &done,
		&j.Error, &j.ChunksTotal, &j.ChunksChecked, &j.ChunksReused, &j.ChunksNew,
		&j.ChunksMissing, &j.ChunksMismatch, &j.AffectedFiles,
		&created, &started, &completed, &updated); err != nil {
		return j, err
	}
	j.Complete = done != 0
	j.CreatedAt, _ = time.Parse(time.RFC3339Nano, created.String)
	j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated.String)
	if started.Valid {
		if t, err := time.Parse(time.RFC3339Nano, started.String); err == nil {
			j.StartedAt = &t
		}
	}
	if completed.Valid {
		if t, err := time.Parse(time.RFC3339Nano, completed.String); err == nil {
			j.CompleteAt = &t
		}
	}
	return j, nil
}

// GetDiffJob fetches one diff job by id.
func (m *Manifest) GetDiffJob(id int64) (DiffJob, error) {
	row := m.db.QueryRow(`SELECT `+diffJobCols+` FROM diff_jobs WHERE id = ?`, id)
	j, err := scanDiffJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

// FindDiffJob returns the existing job for a fixed snapshot pair, if any.
func (m *Manifest) FindDiffJob(baseID, targetID int64) (DiffJob, bool, error) {
	row := m.db.QueryRow(`SELECT `+diffJobCols+`
		FROM diff_jobs WHERE base_snapshot_id = ? AND target_snapshot_id = ?`,
		baseID, targetID)
	j, err := scanDiffJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return DiffJob{}, false, nil
	}
	if err != nil {
		return DiffJob{}, false, err
	}
	return j, true, nil
}

// ListDiffJobs returns every diff request, newest first.
func (m *Manifest) ListDiffJobs() ([]DiffJob, error) {
	rows, err := m.db.Query(`SELECT ` + diffJobCols + ` FROM diff_jobs ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffJob
	for rows.Next() {
		j, err := scanDiffJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// CreateDiffJob inserts a queued job for the pair. A duplicate pair violates
// the unique constraint; call FindDiffJob first to return the same report.
func (m *Manifest) CreateDiffJob(baseID, targetID int64) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := m.db.Exec(`INSERT INTO diff_jobs
		(base_snapshot_id, target_snapshot_id, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)`, baseID, targetID, DiffQueued, now, now)
	if err != nil {
		return 0, fmt.Errorf("create diff job: %w", err)
	}
	return res.LastInsertId()
}

// MarkDiffRunning transitions queued (or a restarted running) job to running.
func (m *Manifest) MarkDiffRunning(id int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := m.db.Exec(`UPDATE diff_jobs
		SET status = ?, started_at = COALESCE(started_at, ?), updated_at = ?
		WHERE id = ?`, DiffRunning, now, now, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkDiffFailed records a fatal processing error. Input snapshots are never
// touched by diffing.
func (m *Manifest) MarkDiffFailed(id int64, msg string) error {
	_, err := m.db.Exec(`UPDATE diff_jobs SET status = ?, error = ?, updated_at = ?
		WHERE id = ?`, DiffFailed, msg, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

// DiffProgress is the mutable verification progress persisted while a job runs.
type DiffProgress struct {
	ChunksTotal   int64
	ChunksChecked int64
}

// UpdateDiffProgress persists liveness and verification progress so an
// interrupted run can tell what was already done.
func (m *Manifest) UpdateDiffProgress(id int64, p DiffProgress) error {
	_, err := m.db.Exec(`UPDATE diff_jobs
		SET chunks_total = ?, chunks_checked = ?, updated_at = ? WHERE id = ?`,
		p.ChunksTotal, p.ChunksChecked, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

// RunningDiffJobs lists jobs that were queued or mid-run (recovered on restart).
func (m *Manifest) RunningDiffJobs() ([]DiffJob, error) {
	rows, err := m.db.Query(`SELECT `+diffJobCols+`
		FROM diff_jobs WHERE status IN (?, ?) ORDER BY id`, DiffQueued, DiffRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffJob
	for rows.Next() {
		j, err := scanDiffJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ChunkCheckState returns the persisted verification verdict for a blob and
// whether it was checked in an earlier (possibly interrupted) run.
func (m *Manifest) ChunkCheckState(jobID int64, digest []byte) (state string, ok bool, err error) {
	var s string
	err = m.db.QueryRow(`SELECT state FROM diff_chunk_checks
		WHERE job_id = ? AND chunk_digest = ?`, jobID, digest).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return s, true, nil
}

// PutChunkCheck persists one blob verdict.
func (m *Manifest) PutChunkCheck(jobID int64, digest []byte, state string) error {
	_, err := m.db.Exec(`INSERT OR REPLACE INTO diff_chunk_checks
		(job_id, chunk_digest, state) VALUES (?, ?, ?)`, jobID, digest, state)
	return err
}

// DiffProblem is one unsatisfied/mismatched chunk reference found by a diff.
type DiffProblem struct {
	ID             int64
	JobID          int64
	SnapshotID     int64
	RelPath        string
	Kind           string
	ChunkDigest    string
	DeclaredLength int64
	ActualDigest   string
	Reason         string
}

// ListDiffProblems returns the integrity problems of a job, deterministic order.
func (m *Manifest) ListDiffProblems(jobID int64) ([]DiffProblem, error) {
	rows, err := m.db.Query(`SELECT id, job_id, snapshot_id, rel_path, kind,
		chunk_digest, declared_length, COALESCE(actual_digest,''), reason
		FROM diff_problems WHERE job_id = ?
		ORDER BY snapshot_id, rel_path, chunk_digest`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffProblem
	for rows.Next() {
		var p DiffProblem
		if err := rows.Scan(&p.ID, &p.JobID, &p.SnapshotID, &p.RelPath, &p.Kind,
			&p.ChunkDigest, &p.DeclaredLength, &p.ActualDigest, &p.Reason); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DiffItem is one classified entry of a finished report.
type DiffItem struct {
	BasePath      string
	TargetPath    string
	EntryKind     string
	ChangeType    string
	ChangedFields string
	FileDigest    string
	ChunksReused  int64
	ChunksNew     int64
	Integrity     string
	Detail        string // JSON extras (e.g. ambiguous candidate paths)
}

// DiffItemFilter narrows a detail query.
type DiffItemFilter struct {
	ChangeType string // "" = every classification
}

// ListDiffItems returns classified entries; per-item counts let the caller
// paginate without re-running the comparison.
func (m *Manifest) ListDiffItems(jobID int64, filter DiffItemFilter, limit, offset int) ([]DiffItem, int64, error) {
	var total int64
	countQ := `SELECT count(*) FROM diff_items WHERE job_id = ?`
	itemQ := `SELECT base_path, target_path, entry_kind, change_type,
		COALESCE(changed_fields,''), COALESCE(file_digest,''),
		chunks_reused, chunks_new, integrity, COALESCE(detail,'')
		FROM diff_items WHERE job_id = ?`
	args := []any{jobID}
	if filter.ChangeType != "" {
		countQ += ` AND change_type = ?`
		itemQ += ` AND change_type = ?`
		args = append(args, filter.ChangeType)
	}
	if err := m.db.QueryRow(countQ, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	itemQ += ` ORDER BY
		CASE change_type
			WHEN 'unverified' THEN 0 WHEN 'changed' THEN 1 WHEN 'ambiguous' THEN 2
			WHEN 'renamed' THEN 3 WHEN 'metadata_changed' THEN 4
			WHEN 'added' THEN 5 WHEN 'deleted' THEN 6 ELSE 7 END,
		target_path, base_path`
	if limit > 0 {
		itemQ += ` LIMIT ? OFFSET ?`
		args = append(args, limit, offset)
	}
	rows, err := m.db.Query(itemQ, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []DiffItem
	for rows.Next() {
		var it DiffItem
		if err := rows.Scan(&it.BasePath, &it.TargetPath, &it.EntryKind, &it.ChangeType,
			&it.ChangedFields, &it.FileDigest, &it.ChunksReused, &it.ChunksNew,
			&it.Integrity, &it.Detail); err != nil {
			return nil, 0, err
		}
		out = append(out, it)
	}
	return out, total, rows.Err()
}

// DiffItemCounts groups items by classification for the progress response.
type DiffItemCounts struct {
	Added      int64
	Deleted    int64
	Changed    int64
	Metadata   int64
	Unchanged  int64
	Renamed    int64
	Ambiguous  int64
	Unverified int64
}

func (m *Manifest) DiffItemCounts(jobID int64) (DiffItemCounts, error) {
	var c DiffItemCounts
	rows, err := m.db.Query(`SELECT change_type, count(*) FROM diff_items
		WHERE job_id = ? GROUP BY change_type`, jobID)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		var n int64
		if err := rows.Scan(&t, &n); err != nil {
			return c, err
		}
		switch t {
		case ChangeAdded:
			c.Added = n
		case ChangeDeleted:
			c.Deleted = n
		case ChangeContent:
			c.Changed = n
		case ChangeMetadata:
			c.Metadata = n
		case ChangeUnchanged:
			c.Unchanged = n
		case ChangeRenamed:
			c.Renamed = n
		case ChangeAmbiguous:
			c.Ambiguous = n
		case ChangeUnverified:
			c.Unverified = n
		}
	}
	return c, rows.Err()
}

// SaveDiffResult replaces (idempotently, under restart) the full result of a
// job: items, problems, counters and the complete marker, in one transaction.
// Nothing here writes to snapshots/entries/chunks of the inputs.
func (m *Manifest) SaveDiffResult(jobID int64, items []DiffItem, problems []DiffProblemInput,
	counts DiffResultCounts) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM diff_items WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM diff_problems WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	for _, it := range items {
		if _, err := tx.Exec(`INSERT INTO diff_items
			(job_id, base_path, target_path, entry_kind, change_type, changed_fields,
			 file_digest, chunks_reused, chunks_new, integrity, detail)
			VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			jobID, it.BasePath, it.TargetPath, it.EntryKind, it.ChangeType,
			it.ChangedFields, it.FileDigest, it.ChunksReused, it.ChunksNew,
			it.Integrity, it.Detail); err != nil {
			return fmt.Errorf("save diff item: %w", err)
		}
	}
	for _, p := range problems {
		if _, err := tx.Exec(`INSERT INTO diff_problems
			(job_id, snapshot_id, rel_path, kind, chunk_digest, declared_length,
			 actual_digest, reason) VALUES (?,?,?,?,?,?,?,?)`,
			jobID, p.SnapshotID, p.RelPath, p.Kind, p.ChunkDigest,
			p.DeclaredLength, p.ActualDigest, p.Reason); err != nil {
			return fmt.Errorf("save diff problem: %w", err)
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.Exec(`UPDATE diff_jobs SET
		status = ?, complete = 1, complete_at = ?, error = '',
		chunks_reused = ?, chunks_new = ?, chunks_missing = ?,
		chunks_mismatch = ?, affected_files = ?, updated_at = ?
		WHERE id = ?`,
		DiffComplete, now, counts.Reused, counts.New, counts.Missing,
		counts.Mismatch, counts.AffectedFiles, now, jobID); err != nil {
		return err
	}
	return tx.Commit()
}

// DiffResultCounts are the aggregate counters stored with a finished report.
type DiffResultCounts struct {
	Reused        int64
	New           int64
	Missing       int64
	Mismatch      int64
	AffectedFiles int64
}

// DiffProblemInput is one integrity problem to persist with the report.
type DiffProblemInput struct {
	SnapshotID     int64
	RelPath        string
	Kind           string
	ChunkDigest    string
	DeclaredLength int64
	ActualDigest   string
	Reason         string
}
