package repo

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Diff report generation states. A report is only "done" after both the
// classification and the chunk-integrity verification phases completed.
const (
	DiffPending = "pending" // request stored, generation not started
	DiffRunning = "running" // generating, or crashed mid-run (resumable)
	DiffDone    = "done"    // classification + integrity verification finished
	DiffFailed  = "failed"  // generation failed; see error column, can be resumed
)

// Diff generation phases, persisted so an interrupted run continues where it
// stopped. The phase names the next step to execute.
const (
	DiffPhaseClassify = "classify"
	DiffPhaseVerify   = "verify"
	DiffPhaseFinalize = "finalize"
	DiffPhaseDone     = "done"
)

// Diff integrity verdicts. Incomplete means at least one participating file
// references a chunk that is missing or whose blob digest does not match;
// such a report must not be read as "everything unchanged".
const (
	IntegrityComplete   = "complete"
	IntegrityIncomplete = "incomplete"
)

// Diff item change types.
const (
	ChangeAdded          = "added"
	ChangeDeleted        = "deleted"
	ChangeContentChanged = "content_changed"
	ChangeMetaChanged    = "meta_changed"
	ChangeRenamed        = "renamed"
	ChangeAmbiguous      = "ambiguous_rename"
)

// ErrDiffNotFound marks a missing diff report.
var ErrDiffNotFound = errors.New("diff report not found")

// DiffReport is the persisted state of one snapshot-pair comparison.
type DiffReport struct {
	ID             int64
	BaseID         int64
	TargetID       int64
	Status         string
	Phase          string
	Integrity      string
	ProgressDone   int64
	ProgressTotal  int64
	Added          int64
	Deleted        int64
	ContentChanged int64
	MetaChanged    int64
	Renamed        int64
	Ambiguous      int64
	Unchanged      int64
	ChunksReused   int64 // distinct target chunks already referenced by base
	ChunksNew      int64 // distinct target chunks not referenced by base
	Error          string
	RequestedAt    time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

// DiffCounts aggregates item counts by classification.
type DiffCounts struct {
	Added          int64
	Deleted        int64
	ContentChanged int64
	MetaChanged    int64
	Renamed        int64
	Ambiguous      int64
	Unchanged      int64
}

// DiffItem is one classified change. Absent sides of one-sided items use
// -1 sizes/modes and nil digests.
type DiffItem struct {
	ID            int64
	ReportID      int64
	ChangeType    string
	Kind          string
	RelPath       string
	OldRelPath    string // renames: path in the base snapshot
	OldSize       int64
	NewSize       int64
	OldDigest     []byte
	NewDigest     []byte
	OldMode       int64
	NewMode       int64
	OldMtimeNS    int64
	NewMtimeNS    int64
	OldLinkTarget string
	NewLinkTarget string
	ChangedFields []string // metadata fields that also differ (mode/uid/gid/mtime)
	ChunksReused  int64    // of the new side's chunk sequence, already in base
	ChunksNew     int64    // of the new side's chunk sequence, not in base
	Candidates    []string // ambiguous_rename: possible counterpart paths
	ItemOrder     int
}

// DiffMissing names one file whose chunk failed integrity during compare.
type DiffMissing struct {
	ID          int64
	ReportID    int64
	SnapshotID  int64
	RelPath     string
	ChunkDigest []byte
	Reason      string
}

// DiffChunkRef is a distinct chunk referenced by a diff's snapshots.
type DiffChunkRef struct {
	Digest []byte
	Length int64 // -1 when the catalog row itself is missing
}

const diffCols = `id, base_id, target_id, status, phase, integrity,
	progress_done, progress_total,
	added_count, deleted_count, content_changed_count, meta_changed_count,
	renamed_count, ambiguous_count, unchanged_count,
	chunks_reused, chunks_new, error, requested_at, started_at, finished_at`

func scanDiffReport(row interface {
	Scan(...any) error
}) (DiffReport, error) {
	var r DiffReport
	var requested string
	var started, finished sql.NullString
	if err := row.Scan(&r.ID, &r.BaseID, &r.TargetID, &r.Status, &r.Phase,
		&r.Integrity, &r.ProgressDone, &r.ProgressTotal,
		&r.Added, &r.Deleted, &r.ContentChanged, &r.MetaChanged,
		&r.Renamed, &r.Ambiguous, &r.Unchanged,
		&r.ChunksReused, &r.ChunksNew, &r.Error,
		&requested, &started, &finished); err != nil {
		return r, err
	}
	r.RequestedAt, _ = time.Parse(time.RFC3339Nano, requested)
	if started.Valid {
		if t, err := time.Parse(time.RFC3339Nano, started.String); err == nil {
			r.StartedAt = &t
		}
	}
	if finished.Valid {
		if t, err := time.Parse(time.RFC3339Nano, finished.String); err == nil {
			r.FinishedAt = &t
		}
	}
	return r, nil
}

// GetOrCreateDiffReport returns the existing report for the exact snapshot
// pair, or creates a fresh pending one. The pair is unique: a repeated
// request always yields the same traceable report.
func (m *Manifest) GetOrCreateDiffReport(baseID, targetID int64) (DiffReport, bool, error) {
	rep, err := m.DiffReportByPair(baseID, targetID)
	if err == nil {
		return rep, false, nil
	}
	if !errors.Is(err, ErrDiffNotFound) {
		return rep, false, err
	}
	res, err := m.db.Exec(`INSERT INTO diff_reports
		(base_id, target_id, status, phase, requested_at)
		VALUES (?,?,?,?,?)`,
		baseID, targetID, DiffPending, DiffPhaseClassify,
		time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		// A concurrent creator won the unique-pair race: return its row.
		if rep2, err2 := m.DiffReportByPair(baseID, targetID); err2 == nil {
			return rep2, false, nil
		}
		return DiffReport{}, false, fmt.Errorf("create diff report: %w", err)
	}
	id, _ := res.LastInsertId()
	rep, err = m.GetDiffReport(id)
	return rep, true, err
}

// DiffReportByPair finds the report pinned to one snapshot pair.
func (m *Manifest) DiffReportByPair(baseID, targetID int64) (DiffReport, error) {
	row := m.db.QueryRow(`SELECT `+diffCols+` FROM diff_reports
		WHERE base_id = ? AND target_id = ?`, baseID, targetID)
	r, err := scanDiffReport(row)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrDiffNotFound
	}
	return r, err
}

// GetDiffReport fetches one report by id.
func (m *Manifest) GetDiffReport(id int64) (DiffReport, error) {
	row := m.db.QueryRow(`SELECT `+diffCols+` FROM diff_reports WHERE id = ?`, id)
	r, err := scanDiffReport(row)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrDiffNotFound
	}
	return r, err
}

// ListDiffReports returns all reports, newest first.
func (m *Manifest) ListDiffReports() ([]DiffReport, error) {
	rows, err := m.db.Query(`SELECT ` + diffCols + ` FROM diff_reports ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffReport
	for rows.Next() {
		r, err := scanDiffReport(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// InterruptedDiffReports returns reports whose generation never finished
// (process killed mid-run); they can be resumed.
func (m *Manifest) InterruptedDiffReports() ([]DiffReport, error) {
	rows, err := m.db.Query(`SELECT `+diffCols+` FROM diff_reports
		WHERE status IN (?, ?) ORDER BY id`, DiffPending, DiffRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffReport
	for rows.Next() {
		r, err := scanDiffReport(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkDiffRunning flips a report to running, keeping the first start time
// and clearing any previous failure message.
func (m *Manifest) MarkDiffRunning(id int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := m.db.Exec(`UPDATE diff_reports
		SET status = ?, started_at = COALESCE(started_at, ?), error = ''
		WHERE id = ? AND status != ?`, DiffRunning, now, id, DiffDone)
	return err
}

// FailDiff records a generation failure. The report stays inspectable and a
// later request for the same pair resumes from the persisted phase.
func (m *Manifest) FailDiff(id int64, cause string) error {
	_, err := m.db.Exec(`UPDATE diff_reports SET status = ?, error = ?, finished_at = ?
		WHERE id = ?`, DiffFailed, cause,
		time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

// SetDiffPhase advances the persisted phase marker.
func (m *Manifest) SetDiffPhase(id int64, phase string) error {
	_, err := m.db.Exec(`UPDATE diff_reports SET phase = ? WHERE id = ?`, phase, id)
	return err
}

// ReplaceDiffItems atomically stores the classification result: any previous
// items are replaced, counts and chunk statistics are updated, and the phase
// advances to verify. Chunk checks from an earlier attempt are kept (chunks
// are immutable, so their verdicts stay valid) and seed the progress count.
func (m *Manifest) ReplaceDiffItems(id int64, items []DiffItem, c DiffCounts, chunksReused, chunksNew, progressTotal int64) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM diff_items WHERE report_id = ?`, id); err != nil {
		return err
	}
	for i, it := range items {
		fields, err := json.Marshal(it.ChangedFields)
		if err != nil {
			return err
		}
		cands, err := json.Marshal(it.Candidates)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO diff_items
			(report_id, change_type, kind, rel_path, old_rel_path,
			 old_size, new_size, old_digest, new_digest, old_mode, new_mode,
			 old_mtime_ns, new_mtime_ns, old_link_target, new_link_target,
			 changed_fields, candidates, chunks_reused, chunks_new, item_order)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, it.ChangeType, it.Kind, it.RelPath, it.OldRelPath,
			it.OldSize, it.NewSize, it.OldDigest, it.NewDigest,
			it.OldMode, it.NewMode, it.OldMtimeNS, it.NewMtimeNS,
			it.OldLinkTarget, it.NewLinkTarget,
			string(fields), string(cands),
			it.ChunksReused, it.ChunksNew, i); err != nil {
			return fmt.Errorf("save diff item %q: %w", it.RelPath, err)
		}
	}
	if _, err := tx.Exec(`UPDATE diff_reports SET
		added_count = ?, deleted_count = ?, content_changed_count = ?,
		meta_changed_count = ?, renamed_count = ?, ambiguous_count = ?,
		unchanged_count = ?, chunks_reused = ?, chunks_new = ?,
		phase = ?, progress_total = ?,
		progress_done = (SELECT count(*) FROM diff_chunk_checks WHERE report_id = ?)
		WHERE id = ?`,
		c.Added, c.Deleted, c.ContentChanged, c.MetaChanged, c.Renamed,
		c.Ambiguous, c.Unchanged, chunksReused, chunksNew,
		DiffPhaseVerify, progressTotal, id, id); err != nil {
		return err
	}
	return tx.Commit()
}

// SaveDiffChunkCheck records one chunk-integrity verdict and bumps progress.
func (m *Manifest) SaveDiffChunkCheck(id int64, digest []byte, length int64, ok bool, reason string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	okInt := 0
	if ok {
		okInt = 1
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO diff_chunk_checks
		(report_id, chunk_digest, length, ok, reason) VALUES (?,?,?,?,?)`,
		id, digest, length, okInt, reason); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE diff_reports SET progress_done = progress_done + 1
		WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// DiffChunkCheckDigests returns the digests already checked for a report, so
// a resumed verify phase skips them.
func (m *Manifest) DiffChunkCheckDigests(id int64) (map[string]bool, error) {
	rows, err := m.db.Query(`SELECT chunk_digest FROM diff_chunk_checks WHERE report_id = ?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var d []byte
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out[string(d)] = true
	}
	return out, rows.Err()
}

// CountFailedDiffChecks returns how many chunk checks failed for a report.
func (m *Manifest) CountFailedDiffChecks(id int64) (int64, error) {
	var n int64
	err := m.db.QueryRow(`SELECT count(*) FROM diff_chunk_checks
		WHERE report_id = ? AND ok = 0`, id).Scan(&n)
	return n, err
}

// DiffChunksToVerify lists the distinct chunks referenced by either snapshot
// of the pair, with catalog lengths (-1 when the chunk row is missing).
func (m *Manifest) DiffChunksToVerify(baseID, targetID int64) ([]DiffChunkRef, error) {
	rows, err := m.db.Query(`SELECT ec.chunk_digest, c.length
		FROM entry_chunks ec LEFT JOIN chunks c ON c.digest = ec.chunk_digest
		WHERE ec.snapshot_id IN (?, ?)
		GROUP BY ec.chunk_digest ORDER BY ec.chunk_digest`, baseID, targetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffChunkRef
	for rows.Next() {
		var ref DiffChunkRef
		var length sql.NullInt64
		if err := rows.Scan(&ref.Digest, &length); err != nil {
			return nil, err
		}
		ref.Length = -1
		if length.Valid {
			ref.Length = length.Int64
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// FinalizeDiff completes a report: it derives the missing-path list from the
// failed chunk checks and stamps status/integrity. Idempotent.
func (m *Manifest) FinalizeDiff(id, baseID, targetID int64, integrity string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM diff_missing WHERE report_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO diff_missing
		(report_id, snapshot_id, rel_path, chunk_digest, reason)
		SELECT DISTINCT cc.report_id, ec.snapshot_id, ec.rel_path, ec.chunk_digest, cc.reason
		FROM diff_chunk_checks cc
		JOIN entry_chunks ec ON ec.chunk_digest = cc.chunk_digest
		WHERE cc.report_id = ? AND cc.ok = 0 AND ec.snapshot_id IN (?, ?)`,
		id, baseID, targetID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE diff_reports SET
		status = ?, phase = ?, integrity = ?, finished_at = ?,
		progress_done = (SELECT count(*) FROM diff_chunk_checks WHERE report_id = ?)
		WHERE id = ?`, DiffDone, DiffPhaseDone, integrity,
		time.Now().UTC().Format(time.RFC3339Nano), id, id); err != nil {
		return err
	}
	return tx.Commit()
}

// DiffItemsOf returns the classified items of a report in stable order.
// changeType, when non-empty, filters to one change type.
func (m *Manifest) DiffItemsOf(id int64, changeType string) ([]DiffItem, error) {
	q := `SELECT id, change_type, kind, rel_path, old_rel_path,
		old_size, new_size, old_digest, new_digest, old_mode, new_mode,
		old_mtime_ns, new_mtime_ns, old_link_target, new_link_target,
		changed_fields, candidates, chunks_reused, chunks_new, item_order
		FROM diff_items WHERE report_id = ?`
	args := []any{id}
	if changeType != "" {
		q += ` AND change_type = ?`
		args = append(args, changeType)
	}
	q += ` ORDER BY item_order`
	rows, err := m.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffItem
	for rows.Next() {
		var it DiffItem
		var fields, cands string
		if err := rows.Scan(&it.ID, &it.ChangeType, &it.Kind, &it.RelPath,
			&it.OldRelPath, &it.OldSize, &it.NewSize, &it.OldDigest, &it.NewDigest,
			&it.OldMode, &it.NewMode, &it.OldMtimeNS, &it.NewMtimeNS,
			&it.OldLinkTarget, &it.NewLinkTarget, &fields, &cands,
			&it.ChunksReused, &it.ChunksNew, &it.ItemOrder); err != nil {
			return nil, err
		}
		it.ReportID = id
		if err := json.Unmarshal([]byte(fields), &it.ChangedFields); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(cands), &it.Candidates); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// DiffMissingOf returns the integrity failures of a report.
func (m *Manifest) DiffMissingOf(id int64) ([]DiffMissing, error) {
	rows, err := m.db.Query(`SELECT id, snapshot_id, rel_path, chunk_digest, reason
		FROM diff_missing WHERE report_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiffMissing
	for rows.Next() {
		var dm DiffMissing
		if err := rows.Scan(&dm.ID, &dm.SnapshotID, &dm.RelPath, &dm.ChunkDigest, &dm.Reason); err != nil {
			return nil, err
		}
		dm.ReportID = id
		out = append(out, dm)
	}
	return out, rows.Err()
}
