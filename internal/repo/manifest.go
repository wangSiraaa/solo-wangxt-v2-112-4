package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Snapshot status values. A snapshot is only "committed" after every
// referenced chunk has been proven present in the content store. Pending and
// failed snapshots are kept on purpose: they are what maintenance inspects to
// find out exactly which chunk is missing.
const (
	StatusPending   = "pending"   // scan done, not yet verified/finalized
	StatusCommitted = "committed" // all chunks verified live, usable for restore
	StatusFailed    = "failed"    // verification or commit failed; see snapshot_errors
)

// Manifest is the SQLite-backed backup catalog.
type Manifest struct {
	db *sql.DB
}

// OpenManifest opens or creates the manifest at path.
func OpenManifest(path string) (*Manifest, error) {
	// _txlock=immediate makes write transactions take a RESERVED lock up
	// front, avoiding SQLITE_BUSY under concurrent snapshots/restores.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // avoid lock churn; all operations are short
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	m := &Manifest{db: db}
	if err := m.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return m, nil
}

// Close releases the database handle.
func (m *Manifest) Close() error { return m.db.Close() }

// DB exposes the handle for package-internal repositories.
func (m *Manifest) DB() *sql.DB { return m.db }

const schemaSQL = `
CREATE TABLE IF NOT EXISTS snapshots (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	root_path   TEXT    NOT NULL,
	status      TEXT    NOT NULL,
	polynomial  INTEGER NOT NULL,
	file_count  INTEGER NOT NULL DEFAULT 0,
	dir_count   INTEGER NOT NULL DEFAULT 0,
	bytes_total INTEGER NOT NULL DEFAULT 0,
	chunks_new  INTEGER NOT NULL DEFAULT 0,
	chunks_ref  INTEGER NOT NULL DEFAULT 0,
	created_at  TEXT    NOT NULL,
	committed_at TEXT,
	message     TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS entries (
	snapshot_id     INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
	rel_path        TEXT    NOT NULL,
	kind            TEXT    NOT NULL,            -- 'file' | 'dir' | 'symlink'
	mode            INTEGER NOT NULL,            -- permission bits (os mode without type)
	uid             INTEGER NOT NULL DEFAULT -1,
	gid             INTEGER NOT NULL DEFAULT -1,
	mod_time_ns     INTEGER NOT NULL,
	size            INTEGER NOT NULL DEFAULT 0,
	file_digest     BLOB,                        -- whole-file SHA-256, files only
	link_target     TEXT    NOT NULL DEFAULT '', -- symlinks only
	entry_order     INTEGER NOT NULL,
	PRIMARY KEY (snapshot_id, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_entries_snap ON entries(snapshot_id, entry_order);

-- Chunks are global and content-addressed: the same digest is one row,
-- referenced by many entries across many snapshots.
CREATE TABLE IF NOT EXISTS chunks (
	digest      BLOB PRIMARY KEY,
	length      INTEGER NOT NULL,
	created_at  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS entry_chunks (
	snapshot_id  INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
	rel_path     TEXT    NOT NULL,
	chunk_digest BLOB    NOT NULL REFERENCES chunks(digest),
	seq          INTEGER NOT NULL,
	PRIMARY KEY (snapshot_id, rel_path, seq),
	FOREIGN KEY (snapshot_id, rel_path) REFERENCES entries(snapshot_id, rel_path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ec_digest ON entry_chunks(chunk_digest);

CREATE TABLE IF NOT EXISTS snapshot_errors (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
	stage       TEXT    NOT NULL,   -- scan | verify | commit
	rel_path    TEXT    NOT NULL DEFAULT '',
	chunk_digest BLOB,
	message     TEXT    NOT NULL,
	created_at  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_err_snap ON snapshot_errors(snapshot_id);

-- Repository-wide settings, notably the chunking polynomial. Content-defined
-- boundaries must be identical across snapshots (and restarts) for chunks of
-- unchanged byte ranges to hash the same.
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

-- Persisted diff comparisons. A (base_snapshot_id, target_snapshot_id) pair is
-- unique: repeating the same request always returns the same traceable report.
-- Jobs survive process restarts; status queued/running is recovered and
-- resumed. Diffing never mutates the two input snapshots.
CREATE TABLE IF NOT EXISTS diff_jobs (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	base_snapshot_id   INTEGER NOT NULL REFERENCES snapshots(id),
	target_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
	status           TEXT    NOT NULL,            -- queued | running | complete | failed
	complete         INTEGER NOT NULL DEFAULT 0, -- 1 once classification was stored
	complete_at      TEXT,
	error            TEXT    NOT NULL DEFAULT '',
	chunks_total     INTEGER NOT NULL DEFAULT 0, -- distinct blobs to verify across both snapshots
	chunks_checked   INTEGER NOT NULL DEFAULT 0, -- blobs whose live SHA-256 was verified
	chunks_reused    INTEGER NOT NULL DEFAULT 0, -- target file chunks also present in the base snapshot
	chunks_new       INTEGER NOT NULL DEFAULT 0, -- target file chunks absent from the base snapshot
	chunks_missing   INTEGER NOT NULL DEFAULT 0, -- missing catalog row / blob / wrong length
	chunks_mismatch  INTEGER NOT NULL DEFAULT 0, -- blob present but digest does not match
	affected_files   INTEGER NOT NULL DEFAULT 0, -- entries that participate with a bad reference
	created_at       TEXT    NOT NULL,
	started_at       TEXT,
	updated_at       TEXT    NOT NULL,
	UNIQUE (base_snapshot_id, target_snapshot_id)
);

-- One classification row per (job, base path, target path). Every entry of
-- either snapshot appears exactly once; change_type says what happened.
CREATE TABLE IF NOT EXISTS diff_items (
	job_id        INTEGER NOT NULL REFERENCES diff_jobs(id) ON DELETE CASCADE,
	base_path     TEXT    NOT NULL DEFAULT '',
	target_path   TEXT    NOT NULL DEFAULT '',
	entry_kind    TEXT    NOT NULL,                  -- file | dir | symlink
	change_type   TEXT    NOT NULL,                  -- added | deleted | changed | metadata_changed
	                                                 -- | unchanged | renamed | ambiguous | unverified
	changed_fields TEXT   NOT NULL DEFAULT '',       -- comma-separated: kind,mode,uid,gid,mtime,link_target
	file_digest   TEXT    NOT NULL DEFAULT '',       -- hex, target digest for content comparison
	chunks_reused INTEGER NOT NULL DEFAULT 0,
	chunks_new    INTEGER NOT NULL DEFAULT 0,
	integrity     TEXT    NOT NULL DEFAULT 'ok',     -- ok | affected (a referenced blob is bad)
	detail        TEXT    NOT NULL DEFAULT '',       -- JSON extras (e.g. ambiguous candidates)
	PRIMARY KEY (job_id, base_path, target_path)
);

-- Missing / digest-mismatched chunks discovered while preparing a diff. These
-- are diagnoses only: they never alter either input snapshot.
CREATE TABLE IF NOT EXISTS diff_problems (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id      INTEGER NOT NULL REFERENCES diff_jobs(id) ON DELETE CASCADE,
	snapshot_id INTEGER NOT NULL,
	rel_path    TEXT    NOT NULL DEFAULT '',
	kind        TEXT    NOT NULL,        -- missing | digest_mismatch
	chunk_digest TEXT   NOT NULL DEFAULT '',
	declared_length INTEGER NOT NULL DEFAULT 0,
	actual_digest   TEXT    NOT NULL DEFAULT '',
	reason      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_diffprob_job ON diff_problems(job_id);

-- Already-verified blobs of one diff job. Resuming after an interruption skips
-- these so verification is incremental and the whole blob set is read at most
-- once per job.
CREATE TABLE IF NOT EXISTS diff_chunk_checks (
	job_id     INTEGER NOT NULL REFERENCES diff_jobs(id) ON DELETE CASCADE,
	chunk_digest BLOB  NOT NULL,
	state      TEXT    NOT NULL,        -- ok | missing | digest_mismatch
	PRIMARY KEY (job_id, chunk_digest)
);
`

func (m *Manifest) migrate() error {
	_, err := m.db.Exec(schemaSQL)
	if err != nil {
		return fmt.Errorf("migrate manifest: %w", err)
	}
	return nil
}

// SnapshotInfo is the catalog view of one snapshot.
type SnapshotInfo struct {
	ID          int64
	RootPath    string
	Status      string
	Polynomial  uint64
	FileCount   int64
	DirCount    int64
	BytesTotal  int64
	ChunksNew   int64
	ChunksRef   int64
	CreatedAt   time.Time
	CommittedAt *time.Time
	Message     string
}

func scanSnapshot(row interface {
	Scan(...any) error
}) (SnapshotInfo, error) {
	var s SnapshotInfo
	var created, committed sql.NullString
	var poly int64
	if err := row.Scan(&s.ID, &s.RootPath, &s.Status, &poly, &s.FileCount,
		&s.DirCount, &s.BytesTotal, &s.ChunksNew, &s.ChunksRef,
		&created, &committed, &s.Message); err != nil {
		return s, err
	}
	s.Polynomial = uint64(poly)
	s.CreatedAt, _ = time.Parse(time.RFC3339Nano, created.String)
	if committed.Valid {
		t, err := time.Parse(time.RFC3339Nano, committed.String)
		if err == nil {
			s.CommittedAt = &t
		}
	}
	return s, nil
}

const snapshotCols = `id, root_path, status, polynomial, file_count, dir_count,
	bytes_total, chunks_new, chunks_ref, created_at, committed_at, message`

// ListSnapshots returns all snapshots, newest first.
func (m *Manifest) ListSnapshots() ([]SnapshotInfo, error) {
	rows, err := m.db.Query(`SELECT ` + snapshotCols + ` FROM snapshots ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotInfo
	for rows.Next() {
		s, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetSnapshot fetches one snapshot.
func (m *Manifest) GetSnapshot(id int64) (SnapshotInfo, error) {
	row := m.db.QueryRow(`SELECT `+snapshotCols+` FROM snapshots WHERE id = ?`, id)
	s, err := scanSnapshot(row)
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrNotFound
	}
	return s, err
}

// ErrNotFound marks a missing snapshot.
var ErrNotFound = errors.New("snapshot not found")

// SnapshotError is a recorded failure detail for a (possibly failed) snapshot.
type SnapshotError struct {
	ID          int64
	Stage       string
	RelPath     string
	ChunkDigest []byte
	Message     string
	CreatedAt   time.Time
}

// ListErrors returns every recorded error for a snapshot, oldest first.
func (m *Manifest) ListErrors(id int64) ([]SnapshotError, error) {
	rows, err := m.db.Query(`SELECT id, stage, rel_path, chunk_digest, message, created_at
		FROM snapshot_errors WHERE snapshot_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotError
	for rows.Next() {
		var e SnapshotError
		var created string
		if err := rows.Scan(&e.ID, &e.Stage, &e.RelPath, &e.ChunkDigest, &e.Message, &created); err != nil {
			return nil, err
		}
		e.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, e)
	}
	return out, rows.Err()
}
