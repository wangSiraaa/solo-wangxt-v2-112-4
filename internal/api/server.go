// Package api exposes the backup engine over a small local HTTP API.
package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// Server wires the engine to HTTP.
type Server struct {
	Engine *backup.Engine
	Diff   *backup.DiffService

	diffOnce sync.Once
	diffSvc  *backup.DiffService
}

// diffService returns the configured diff service, lazily creating one bound
// to the engine so existing callers that only set Engine keep working.
func (s *Server) diffService() *backup.DiffService {
	s.diffOnce.Do(func() {
		if s.Diff != nil {
			s.diffSvc = s.Diff
		} else {
			s.diffSvc = backup.NewDiffService(s.Engine)
		}
	})
	return s.diffSvc
}

// NewRouter builds the mux.
func (s *Server) NewRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/recover", s.recover)
	mux.HandleFunc("GET /v1/snapshots", s.list)
	mux.HandleFunc("POST /v1/snapshots", s.create)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.get)
	mux.HandleFunc("POST /v1/snapshots/{id}/verify", s.verify)
	mux.HandleFunc("GET /v1/snapshots/{id}/errors", s.listErrors)
	mux.HandleFunc("GET /v1/snapshots/{id}/missing", s.missing)
	mux.HandleFunc("POST /v1/snapshots/{id}/restore", s.restore)
	mux.HandleFunc("POST /v1/diffs", s.createDiff)
	mux.HandleFunc("GET /v1/diffs", s.listDiffs)
	mux.HandleFunc("GET /v1/diffs/{id}", s.getDiff)
	mux.HandleFunc("GET /v1/diffs/{id}/items", s.diffItems)
	mux.HandleFunc("GET /v1/diffs/{id}/problems", s.diffProblems)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string, details any) {
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": msg,
		"details": details,
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

type snapshotResp struct {
	ID          int64      `json:"id"`
	RootPath    string     `json:"root_path"`
	Status      string     `json:"status"`
	FileCount   int64      `json:"file_count"`
	DirCount    int64      `json:"dir_count"`
	BytesTotal  int64      `json:"bytes_total"`
	ChunksNew   int64      `json:"chunks_new"`
	ChunksRef   int64      `json:"chunks_referenced"`
	Polynomial  string     `json:"polynomial"`
	CreatedAt   time.Time  `json:"created_at"`
	CommittedAt *time.Time `json:"committed_at,omitempty"`
	Message     string     `json:"message"`
}

func toSnapshotResp(si repo.SnapshotInfo) snapshotResp {
	return snapshotResp{
		ID:          si.ID,
		RootPath:    si.RootPath,
		Status:      si.Status,
		FileCount:   si.FileCount,
		DirCount:    si.DirCount,
		BytesTotal:  si.BytesTotal,
		ChunksNew:   si.ChunksNew,
		ChunksRef:   si.ChunksRef,
		Polynomial:  "0x" + strconv.FormatUint(si.Polynomial, 16),
		CreatedAt:   si.CreatedAt,
		CommittedAt: si.CommittedAt,
		Message:     si.Message,
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	all, err := s.Engine.Manifest.ListSnapshots()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]snapshotResp, 0, len(all))
	for _, si := range all {
		out = append(out, toSnapshotResp(si))
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": out})
}

type createReq struct {
	Root          string `json:"root"`
	Message       string `json:"message"`
	Finish        *bool  `json:"finish"`      // default true; false = die before commit (demo)
	LoseChunks    int    `json:"lose_chunks"` // failpoint: delete N blobs pre-verify
	UnstableRetry int    `json:"-"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
	}
	if strings.TrimSpace(req.Root) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "root is required", nil)
		return
	}
	finish := true
	if req.Finish != nil {
		finish = *req.Finish
	}
	s.Engine.Fail.LoseChunkCount = req.LoseChunks
	defer func() { s.Engine.Fail.LoseChunkCount = 0 }()

	res, err := s.Engine.CreateSnapshot(req.Root, req.Message, finish)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": rej.SnapshotID,
				"status":      repo.StatusFailed,
				"error":       "snapshot_rejected",
				"reasons":     rej.Reasons,
				"hint":        "GET /v1/snapshots/" + strconv.FormatInt(rej.SnapshotID, 10) + "/missing",
			})
			return
		}
		status := http.StatusInternalServerError
		if res == nil {
			writeErr(w, status, "snapshot_failed", err.Error(), nil)
			return
		}
		writeJSON(w, status, map[string]any{
			"snapshot_id": res.SnapshotID,
			"status":      res.Status,
			"error":       err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id":       res.SnapshotID,
		"status":            res.Status,
		"chunks_new":        res.NewChunks,
		"chunks_referenced": res.RefChunks,
	})
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", "snapshot id must be an integer", nil)
		return 0, false
	}
	return id, true
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, toSnapshotResp(si))
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	res, err := s.Engine.VerifyAndFinalize(id)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": id,
				"status":      repo.StatusFailed,
				"error":       "verification_failed",
				"missing":     rej.Reasons,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, "verify_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot_id": id,
		"status":      res.Status,
	})
}

type missingResp struct {
	SnapshotID int64             `json:"snapshot_id"`
	Status     string            `json:"status"`
	Missing    []missingItemResp `json:"missing"`
}

type missingItemResp struct {
	RelPath  string `json:"rel_path"`
	Digest   string `json:"chunk_digest"`
	Length   int64  `json:"declared_length"`
	BlobPath string `json:"expected_blob_path"`
	Reason   string `json:"reason"`
}

func (s *Server) missing(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	found, err := s.Engine.Manifest.FindMissingChunks(id, func(digest []byte, length int64) (bool, error) {
		return s.Engine.Store.Has(digest, length)
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	items := make([]missingItemResp, 0, len(found))
	for _, mc := range found {
		item := missingItemResp{
			RelPath: mc.RelPath,
			Digest:  hex.EncodeToString(mc.Digest),
			Length:  mc.Length,
			Reason:  mc.Reason,
		}
		if p, err := s.Engine.Store.Path(mc.Digest); err == nil {
			item.BlobPath = p
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, missingResp{SnapshotID: id, Status: si.Status, Missing: items})
}

func (s *Server) listErrors(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	errs, err := s.Engine.Manifest.ListErrors(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type item struct {
		Stage       string    `json:"stage"`
		RelPath     string    `json:"rel_path"`
		ChunkDigest string    `json:"chunk_digest,omitempty"`
		Message     string    `json:"message"`
		CreatedAt   time.Time `json:"created_at"`
	}
	out := make([]item, 0, len(errs))
	for _, e := range errs {
		it := item{Stage: e.Stage, RelPath: e.RelPath, Message: e.Message, CreatedAt: e.CreatedAt}
		if len(e.ChunkDigest) > 0 {
			it.ChunkDigest = hex.EncodeToString(e.ChunkDigest)
		}
		out = append(out, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": id, "errors": out})
}

type restoreReq struct {
	Target string `json:"target"`
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req restoreReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	if strings.TrimSpace(req.Target) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "target is required", nil)
		return
	}
	res, err := s.Engine.Restore(id, req.Target)
	if err != nil {
		if errors.Is(err, backup.ErrTargetExists) {
			writeErr(w, http.StatusConflict, "target_exists", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "only committed snapshots") {
			writeErr(w, http.StatusConflict, "snapshot_not_committed", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "escapes restore root") {
			writeErr(w, http.StatusUnprocessableEntity, "unsafe_symlink", err.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "restore_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id": res.SnapshotID,
		"target":      res.Target,
		"files":       res.Files,
		"directories": res.Dirs,
		"symlinks":    res.Symlinks,
		"bytes":       res.Bytes,
		"verified":    res.Verified,
	})
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	out, err := s.Engine.RecoverPending()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "recover_failed", err.Error(), nil)
		return
	}
	ids := make([]map[string]any, 0, len(out))
	for _, r := range out {
		ids = append(ids, map[string]any{"snapshot_id": r.SnapshotID, "status": r.Status})
	}
	writeJSON(w, http.StatusOK, map[string]any{"recovered": ids})
}

// ---------- snapshot diff reports ----------

type diffReq struct {
	BaseSnapshotID   int64 `json:"base_snapshot_id"`
	TargetSnapshotID int64 `json:"target_snapshot_id"`
}

func diffJobJSON(j repo.DiffJob) map[string]any {
	out := map[string]any{
		"id":                 j.ID,
		"base_snapshot_id":   j.BaseSnapshotID,
		"target_snapshot_id": j.TargetSnapshot,
		"status":             j.Status,
		"complete":           j.Complete,
		"chunks_total":       j.ChunksTotal,
		"chunks_checked":     j.ChunksChecked,
		"chunks_reused":      j.ChunksReused,
		"chunks_new":         j.ChunksNew,
		"chunks_missing":     j.ChunksMissing,
		"chunks_mismatch":    j.ChunksMismatch,
		"affected_files":     j.AffectedFiles,
		"created_at":         j.CreatedAt,
		"updated_at":         j.UpdatedAt,
		"incomplete":         j.Complete && (j.ChunksMissing > 0 || j.ChunksMismatch > 0),
	}
	if j.Error != "" {
		out["error"] = j.Error
	}
	if j.StartedAt != nil {
		out["started_at"] = j.StartedAt
	}
	if j.CompleteAt != nil {
		out["complete_at"] = j.CompleteAt
	}
	return out
}

func (s *Server) createDiff(w http.ResponseWriter, r *http.Request) {
	var req diffReq
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
	}
	if req.BaseSnapshotID <= 0 || req.TargetSnapshotID <= 0 {
		writeErr(w, http.StatusBadRequest, "bad_request",
			"base_snapshot_id and target_snapshot_id are required", nil)
		return
	}
	j, err := s.diffService().EnsureDiff(req.BaseSnapshotID, req.TargetSnapshotID)
	if err != nil {
		var notCommitted *backup.ErrDiffNotCommitted
		if errors.As(err, &notCommitted) {
			writeErr(w, http.StatusConflict, "snapshot_not_committed", err.Error(), map[string]any{
				"snapshot_id": notCommitted.SnapshotID,
				"status":      notCommitted.Status,
			})
			return
		}
		if errors.Is(err, repo.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", err.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "diff_failed", err.Error(), nil)
		return
	}
	// Repeated request: same persisted, traceable report.
	writeJSON(w, http.StatusAccepted, diffJobJSON(j))
}

func (s *Server) listDiffs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.Engine.Manifest.ListDiffJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, diffJobJSON(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"diffs": out})
}

func parseDiffID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", "diff id must be an integer", nil)
		return 0, false
	}
	return id, true
}

func (s *Server) loadDiff(w http.ResponseWriter, id int64) (repo.DiffJob, bool) {
	j, err := s.Engine.Manifest.GetDiffJob(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "diff report does not exist", nil)
		return j, false
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return j, false
	}
	return j, true
}

func (s *Server) getDiff(w http.ResponseWriter, r *http.Request) {
	id, ok := parseDiffID(w, r)
	if !ok {
		return
	}
	j, ok := s.loadDiff(w, id)
	if !ok {
		return
	}
	resp := diffJobJSON(j)
	if j.Complete {
		counts, err := s.Engine.Manifest.DiffItemCounts(id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
			return
		}
		resp["counts"] = map[string]any{
			"added":            counts.Added,
			"deleted":          counts.Deleted,
			"changed":          counts.Changed,
			"metadata_changed": counts.Metadata,
			"unchanged":        counts.Unchanged,
			"renamed":          counts.Renamed,
			"ambiguous":        counts.Ambiguous,
			"unverified":       counts.Unverified,
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) diffItems(w http.ResponseWriter, r *http.Request) {
	id, ok := parseDiffID(w, r)
	if !ok {
		return
	}
	j, ok := s.loadDiff(w, id)
	if !ok {
		return
	}
	q := r.URL.Query()
	filter := repo.DiffItemFilter{ChangeType: q.Get("change_type")}
	limit, offset := 0, 0
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	items, total, err := s.Engine.Manifest.ListDiffItems(id, filter, limit, offset)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type itemResp struct {
		BasePath      string          `json:"base_path,omitempty"`
		TargetPath    string          `json:"target_path,omitempty"`
		Kind          string          `json:"kind"`
		ChangeType    string          `json:"change_type"`
		ChangedFields string          `json:"changed_fields,omitempty"`
		FileDigest    string          `json:"file_digest,omitempty"`
		ChunksReused  int64           `json:"chunks_reused"`
		ChunksNew     int64           `json:"chunks_new"`
		Integrity     string          `json:"integrity"`
		Detail        json.RawMessage `json:"detail"`
	}
	out := make([]itemResp, 0, len(items))
	for _, it := range items {
		detail := json.RawMessage("null")
		if it.Detail != "" {
			detail = json.RawMessage(it.Detail)
		}
		out = append(out, itemResp{
			BasePath: it.BasePath, TargetPath: it.TargetPath, Kind: it.EntryKind,
			ChangeType: it.ChangeType, ChangedFields: it.ChangedFields,
			FileDigest: it.FileDigest, ChunksReused: it.ChunksReused,
			ChunksNew: it.ChunksNew, Integrity: it.Integrity, Detail: detail,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"diff_id":    id,
		"status":     j.Status,
		"complete":   j.Complete,
		"incomplete": j.Complete && (j.ChunksMissing > 0 || j.ChunksMismatch > 0),
		"total":      total,
		"limit":      limit,
		"offset":     offset,
		"items":      out,
	})
}

func (s *Server) diffProblems(w http.ResponseWriter, r *http.Request) {
	id, ok := parseDiffID(w, r)
	if !ok {
		return
	}
	j, ok := s.loadDiff(w, id)
	if !ok {
		return
	}
	probs, err := s.Engine.Manifest.ListDiffProblems(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type probResp struct {
		SnapshotID     int64  `json:"snapshot_id"`
		RelPath        string `json:"rel_path"`
		Kind           string `json:"kind"`
		ChunkDigest    string `json:"chunk_digest"`
		DeclaredLength int64  `json:"declared_length"`
		ActualDigest   string `json:"actual_digest,omitempty"`
		Reason         string `json:"reason"`
	}
	out := make([]probResp, 0, len(probs))
	seen := map[string]bool{}
	for _, p := range probs {
		out = append(out, probResp{
			SnapshotID: p.SnapshotID, RelPath: p.RelPath, Kind: p.Kind,
			ChunkDigest: p.ChunkDigest, DeclaredLength: p.DeclaredLength,
			ActualDigest: p.ActualDigest, Reason: p.Reason,
		})
		seen[p.RelPath] = true
	}
	paths := make([]string, 0, len(seen))
	for p := range seen {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	writeJSON(w, http.StatusOK, map[string]any{
		"diff_id":        id,
		"status":         j.Status,
		"incomplete":     j.Complete && (j.ChunksMissing > 0 || j.ChunksMismatch > 0),
		"affected_paths": paths,
		"problems":       out,
	})
}
