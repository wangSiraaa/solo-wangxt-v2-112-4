// Package api exposes the backup engine over a small local HTTP API.
package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// Server wires the engine to HTTP.
type Server struct {
	Engine *backup.Engine
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
	mux.HandleFunc("GET /v1/diffs/{id}/missing", s.diffMissing)
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

// ---------- snapshot diffs ----------

type diffReq struct {
	BaseID   int64 `json:"base_id"`
	TargetID int64 `json:"target_id"`
	// StopAfterChecks is a failpoint for the demo/tests: interrupt chunk
	// verification after N checks, leaving a resumable "running" report.
	StopAfterChecks int `json:"stop_after_checks"`
}

type diffCountsResp struct {
	Added          int64 `json:"added"`
	Deleted        int64 `json:"deleted"`
	ContentChanged int64 `json:"content_changed"`
	MetaChanged    int64 `json:"meta_changed"`
	Renamed        int64 `json:"renamed"`
	Ambiguous      int64 `json:"ambiguous_rename"`
	Unchanged      int64 `json:"unchanged"`
}

type diffResp struct {
	ID           int64          `json:"id"`
	BaseID       int64          `json:"base_id"`
	TargetID     int64          `json:"target_id"`
	Status       string         `json:"status"`
	Phase        string         `json:"phase"`
	Integrity    string         `json:"integrity,omitempty"`
	Progress     diffProgress   `json:"progress"`
	Counts       diffCountsResp `json:"counts"`
	ChunksReused int64          `json:"chunks_reused"`
	ChunksNew    int64          `json:"chunks_new"`
	Error        string         `json:"error,omitempty"`
	RequestedAt  time.Time      `json:"requested_at"`
	StartedAt    *time.Time     `json:"started_at,omitempty"`
	FinishedAt   *time.Time     `json:"finished_at,omitempty"`
}

type diffProgress struct {
	Done  int64 `json:"done"`
	Total int64 `json:"total"`
}

func toDiffResp(r repo.DiffReport) diffResp {
	return diffResp{
		ID: r.ID, BaseID: r.BaseID, TargetID: r.TargetID,
		Status: r.Status, Phase: r.Phase, Integrity: r.Integrity,
		Progress: diffProgress{Done: r.ProgressDone, Total: r.ProgressTotal},
		Counts: diffCountsResp{
			Added: r.Added, Deleted: r.Deleted, ContentChanged: r.ContentChanged,
			MetaChanged: r.MetaChanged, Renamed: r.Renamed, Ambiguous: r.Ambiguous,
			Unchanged: r.Unchanged,
		},
		ChunksReused: r.ChunksReused, ChunksNew: r.ChunksNew,
		Error: r.Error, RequestedAt: r.RequestedAt,
		StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
	}
}

func (s *Server) createDiff(w http.ResponseWriter, r *http.Request) {
	var req diffReq
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
	}
	if req.BaseID <= 0 || req.TargetID <= 0 {
		writeErr(w, http.StatusBadRequest, "bad_request", "base_id and target_id are required", nil)
		return
	}
	if req.BaseID == req.TargetID {
		writeErr(w, http.StatusBadRequest, "bad_request", "base_id and target_id must be two different snapshots", nil)
		return
	}
	s.Engine.Fail.DiffStopAfterChecks = req.StopAfterChecks
	defer func() { s.Engine.Fail.DiffStopAfterChecks = 0 }()

	rep, created, err := s.Engine.CreateDiff(req.BaseID, req.TargetID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "base or target snapshot does not exist", nil)
			return
		}
		var rej *backup.ErrDiffRejected
		if errors.As(err, &rej) {
			writeErr(w, http.StatusConflict, "snapshot_not_committed", rej.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "diff_failed", err.Error(), nil)
		return
	}
	status := http.StatusOK // existing report returned as-is
	switch {
	case rep.Status != repo.DiffDone && rep.Status != repo.DiffFailed:
		status = http.StatusAccepted // interrupted mid-run; resumable
	case created:
		status = http.StatusCreated
	}
	writeJSON(w, status, toDiffResp(rep))
}

func (s *Server) listDiffs(w http.ResponseWriter, r *http.Request) {
	all, err := s.Engine.Manifest.ListDiffReports()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]diffResp, 0, len(all))
	for _, rep := range all {
		out = append(out, toDiffResp(rep))
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

// getDiffReportOr404 loads the report for the {id} path value.
func (s *Server) getDiffReportOr404(w http.ResponseWriter, r *http.Request) (repo.DiffReport, bool) {
	id, ok := parseDiffID(w, r)
	if !ok {
		return repo.DiffReport{}, false
	}
	rep, err := s.Engine.Manifest.GetDiffReport(id)
	if errors.Is(err, repo.ErrDiffNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "diff report does not exist", nil)
		return rep, false
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return rep, false
	}
	return rep, true
}

func (s *Server) getDiff(w http.ResponseWriter, r *http.Request) {
	rep, ok := s.getDiffReportOr404(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, toDiffResp(rep))
}

type diffSideResp struct {
	Size       int64     `json:"size"`
	Digest     string    `json:"digest,omitempty"`
	Mode       int64     `json:"mode"`
	Mtime      time.Time `json:"mtime"`
	LinkTarget string    `json:"link_target,omitempty"`
}

type diffItemResp struct {
	ChangeType    string        `json:"change_type"`
	Kind          string        `json:"kind"`
	RelPath       string        `json:"rel_path"`
	OldRelPath    string        `json:"old_rel_path,omitempty"`
	Old           *diffSideResp `json:"old,omitempty"`
	New           *diffSideResp `json:"new,omitempty"`
	ChangedFields []string      `json:"changed_fields,omitempty"`
	ChunksReused  int64         `json:"chunks_reused"`
	ChunksNew     int64         `json:"chunks_new"`
	Candidates    []string      `json:"candidates,omitempty"`
}

func toDiffItemResp(it repo.DiffItem) diffItemResp {
	out := diffItemResp{
		ChangeType: it.ChangeType, Kind: it.Kind,
		RelPath: it.RelPath, OldRelPath: it.OldRelPath,
		ChangedFields: it.ChangedFields, Candidates: it.Candidates,
		ChunksReused: it.ChunksReused, ChunksNew: it.ChunksNew,
	}
	if it.OldSize >= 0 {
		out.Old = &diffSideResp{
			Size: it.OldSize, Mode: it.OldMode,
			Mtime: time.Unix(0, it.OldMtimeNS).UTC(), LinkTarget: it.OldLinkTarget,
		}
		if len(it.OldDigest) > 0 {
			out.Old.Digest = hex.EncodeToString(it.OldDigest)
		}
	}
	if it.NewSize >= 0 {
		out.New = &diffSideResp{
			Size: it.NewSize, Mode: it.NewMode,
			Mtime: time.Unix(0, it.NewMtimeNS).UTC(), LinkTarget: it.NewLinkTarget,
		}
		if len(it.NewDigest) > 0 {
			out.New.Digest = hex.EncodeToString(it.NewDigest)
		}
	}
	return out
}

func toSlashPath(p string) string { return p }

var diffChangeTypes = map[string]bool{
	repo.ChangeAdded: true, repo.ChangeDeleted: true,
	repo.ChangeContentChanged: true, repo.ChangeMetaChanged: true,
	repo.ChangeRenamed: true, repo.ChangeAmbiguous: true,
}

func (s *Server) diffItems(w http.ResponseWriter, r *http.Request) {
	rep, ok := s.getDiffReportOr404(w, r)
	if !ok {
		return
	}
	filter := r.URL.Query().Get("type")
	if filter != "" && !diffChangeTypes[filter] {
		writeErr(w, http.StatusBadRequest, "bad_request",
			"unknown change type "+filter, nil)
		return
	}
	items, err := s.Engine.Manifest.DiffItemsOf(rep.ID, filter)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]diffItemResp, 0, len(items))
	for _, it := range items {
		out = append(out, toDiffItemResp(it))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"diff_id": rep.ID, "status": rep.Status, "integrity": rep.Integrity, "items": out,
	})
}

func (s *Server) diffMissing(w http.ResponseWriter, r *http.Request) {
	rep, ok := s.getDiffReportOr404(w, r)
	if !ok {
		return
	}
	missing, err := s.Engine.Manifest.DiffMissingOf(rep.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type item struct {
		SnapshotID  int64  `json:"snapshot_id"`
		RelPath     string `json:"rel_path"`
		ChunkDigest string `json:"chunk_digest"`
		Reason      string `json:"reason"`
	}
	out := make([]item, 0, len(missing))
	for _, dm := range missing {
		out = append(out, item{
			SnapshotID:  dm.SnapshotID,
			RelPath:     dm.RelPath,
			ChunkDigest: hex.EncodeToString(dm.ChunkDigest),
			Reason:      dm.Reason,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"diff_id": rep.ID, "status": rep.Status, "integrity": rep.Integrity, "missing": out,
	})
}
