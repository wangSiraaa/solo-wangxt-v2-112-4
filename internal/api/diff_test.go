package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

type harness struct {
	srv    *httptest.Server
	engine *backup.Engine
	dir    string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	m, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	store, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := backup.NewEngine(m, store)
	if err != nil {
		t.Fatal(err)
	}
	diffs := backup.NewDiffService(e)
	srv := httptest.NewServer((&api.Server{Engine: e, Diff: diffs}).NewRouter())
	t.Cleanup(srv.Close)
	return &harness{srv: srv, engine: e, dir: dir}
}

func (h *harness) do(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, h.srv.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("bad json %q: %v", string(data), err)
		}
	}
	return resp.StatusCode, out
}

func (h *harness) snapshot(t *testing.T, root, msg string, finish *bool) int64 {
	t.Helper()
	body := map[string]any{"root": root, "message": msg}
	if finish != nil {
		body["finish"] = *finish
	}
	code, b := h.do(t, "POST", "/v1/snapshots", body)
	if code >= 300 {
		t.Fatalf("create snapshot: %d %v", code, b)
	}
	return int64(b["snapshot_id"].(float64))
}

func waitStatus(t *testing.T, h *harness, id int64, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, b := h.do(t, "GET", "/v1/diffs/"+jsonNum(id), nil)
		if code != 200 {
			t.Fatalf("get diff: %d %v", code, b)
		}
		if b["status"] == want {
			return b
		}
		if b["status"] == "failed" {
			t.Fatalf("diff failed: %v", b)
		}
		if time.Now().After(deadline) {
			t.Fatalf("diff never reached %s: %v", want, b)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func jsonNum(i int64) string {
	b, _ := json.Marshal(i)
	return string(b)
}

func TestAPIDiffLifecycle(t *testing.T) {
	h := newHarness(t)
	src := filepath.Join(h.dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("0123456789ABCDEF\n", 12000) // ~180KiB -> many chunks
	if err := os.WriteFile(filepath.Join(src, "big.txt"), []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	id1 := h.snapshot(t, src, "base", nil)

	buf := []byte(big)
	copy(buf[90*1024:], []byte("PATCHED IN THE MIDDLE"))
	if err := os.WriteFile(filepath.Join(src, "big.txt"), buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "new.txt"), []byte("brand new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	id2 := h.snapshot(t, src, "target", nil)

	// create
	code, b := h.do(t, "POST", "/v1/diffs", map[string]any{
		"base_snapshot_id":   id1,
		"target_snapshot_id": id2,
	})
	if code != http.StatusAccepted {
		t.Fatalf("create = %d %v", code, b)
	}
	diffID := int64(b["id"].(float64))

	// duplicate request returns the same traceable report
	_, b2 := h.do(t, "POST", "/v1/diffs", map[string]any{
		"base_snapshot_id":   id1,
		"target_snapshot_id": id2,
	})
	if int64(b2["id"].(float64)) != diffID {
		t.Fatalf("duplicate request created a new report: %v", b2)
	}

	// progress -> complete with counts
	done := waitStatus(t, h, diffID, "complete")
	if done["incomplete"] != false {
		t.Fatalf("healthy report must not be incomplete: %v", done)
	}
	counts := done["counts"].(map[string]any)
	if counts["changed"].(float64) != 1 {
		t.Fatalf("counts: %v", counts)
	}
	if counts["added"].(float64) != 1 {
		t.Fatalf("counts: %v", counts)
	}
	if done["chunks_reused"].(float64) == 0 {
		t.Fatalf("reuse counters missing: %v", done)
	}

	// detail query
	code, items := h.do(t, "GET", "/v1/diffs/"+jsonNum(diffID)+"/items?change_type=changed", nil)
	if code != 200 {
		t.Fatalf("items: %d %v", code, items)
	}
	arr := items["items"].([]any)
	if len(arr) != 1 {
		t.Fatalf("want 1 changed item, got %d: %v", len(arr), items)
	}
	row := arr[0].(map[string]any)
	if row["target_path"] != "big.txt" || row["chunks_new"].(float64) == 0 {
		t.Fatalf("changed item: %v", row)
	}

	// problems endpoint on a healthy report is empty but present
	code, probs := h.do(t, "GET", "/v1/diffs/"+jsonNum(diffID)+"/problems", nil)
	if code != 200 || len(probs["problems"].([]any)) != 0 {
		t.Fatalf("problems: %d %v", code, probs)
	}

	// list endpoint
	_, list := h.do(t, "GET", "/v1/diffs", nil)
	if len(list["diffs"].([]any)) != 1 {
		t.Fatalf("list: %v", list)
	}
}

func TestAPIDiffRejectsPendingSnapshot(t *testing.T) {
	h := newHarness(t)
	src := filepath.Join(h.dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	good := h.snapshot(t, src, "good", nil)
	no := false
	pending := h.snapshot(t, src, "pending", &no)

	code, b := h.do(t, "POST", "/v1/diffs", map[string]any{
		"base_snapshot_id":   good,
		"target_snapshot_id": pending,
	})
	if code != http.StatusConflict {
		t.Fatalf("want 409, got %d: %v", code, b)
	}
	if b["error"] != "snapshot_not_committed" {
		t.Fatalf("error code: %v", b)
	}

	// missing snapshot -> 404
	code, b = h.do(t, "POST", "/v1/diffs", map[string]any{
		"base_snapshot_id":   good,
		"target_snapshot_id": 99999,
	})
	if code != http.StatusNotFound {
		t.Fatalf("want 404, got %d: %v", code, b)
	}
}

func TestAPIDiffIncompleteReportWithCorruptedBlob(t *testing.T) {
	h := newHarness(t)
	src := filepath.Join(h.dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "keep.txt"), []byte("kept\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	id1 := h.snapshot(t, src, "base", nil)
	if err := os.WriteFile(filepath.Join(src, "big.bin"),
		[]byte(strings.Repeat("z", 20000)), 0o644); err != nil {
		t.Fatal(err)
	}
	id2 := h.snapshot(t, src, "target", nil)

	// remove a singleton blob of snapshot 2 (catalog row remains -> missing)
	singletons, err := h.engine.Manifest.SingletonChunks(id2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(singletons) == 0 {
		t.Fatal("no singleton chunks")
	}
	if err := h.engine.Store.Remove(singletons[0].Digest); err != nil {
		t.Fatal(err)
	}

	code, b := h.do(t, "POST", "/v1/diffs", map[string]any{
		"base_snapshot_id":   id1,
		"target_snapshot_id": id2,
	})
	if code != http.StatusAccepted {
		t.Fatalf("create: %d %v", code, b)
	}
	diffID := int64(b["id"].(float64))
	done := waitStatus(t, h, diffID, "complete")
	if done["incomplete"] != true {
		t.Fatalf("missing-blob report must be incomplete: %v", done)
	}
	if done["chunks_missing"].(float64) == 0 {
		t.Fatalf("missing counter: %v", done)
	}

	_, probs := h.do(t, "GET", "/v1/diffs/"+jsonNum(diffID)+"/problems", nil)
	if len(probs["problems"].([]any)) == 0 {
		t.Fatalf("problems must list the bad reference: %v", probs)
	}
	paths := probs["affected_paths"].([]any)
	if len(paths) == 0 || paths[0] != "big.bin" {
		t.Fatalf("affected_paths: %v", paths)
	}

	// snapshots themselves were not modified
	for _, id := range []int64{id1, id2} {
		si, err := h.engine.Manifest.GetSnapshot(id)
		if err != nil {
			t.Fatal(err)
		}
		if si.Status != repo.StatusCommitted {
			t.Fatalf("snapshot %d altered to %s", id, si.Status)
		}
	}
}
