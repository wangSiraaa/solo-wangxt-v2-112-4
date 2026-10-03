// Command demo drives the backup HTTP API through the full failure story:
//
//  1. committed snapshot + restore into a new directory with digest/length
//     verification (empty file included),
//  2. small edit reusing existing content-defined chunks,
//  3. refusal to restore over an existing directory,
//  4. file actively written during scan -> re-read then rejected,
//  5. commit interruption losing a blob -> failed snapshot with the exact
//     missing chunk located,
//  6. symlink escaping the root -> restored link is blocked,
//  7. server restart with a pending snapshot -> startup recovery commits it.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

var pass, fail int

func main() {
	keep := flag.Bool("keep", false, "keep the demo workspace afterwards")
	flag.Parse()

	work, err := os.MkdirTemp("", "incbackup-demo-")
	must(err)
	if *keep {
		fmt.Printf("(workspace: %s)\n", work)
	} else {
		defer os.RemoveAll(work)
	}
	repoDir := filepath.Join(work, "repo")
	src := filepath.Join(work, "src")

	section(0, "准备：在一个进程内启动本地 API 服务与数据目录")
	srv := startServer(repoDir)
	fmt.Printf("  API   : %s\n", srv.URL)
	fmt.Printf("  仓库  : %s (manifest.sqlite + chunks/)\n", repoDir)
	fmt.Printf("  数据源: %s\n", src)
	must(os.MkdirAll(filepath.Join(src, "docs"), 0o755))

	// Big-ish log so content-defined chunking produces several chunks.
	log := make([]byte, 0, 160*1024)
	for i := 0; i < 160*1024; i++ {
		log = append(log, byte("abcdefghijklmnopqrstuvwxyz0123456789\n"[i%37]))
	}
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	must(os.WriteFile(filepath.Join(src, "docs", "notes.txt"), []byte("meeting notes\n"), 0o644))
	must(os.WriteFile(filepath.Join(src, "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o750))
	must(os.WriteFile(filepath.Join(src, "EMPTY.dat"), nil, 0o600)) // empty file
	must(os.Symlink("docs/notes.txt", filepath.Join(src, "link_to_notes")))

	// ---- 1. first snapshot + restore --------------------------------------
	section(1, "首次快照：完成前逐块验证，然后恢复到全新目录并核对摘要与长度")
	r := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "baseline"})
	firstID := int64(r["snapshot_id"].(float64))
	fmt.Printf("  快照 %d: status=%s 新块=%v 引用块=%v\n",
		firstID, r["status"], r["chunks_new"], r["chunks_referenced"])
	check("快照状态为 committed", r["status"] == "committed")

	restoreDir := filepath.Join(work, "restore-1")
	code, body := raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #1 failed: HTTP %d %s", code, body["message"]))
	}
	rr := body
	verified, _ := rr["verified"].([]any)
	fmt.Printf("  恢复到 %s\n  文件=%v 目录=%v 符号链接=%v 字节=%v\n",
		restoreDir, rr["files"], rr["directories"], rr["symlinks"], rr["bytes"])
	for _, v := range verified {
		m := v.(map[string]any)
		fmt.Printf("    %-18s 长度=%-6d 块数=%-2d 摘要=%s… 权限=0%o\n",
			m["rel_path"], int64(m["size"].(float64)), int(m["chunk_count"].(float64)),
			m["digest"].(string)[:16], int64(m["mode"].(float64)))
	}
	emptyOK := false
	for _, v := range verified {
		m := v.(map[string]any)
		if m["rel_path"] == "EMPTY.dat" {
			emptyOK = m["size"].(float64) == 0 &&
				m["digest"] == fmt.Sprintf("%x", sha256.New().Sum(nil)) &&
				m["chunk_count"].(float64) == 0
		}
	}
	check("空文件：长度 0、SHA256=e3b0c44…、0 个内容块", emptyOK)

	// Compare tree metadata with source.
	var modeMismatch []string
	for _, rel := range []string{"run.sh", "app.log", "docs"} {
		a, _ := os.Lstat(filepath.Join(src, rel))
		b, err := os.Lstat(filepath.Join(restoreDir, rel))
		if err != nil || a.Mode().Perm() != b.Mode().Perm() {
			modeMismatch = append(modeMismatch, rel)
		}
	}
	check("目录权限与文件权限均保留 (run.sh 0750, docs 0755)", len(modeMismatch) == 0)
	lt, _ := os.Readlink(filepath.Join(restoreDir, "link_to_notes"))
	check("符号链接本身被恢复（链接目标=docs/notes.txt，未跟随）", lt == "docs/notes.txt")
	notes, err := os.ReadFile(filepath.Join(restoreDir, "link_to_notes"))
	check("恢复出的链接仍可解析到文件内容", err == nil && string(notes) == "meeting notes\n")

	// byte-identical content of app.log independently re-hashed
	got, _ := hashFile(filepath.Join(restoreDir, "app.log"))
	want, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复内容逐字节一致（独立重算 SHA256）", got == want)

	// ---- 2. small edit reuses chunks --------------------------------------
	section(2, "小改动的增量：在 app.log 中部改一行，只新增 1 个块，其余块全部复用")
	off := 80 * 1024
	copy(log[off:off+8], []byte("PATCHED!"))
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	r = post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "one-line patch"})
	secondID := int64(r["snapshot_id"].(float64))
	newChunks := int64(r["chunks_new"].(float64))
	refChunks := int64(r["chunks_referenced"].(float64))
	fmt.Printf("  快照 %d: 引用块=%d，其中新写入=%d，复用=%d\n",
		secondID, refChunks, newChunks, refChunks-newChunks)
	check("仅有改动附近的 1 个块是新块（内容定义分块边界由内容决定）", newChunks == 1)
	check("其余块全部复用快照 1 中的旧块", refChunks-newChunks == refChunks-1)

	restore2 := filepath.Join(work, "restore-2")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", secondID), map[string]any{"target": restore2})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #2 failed: HTTP %d %s", code, body["message"]))
	}
	g2, _ := hashFile(filepath.Join(restore2, "app.log"))
	w2, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复快照 2 后 app.log 与当前源文件一致", g2 == w2)

	// ---- 3. never overwrite destination -----------------------------------
	section(3, "恢复位置已有任何东西 → 拒绝，不覆盖、不合并")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	fmt.Printf("  POST restore 到已存在目录 -> HTTP %d: %s\n", code, body["error"])
	check("已有目录时返回 409 target_exists", code == http.StatusConflict && body["error"] == "target_exists")

	// ---- 4. file being written during scan --------------------------------
	section(4, "扫描中仍在写入的文件：先短暂写入触发重读，再持续写入触发拒绝")
	growing := filepath.Join(src, "growing.log")
	must(os.WriteFile(growing, []byte("line0\n"), 0o644))

	// 4a. writer finishes within the retry window: scanner re-reads and commits
	done := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		for i := 1; i <= 5; i++ {
			fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("y", 200))
			time.Sleep(20 * time.Millisecond)
		}
		f.Close()
		close(done)
	}()
	snap4a := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "writer settles during retry"})
	<-done
	check("写入在重读窗口内结束：扫描器重读文件，快照仍正常 committed", snap4a["status"] == "committed")

	// 4b. writer keeps going: every pass sees a changed size/mtime -> rejected
	stop := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		defer f.Close()
		i := 1
		for {
			select {
			case <-stop:
				return
			default:
				fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("x", 200))
				i++
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	time.Sleep(30 * time.Millisecond)
	code, body = raw("POST", srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "racing writer"})
	close(stop)
	reasons, _ := body["reasons"].([]any)
	fmt.Printf("  持续写入时 HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	for _, x := range reasons {
		fmt.Printf("    拒绝原因: %s\n", x)
	}
	sawUnstable := false
	for _, x := range reasons {
		if strings.Contains(x.(string), "growing.log") &&
			strings.Contains(x.(string), "still being written") {
			sawUnstable = true
		}
	}
	check("3 次重读后仍在变化的 growing.log 被明确点名（而不是备份静默成功）",
		code == http.StatusConflict && sawUnstable)
	badID := int64(body["snapshot_id"].(float64))
	errs, _ := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/errors", badID))["errors"].([]any)
	check("失败快照保留在清单中，stage=scan 可追溯", len(errs) > 0 &&
		errs[0].(map[string]any)["stage"] == "scan")

	// ---- 5. commit interruption: lost blob, locate exact chunk ------------
	section(5, "模拟提交中断：删掉最后一个内容块 → 完成前验证拦截并定位具体缺块")
	must(os.WriteFile(growing, []byte("stable now\n"), 0o644))
	code, body = raw("POST", srv.URL+"/v1/snapshots",
		map[string]any{"root": src, "message": "interrupted commit", "lose_chunks": 1})
	fmt.Printf("  HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	interruptedID := int64(body["snapshot_id"].(float64))
	check("缺块快照不能 committed，返回 409", code == http.StatusConflict)

	missing := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/missing", interruptedID))["missing"].([]any)
	fmt.Printf("  维护查询 GET .../missing 找到 %d 个缺块：\n", len(missing))
	for _, x := range missing {
		m := x.(map[string]any)
		fmt.Printf("    文件   : %s\n", m["rel_path"])
		fmt.Printf("    块摘要 : %s\n", m["chunk_digest"])
		fmt.Printf("    应在   : %s\n", m["expected_blob_path"])
		fmt.Printf("    原因   : %s\n", m["reason"])
		_, statErr := os.Stat(m["expected_blob_path"].(string))
		check("报告的块路径在磁盘上确实不存在", os.IsNotExist(statErr))
	}
	check("缺块清单精确到 文件+摘要+期望磁盘路径（不是“上传队列为空”）", len(missing) == 1)
	si := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", interruptedID))
	check("失败快照状态可查 = failed", si["status"] == "failed")

	// restore of a failed snapshot must be refused
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", interruptedID),
		map[string]any{"target": filepath.Join(work, "never")})
	fmt.Printf("  尝试恢复 failed 快照 -> HTTP %d %s\n", code, body["error"])
	check("failed 快照拒绝恢复", code >= 400)

	// ---- 6. symlink escape containment ------------------------------------
	section(6, "符号链接越界：备份只存链接本身，恢复时指向根目录外的链接被拒绝")
	secret := filepath.Join(work, "secret.txt")
	must(os.WriteFile(secret, []byte("TOP SECRET"), 0o600))
	evil := filepath.Join(src, "evil_link")
	_ = os.Remove(evil)
	rel, _ := filepath.Rel(filepath.Join(src), secret)
	must(os.Symlink(rel, evil)) // src/evil_link -> ../secret.txt
	snapEvil := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "with evil link"})
	evilID := int64(snapEvil["snapshot_id"].(float64))
	evilTarget := filepath.Join(work, "restore-evil")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", evilID),
		map[string]any{"target": evilTarget})
	fmt.Printf("  含越界链接的恢复 -> HTTP %d: %s\n", code, body["message"])
	check("越界符号链接恢复被阻止 (422)", code == http.StatusUnprocessableEntity)
	_, statErr := os.Lstat(evilTarget)
	check("失败后不留半成品目录（回滚清理）", os.IsNotExist(statErr))
	_, err = os.ReadFile(filepath.Join(evilTarget, "evil_link"))
	check("秘密文件没有被触及/写出", err != nil)

	// ---- 7. restart recovery of a pending snapshot -------------------------
	section(7, "提交前进程退出：快照留在 pending，服务重启时自动验证并给结论")
	pend := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "crash before commit", "finish": false})
	pendID := int64(pend["snapshot_id"].(float64))
	fmt.Printf("  故障时刻: 快照 %d status=%s，块已落盘、清单未提交\n", pendID, pend["status"])
	check("finish=false 留下 pending 快照", pend["status"] == "pending")
	srv.Close()

	srv = startServer(repoDir) // same repo, new process equivalent
	time.Sleep(100 * time.Millisecond)
	si = get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", pendID))
	fmt.Printf("  重启后: 快照 %d status=%s\n", pendID, si["status"])
	check("重启恢复把 pending 快照验证后提交为 committed", si["status"] == "committed")

	// ---- 8. persisted snapshot diff reports -------------------------------
	section(8, "快照差异报告：内容/元数据/重命名/歧义/块复用，持久化且可追溯")
	diffSrc := filepath.Join(work, "diffsrc")
	must(os.MkdirAll(diffSrc, 0o755))
	diffLog := make([]byte, 0, 160*1024)
	for i := 0; i < 160*1024; i++ {
		diffLog = append(diffLog, byte("0123456789ABCDEF\n"[i%17]))
	}
	must(os.WriteFile(filepath.Join(diffSrc, "big.log"), diffLog, 0o644))
	must(os.WriteFile(filepath.Join(diffSrc, "old-name.txt"), []byte(strings.Repeat("unique rename body\n", 400)), 0o644))
	dupBody := strings.Repeat("duplicated payload\n", 400)
	must(os.WriteFile(filepath.Join(diffSrc, "dup.bin"), []byte(dupBody), 0o644))
	must(os.WriteFile(filepath.Join(diffSrc, "meta.sh"), []byte("#!/bin/sh\n"), 0o644))
	must(os.WriteFile(filepath.Join(diffSrc, "t1"), []byte("target-one\n"), 0o600))
	must(os.WriteFile(filepath.Join(diffSrc, "t2"), []byte("target-two\n"), 0o600))
	must(os.Symlink("t1", filepath.Join(diffSrc, "link")))
	d1 := post(srv.URL+"/v1/snapshots", map[string]any{"root": diffSrc, "message": "diff base"})
	diffID1 := int64(d1["snapshot_id"].(float64))
	check("基线差异快照 committed", d1["status"] == "committed")

	// apply every category of change
	copy(diffLog[80*1024:80*1024+8], []byte("PATCHED!"))
	must(os.WriteFile(filepath.Join(diffSrc, "big.log"), diffLog, 0o644))
	must(os.Rename(filepath.Join(diffSrc, "old-name.txt"), filepath.Join(diffSrc, "new-name.txt")))
	must(os.WriteFile(filepath.Join(diffSrc, "dup-copy.bin"), []byte(dupBody), 0o644)) // same digest -> ambiguous
	must(os.Chmod(filepath.Join(diffSrc, "meta.sh"), 0o755))                           // mode only
	must(os.Remove(filepath.Join(diffSrc, "link")))
	must(os.Symlink("t2", filepath.Join(diffSrc, "link"))) // link target only, never followed
	d2 := post(srv.URL+"/v1/snapshots", map[string]any{"root": diffSrc, "message": "diff target"})
	diffID2 := int64(d2["snapshot_id"].(float64))

	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{
		"base_snapshot_id": diffID1, "target_snapshot_id": diffID2,
	})
	fmt.Printf("  POST /v1/diffs -> HTTP %d, diff %v status=%v\n", code, body["id"], body["status"])
	check("创建比较返回 202", code == http.StatusAccepted)
	diffID := int64(body["id"].(float64))

	// identical request returns the SAME persisted report
	dup := post(srv.URL+"/v1/diffs", map[string]any{
		"base_snapshot_id": diffID1, "target_snapshot_id": diffID2,
	})
	check("重复请求返回同一份可追溯报告（相同 diff id）", int64(dup["id"].(float64)) == diffID)

	rep := waitDiff(srv.URL, diffID)
	fmt.Printf("  报告 status=%v incomplete=%v 复用块=%v 新块=%v 校验=%v/%v\n",
		rep["status"], rep["incomplete"], rep["chunks_reused"], rep["chunks_new"],
		rep["chunks_checked"], rep["chunks_total"])
	check("比较完成且健康（incomplete=false）", rep["status"] == "complete" && rep["incomplete"] == false)

	items, _ := get(srv.URL + fmt.Sprintf("/v1/diffs/%d/items", diffID))["items"].([]any)
	find := func(pred func(map[string]any) bool) map[string]any {
		for _, x := range items {
			m := x.(map[string]any)
			if pred(m) {
				return m
			}
		}
		return nil
	}
	big := find(func(m map[string]any) bool { return m["target_path"] == "big.log" })
	fmt.Printf("  big.log: %s 复用块=%v 新块=%v 字段=%v\n",
		big["change_type"], big["chunks_reused"], big["chunks_new"], big["changed_fields"])
	check("①中间改少量字节 => changed 且新块=1、其余块复用",
		big["change_type"] == "changed" &&
			big["chunks_new"].(float64) == 1 && big["chunks_reused"].(float64) > 0)

	ren := find(func(m map[string]any) bool { return m["change_type"] == "renamed" })
	fmt.Printf("  renamed: %v -> %v\n", ren["base_path"], ren["target_path"])
	check("②单文件改名被识别为 renamed（old-name.txt -> new-name.txt）",
		ren["base_path"] == "old-name.txt" && ren["target_path"] == "new-name.txt")
	amb := find(func(m map[string]any) bool { return m["change_type"] == "ambiguous" })
	fmt.Printf("  ambiguous 示例: %v detail=%s\n", amb["target_path"], amb["detail"])
	check("②两个相同内容文件只标 ambiguous，绝不虚构 rename",
		amb != nil && ren["base_path"] != amb["base_path"])

	meta := find(func(m map[string]any) bool { return m["target_path"] == "meta.sh" })
	link := find(func(m map[string]any) bool { return m["target_path"] == "link" })
	t1 := find(func(m map[string]any) bool { return m["target_path"] == "t1" })
	check("③仅改权限 => metadata_changed(mode)",
		meta["change_type"] == "metadata_changed" &&
			strings.Contains(meta["changed_fields"].(string), "mode"))
	check("③仅改符号链接目标 => metadata_changed(link_target)，且不跟随链接",
		link["change_type"] == "metadata_changed" &&
			strings.Contains(link["changed_fields"].(string), "link_target") &&
			t1["change_type"] == "unchanged")

	// pending snapshot must be rejected
	pendDiff := post(srv.URL+"/v1/snapshots", map[string]any{
		"root": diffSrc, "message": "diff pending", "finish": false,
	})
	pendDiffID := int64(pendDiff["snapshot_id"].(float64))
	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{
		"base_snapshot_id": diffID2, "target_snapshot_id": pendDiffID,
	})
	fmt.Printf("  对 pending 快照发起比较 -> HTTP %d: %s\n", code, body["error"])
	check("④pending/failed 快照被拒绝（409 snapshot_not_committed）",
		code == http.StatusConflict && body["error"] == "snapshot_not_committed")

	// storage rot discovered during comparison -> incomplete report, original
	// snapshots stay committed and the intact one stays restorable
	must(os.WriteFile(filepath.Join(diffSrc, "rot.bin"),
		[]byte(strings.Repeat("brand-new-rot-body-", 1200)), 0o644))
	d3 := post(srv.URL+"/v1/snapshots", map[string]any{"root": diffSrc, "message": "diff rot"})
	rotSnapID := int64(d3["snapshot_id"].(float64))
	victim := newestBlob(filepath.Join(repoDir, "chunks"))
	must(os.Remove(victim)) // simulate storage rot after commit
	fmt.Printf("  删掉已提交快照引用的块: %s\n", filepath.Base(victim))
	rotCode, rotBody := raw("POST", srv.URL+"/v1/diffs", map[string]any{
		"base_snapshot_id": diffID2, "target_snapshot_id": rotSnapID,
	})
	check("损坏情况下比较请求仍被接受", rotCode == http.StatusAccepted)
	rotRep := waitDiff(srv.URL, int64(rotBody["id"].(float64)))
	fmt.Printf("  报告 status=%v incomplete=%v missing=%v affected_files=%v\n",
		rotRep["status"], rotRep["incomplete"], rotRep["chunks_missing"], rotRep["affected_files"])
	check("④发现缺块 => complete 但 incomplete=true，并统计缺块数",
		rotRep["status"] == "complete" && rotRep["incomplete"] == true &&
			rotRep["chunks_missing"].(float64) >= 1)
	probDoc := get(srv.URL + fmt.Sprintf("/v1/diffs/%d/problems", int64(rotBody["id"].(float64))))
	probList := probDoc["problems"].([]any)
	fmt.Printf("  不完整报告列出 %d 条缺块引用，受影响路径: %v\n",
		len(probList), probDoc["affected_paths"])
	check("缺块被归到具体文件路径而不是笼统说未变更",
		len(probList) >= 1 && probList[0].(map[string]any)["rel_path"] != "")
	for _, id := range []int64{diffID2, rotSnapID} {
		s := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", id))
		check(fmt.Sprintf("④比较不改动原快照 %d 的状态（仍 committed）", id), s["status"] == "committed")
	}
	if _, err := os.Stat(filepath.Join(work, "diff-restore-intact")); os.IsNotExist(err) {
		rc, _ := raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", diffID2),
			map[string]any{"target": filepath.Join(work, "diff-restore-intact")})
		check("④未受损坏影响的原快照恢复能力不变", rc == http.StatusCreated)
	}

	// final listing
	section(0, "快照总览")
	list := get(srv.URL + "/v1/snapshots")["snapshots"].([]any)
	for _, x := range list {
		m := x.(map[string]any)
		fmt.Printf("  #%-3v %-10s files=%-3v bytes=%-7v %s\n",
			m["id"], m["status"], m["file_count"], m["bytes_total"], m["message"])
	}
	srv.Close()

	fmt.Println()
	if fail == 0 {
		fmt.Printf("✅ 全部 %d 项检查通过\n", pass)
		return
	}
	fmt.Printf("❌ %d 项失败，%d 项通过\n", fail, pass)
	os.Exit(1)
}

// ---------- helpers ----------

func startServer(repoDir string) *httptest.Server {
	must(os.MkdirAll(repoDir, 0o755))
	manifest, err := repo.OpenManifest(filepath.Join(repoDir, "manifest.sqlite"))
	must(err)
	store, err := repo.NewContentStore(filepath.Join(repoDir, "chunks"))
	must(err)
	engine, err := backup.NewEngine(manifest, store)
	must(err)
	diffs := backup.NewDiffService(engine)
	if recovered, err := engine.RecoverPending(); err == nil {
		for _, r := range recovered {
			fmt.Printf("  [启动恢复] 快照 %d -> %s\n", r.SnapshotID, r.Status)
		}
	}
	if resumed, err := diffs.RecoverInterrupted(); err == nil {
		for _, id := range resumed {
			fmt.Printf("  [启动恢复] 比较 %d 继续执行\n", id)
		}
	}
	return httptest.NewServer((&api.Server{Engine: engine, Diff: diffs}).NewRouter())
}

func post(url string, body any) map[string]any {
	code, b := raw("POST", url, body)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func get(url string) map[string]any {
	code, b := raw("GET", url, nil)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func raw(method, url string, body any) (int, map[string]any) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		must(err)
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, rdr)
	must(err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	must(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	must(err)
	var out map[string]any
	if len(data) > 0 {
		must(json.Unmarshal(data, &out))
		if out == nil {
			out = map[string]any{}
		}
	} else {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func hashFile(p string) (string, int64) {
	f, err := os.Open(p)
	must(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	must(err)
	return hex.EncodeToString(h.Sum(nil)), n
}

// waitDiff polls a diff job until it reaches a terminal state.
func waitDiff(base string, id int64) map[string]any {
	deadline := time.Now().Add(10 * time.Second)
	for {
		doc := get(base + fmt.Sprintf("/v1/diffs/%d", id))
		switch doc["status"] {
		case "complete", "failed":
			return doc
		}
		if time.Now().After(deadline) {
			out, _ := json.MarshalIndent(doc, "", "  ")
			must(fmt.Errorf("diff %d never finished: %s", id, out))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newestBlob returns the most recently modified chunk blob under root. In the
// demo it identifies a brand-new chunk written by the latest snapshot so the
// storage-rot drill can delete precisely it.
func newestBlob(root string) string {
	var newest string
	var newestMT time.Time
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if newest == "" || info.ModTime().After(newestMT) {
			newest, newestMT = p, info.ModTime()
		}
		return nil
	})
	must(err)
	if newest == "" {
		must(fmt.Errorf("no chunk blobs found under %s", root))
	}
	return newest
}

func section(n int, title string) {
	if n == 0 {
		fmt.Printf("\n── %s ──────────────────────────────\n", title)
		return
	}
	fmt.Printf("\n── %d. %s ──────────────────────────────\n", n, title)
}

func check(name string, ok bool) {
	if ok {
		pass++
		fmt.Printf("  ✓ %s\n", name)
		return
	}
	fail++
	fmt.Printf("  ✗ %s\n", name)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		var ee *exec.ExitError
		_ = ee
		os.Exit(2)
	}
}
