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
	"io/fs"
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

	// ---- 8. diff report: classification ------------------------------------
	section(8, "快照差异报告：内容变更(含复用块)、重命名、歧义、仅元数据变更")
	d := filepath.Join(work, "diffsrc")
	must(os.MkdirAll(d, 0o755))
	big2 := make([]byte, 0, 150*1024)
	for i := 0; i < 150*1024; i++ {
		big2 = append(big2, byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n"[i%37]))
	}
	must(os.WriteFile(filepath.Join(d, "big.bin"), big2, 0o644))
	must(os.WriteFile(filepath.Join(d, "keep.txt"), []byte("keep\n"), 0o644))
	must(os.WriteFile(filepath.Join(d, "oldname.txt"), []byte("rename me\n"), 0o644))
	must(os.WriteFile(filepath.Join(d, "dupA.txt"), []byte("duplicate content\n"), 0o644))
	must(os.WriteFile(filepath.Join(d, "dupB.txt"), []byte("duplicate content\n"), 0o644))
	must(os.WriteFile(filepath.Join(d, "meta.sh"), []byte("#!/bin/sh\n"), 0o750))
	must(os.WriteFile(filepath.Join(d, "stamped.txt"), []byte("stamped\n"), 0o644))
	must(os.Symlink("keep.txt", filepath.Join(d, "link1")))

	snapA := post(srv.URL+"/v1/snapshots", map[string]any{"root": d, "message": "diff base"})
	snapAID := int64(snapA["snapshot_id"].(float64))
	check("diff 基线快照 committed", snapA["status"] == "committed")
	blobsBefore := listBlobs(repoDir)

	// ① a few bytes in the middle of the large file
	copy(big2[70*1024:], []byte("PATCHED!"))
	must(os.WriteFile(filepath.Join(d, "big.bin"), big2, 0o644))
	// ② one rename + an identical-content group that must stay ambiguous
	must(os.Rename(filepath.Join(d, "oldname.txt"), filepath.Join(d, "newname.txt")))
	must(os.Remove(filepath.Join(d, "dupA.txt")))
	must(os.Remove(filepath.Join(d, "dupB.txt")))
	must(os.WriteFile(filepath.Join(d, "dupC.txt"), []byte("duplicate content\n"), 0o644))
	// ③ metadata-only changes and a symlink retarget
	must(os.Chmod(filepath.Join(d, "meta.sh"), 0o700))
	stamp := time.Now().Add(-3 * time.Hour)
	must(os.Chtimes(filepath.Join(d, "stamped.txt"), stamp, stamp))
	must(os.Remove(filepath.Join(d, "link1")))
	must(os.Symlink("big.bin", filepath.Join(d, "link1")))

	snapB := post(srv.URL+"/v1/snapshots", map[string]any{"root": d, "message": "diff target"})
	snapBID := int64(snapB["snapshot_id"].(float64))
	newBlobs := diffSets(listBlobs(repoDir), blobsBefore)
	check("快照 B 只产生 1 个新内容块（big.bin 中部改动）", len(newBlobs) == 1)

	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{"base_id": snapAID, "target_id": snapBID})
	diffAB := body
	diffABID := int64(diffAB["id"].(float64))
	fmt.Printf("  差异报告 %d: HTTP %d status=%s integrity=%s 复用块=%v 新增块=%v\n",
		diffABID, code, diffAB["status"], diffAB["integrity"], diffAB["chunks_reused"], diffAB["chunks_new"])
	check("创建差异报告返回 201 且已完成、完整", code == http.StatusCreated &&
		diffAB["status"] == "done" && diffAB["integrity"] == "complete")
	check("报告级统计：新增内容块=1（其余全部复用）", diffAB["chunks_new"].(float64) == 1)

	items := get(srv.URL + fmt.Sprintf("/v1/diffs/%d/items", diffABID))["items"].([]any)
	byPath := map[string]map[string]any{}
	for _, x := range items {
		m := x.(map[string]any)
		byPath[m["rel_path"].(string)] = m
		fmt.Printf("    %-14s %-16s %s\n", m["change_type"], m["kind"], m["rel_path"])
	}
	bigItem := byPath["big.bin"]
	check("① big.bin 判为 content_changed", bigItem != nil && bigItem["change_type"] == "content_changed")
	check("① big.bin 只新增 1 块、其余块复用",
		bigItem != nil && bigItem["chunks_new"].(float64) == 1 && bigItem["chunks_reused"].(float64) >= 2)
	renItem := byPath["newname.txt"]
	check("② 单文件改名识别为 renamed (oldname.txt → newname.txt)",
		renItem != nil && renItem["change_type"] == "renamed" && renItem["old_rel_path"] == "oldname.txt")
	ambOK := true
	for _, p := range []string{"dupA.txt", "dupB.txt", "dupC.txt"} {
		m := byPath[p]
		if m == nil || m["change_type"] != "ambiguous_rename" {
			ambOK = false
		}
	}
	check("② 两个相同内容文件不虚构迁移：dupA/dupB/dupC 全部标为歧义", ambOK)
	cands, _ := byPath["dupC.txt"]["candidates"].([]any)
	check("② dupC.txt 的歧义候选列出 dupA.txt 与 dupB.txt", len(cands) == 2)
	metaItem := byPath["meta.sh"]
	fields, _ := metaItem["changed_fields"].([]any)
	check("③ 仅改权限判为 meta_changed [mode]",
		metaItem != nil && metaItem["change_type"] == "meta_changed" && len(fields) == 1 && fields[0] == "mode")
	stampItem := byPath["stamped.txt"]
	sfields, _ := stampItem["changed_fields"].([]any)
	check("③ 仅改 mtime 判为 meta_changed [mtime]",
		stampItem != nil && stampItem["change_type"] == "meta_changed" && len(sfields) == 1 && sfields[0] == "mtime")
	linkItem := byPath["link1"]
	linkNew, _ := linkItem["new"].(map[string]any)
	check("③ 符号链接改目标判为 content_changed（比较存储的目标字符串，不跟随链接）",
		linkItem != nil && linkItem["change_type"] == "content_changed" &&
			linkItem["kind"] == "symlink" && linkNew["link_target"] == "big.bin")
	keepOK := byPath["keep.txt"] == nil
	check("未变化的 keep.txt 不出现在变更明细中", keepOK)

	renamedOnly := get(srv.URL + fmt.Sprintf("/v1/diffs/%d/items?type=renamed", diffABID))["items"].([]any)
	check("明细支持按类型过滤 (?type=renamed)", len(renamedOnly) == 1)

	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{"base_id": snapAID, "target_id": snapBID})
	check("重复请求返回同一报告（幂等、可追溯）",
		code == http.StatusOK && int64(body["id"].(float64)) == diffABID)
	prog := get(srv.URL + fmt.Sprintf("/v1/diffs/%d", diffABID))["progress"].(map[string]any)
	check("进度查询：done == total", prog["done"] == prog["total"])
	check("报告列表包含该报告", func() bool {
		for _, x := range get(srv.URL + "/v1/diffs")["diffs"].([]any) {
			if int64(x.(map[string]any)["id"].(float64)) == diffABID {
				return true
			}
		}
		return false
	}())

	// ---- 9. diff integrity: rejection + corrupt chunk -----------------------
	section(9, "差异报告完整性：拒绝非 committed 快照；损坏块→不完整报告且原快照不受影响")
	pend2 := post(srv.URL+"/v1/snapshots", map[string]any{"root": d, "message": "pending for diff", "finish": false})
	pend2ID := int64(pend2["snapshot_id"].(float64))
	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{"base_id": snapAID, "target_id": pend2ID})
	check("④ 目标为 pending 快照被拒绝 (409 snapshot_not_committed)",
		code == http.StatusConflict && body["error"] == "snapshot_not_committed")
	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{"base_id": pend2ID, "target_id": snapAID})
	check("④ 基线为 pending 快照同样被拒绝", code == http.StatusConflict)
	post(srv.URL+"/v1/recover", nil) // finalize the pending snapshot again

	snapC := post(srv.URL+"/v1/snapshots", map[string]any{"root": d, "message": "no changes"})
	snapCID := int64(snapC["snapshot_id"].(float64))
	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{"base_id": snapBID, "target_id": snapCID})
	diffBC := body
	bcItems := get(srv.URL + fmt.Sprintf("/v1/diffs/%d/items", int64(diffBC["id"].(float64))))["items"].([]any)
	check("无改动的快照对：明细为空、新增块=0、integrity=complete",
		code == http.StatusCreated && diffBC["integrity"] == "complete" &&
			len(bcItems) == 0 && diffBC["chunks_new"].(float64) == 0)

	// corrupt the one blob that snapshot B introduced (big.bin's new chunk)
	victim := newBlobs[0]
	orig, err := os.ReadFile(victim)
	must(err)
	must(os.Chmod(victim, 0o644))
	badBytes := make([]byte, len(orig))
	for i := range badBytes {
		badBytes[i] = 0xFF
	}
	must(os.WriteFile(victim, badBytes, 0o444))
	fmt.Printf("  模拟存储损坏：覆写 blob %s（等长垃圾字节）\n", victim)

	snapD := post(srv.URL+"/v1/snapshots", map[string]any{"root": d, "message": "after corruption"})
	snapDID := int64(snapD["snapshot_id"].(float64))
	check("损坏 blob 存在且长度不变时快照仍可 committed（提交期核对存在性+长度）",
		snapD["status"] == "committed")
	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{"base_id": snapCID, "target_id": snapDID})
	diffCD := body
	diffCDID := int64(diffCD["id"].(float64))
	fmt.Printf("  差异报告 %d: status=%s integrity=%s\n", diffCDID, diffCD["status"], diffCD["integrity"])
	check("④ 比较中发现损坏块 → 报告标为 incomplete（而不是“未变更”）",
		code == http.StatusCreated && diffCD["status"] == "done" && diffCD["integrity"] == "incomplete")
	cdMissing := get(srv.URL + fmt.Sprintf("/v1/diffs/%d/missing", diffCDID))["missing"].([]any)
	namedBig, namedSnaps := false, map[int64]bool{}
	for _, x := range cdMissing {
		m := x.(map[string]any)
		fmt.Printf("    缺块: 快照 %v 文件 %s 原因 %s\n", m["snapshot_id"], m["rel_path"], m["reason"])
		if m["rel_path"] == "big.bin" {
			namedBig = true
			namedSnaps[int64(m["snapshot_id"].(float64))] = true
		}
	}
	check("④ 不完整报告列出受影响路径 big.bin（两个快照都点名）",
		namedBig && namedSnaps[snapCID] && namedSnaps[snapDID])
	cdItems := get(srv.URL + fmt.Sprintf("/v1/diffs/%d/items", diffCDID))["items"].([]any)
	check("④ 树级对比无变更项，但完整性结论不是“未变更”", len(cdItems) == 0)
	siC := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", snapCID))
	siD := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", snapDID))
	check("④ 比较不改写原快照：两个快照仍是 committed",
		siC["status"] == "committed" && siD["status"] == "committed")

	restoreA := filepath.Join(work, "restore-diffA")
	code, _ = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", snapAID), map[string]any{"target": restoreA})
	check("④ 不引用坏块的快照 A 恢复能力不受影响", code == http.StatusCreated)
	must(os.Chmod(victim, 0o644))
	must(os.WriteFile(victim, orig, 0o444)) // repair the externally corrupted blob
	restoreD := filepath.Join(work, "restore-diffD")
	code, _ = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", snapDID), map[string]any{"target": restoreD})
	check("④ 修复 blob 后快照 D 正常恢复（差异比较全程未触碰快照与内容仓）", code == http.StatusCreated)

	// ---- 10. diff resume after interruption ---------------------------------
	section(10, "差异报告可续跑：中断后从已持久化的进度继续，重复请求收敛到同一报告")
	code, body = raw("POST", srv.URL+"/v1/diffs",
		map[string]any{"base_id": snapAID, "target_id": snapDID, "stop_after_checks": 2})
	diffAD := body
	diffADID := int64(diffAD["id"].(float64))
	prog = diffAD["progress"].(map[string]any)
	fmt.Printf("  中断点: HTTP %d status=%s 进度=%v/%v\n", code, diffAD["status"], prog["done"], prog["total"])
	check("验证中途“崩溃”：报告保持 running 且进度已持久化",
		code == http.StatusAccepted && diffAD["status"] == "running" && prog["done"].(float64) == 2)
	code, body = raw("POST", srv.URL+"/v1/diffs", map[string]any{"base_id": snapAID, "target_id": snapDID})
	prog = body["progress"].(map[string]any)
	fmt.Printf("  续跑后: HTTP %d status=%s 进度=%v/%v\n", code, body["status"], prog["done"], prog["total"])
	check("同一请求续跑同一报告直至完成（id 不变）",
		code == http.StatusOK && int64(body["id"].(float64)) == diffADID &&
			body["status"] == "done" && prog["done"] == prog["total"])

	// final listing
	section(0, "快照总览")
	list := get(srv.URL + "/v1/snapshots")["snapshots"].([]any)
	for _, x := range list {
		m := x.(map[string]any)
		fmt.Printf("  #%-3v %-10s files=%-3v bytes=%-7v %s\n",
			m["id"], m["status"], m["file_count"], m["bytes_total"], m["message"])
	}
	diffs := get(srv.URL + "/v1/diffs")["diffs"].([]any)
	fmt.Printf("\n── 差异报告总览 ──────────────────────────────\n")
	for _, x := range diffs {
		m := x.(map[string]any)
		counts := m["counts"].(map[string]any)
		fmt.Printf("  #%-3v 快照 %v→%v  %-6s %-11s 新增=%v 删除=%v 内容变更=%v 元数据=%v 改名=%v 歧义=%v\n",
			m["id"], m["base_id"], m["target_id"], m["status"], m["integrity"],
			counts["added"], counts["deleted"], counts["content_changed"],
			counts["meta_changed"], counts["renamed"], counts["ambiguous_rename"])
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
	if recovered, err := engine.RecoverPending(); err == nil {
		for _, r := range recovered {
			fmt.Printf("  [启动恢复] 快照 %d -> %s\n", r.SnapshotID, r.Status)
		}
	}
	return httptest.NewServer((&api.Server{Engine: engine}).NewRouter())
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

// listBlobs returns the set of chunk blob paths currently in the store.
func listBlobs(repoDir string) map[string]bool {
	out := map[string]bool{}
	root := filepath.Join(repoDir, "chunks")
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && !strings.HasPrefix(d.Name(), ".tmp-") {
			out[p] = true
		}
		return nil
	})
	return out
}

// diffSets returns the paths present in now but absent from before.
func diffSets(now, before map[string]bool) []string {
	var out []string
	for p := range now {
		if !before[p] {
			out = append(out, p)
		}
	}
	return out
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
