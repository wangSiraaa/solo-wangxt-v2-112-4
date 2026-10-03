# incbackup — 可验证的本地增量备份服务（仅 API）

“备份任务显示成功，恢复时却缺了一段文件”——本服务用一个硬规则拦住它：

> **快照在提交之前，必须逐个核对清单引用的每个内容块在内容仓中真实存在且长度正确；
> 恢复时再对流过的每个块和每个整文件做 SHA-256 与长度核对。**

任何一块对不上，快照就是 `failed`（或崩溃留下的 `pending`，重启后自动复验），
永远不会出现“成功”的快照恢复出残缺目录。

## 技术栈

| 组件 | 选择 | 用途 |
|---|---|---|
| 分块 | `github.com/restic/chunker`（Rabin 指纹内容定义分块） | 小改动只产生 1 个新块，其余块哈希相同直接复用 |
| 清单 | SQLite（`modernc.org/sqlite`，纯 Go，无 CGO） | 快照、条目、块索引、错误记录 |
| 内容仓 | 独立目录 `<repo>/chunks/ab/cdef…` | SHA-256 内容寻址、去重、只读不可变 blob |
| 接口 | 本地 HTTP API（默认 `127.0.0.1:8090`） | 无 UI、无鉴权，设计为只监听本地 |

分块多项式持久化在 `meta` 表中，跨快照/跨重启保持一致——否则边界漂移会让增量失效。

## 目录结构

```
cmd/backupd/main.go          HTTP 服务（启动时自动复验 pending 快照、续跑中断的差异报告）
cmd/demo/main.go             端到端演示（走真实 HTTP API，含 52 项断言）
internal/repo/
  contentstore.go            内容寻址块仓（原子写、读时校验摘要、分片目录）
  manifest.go                SQLite schema 与快照状态机（pending/committed/failed）
  manifest_write.go          条目/块写入、缺块诊断查询
  diff.go                    差异报告持久化（报告、明细、逐块校验、缺块清单）
  meta.go                    分块多项式持久化
internal/backup/
  scan.go                    不跟随链接的目录扫描、分块、整文件摘要、写入中重读
  engine.go                  快照编排、提交前逐块验证、恢复与全部安全约束
  diff.go                    快照对比分类、可续跑的报告生成、块完整性核验
  util_linux.go              O_EXCL|O_NOFOLLOW 建文件（阻止沿预置符号链接写出）
internal/api/server.go       HTTP 路由
```

## 快速开始

```bash
go run ./cmd/demo            # 端到端演示（临时目录，自动清理）
go test ./...                # 单元测试
go run ./cmd/backupd --repo ./backup-repo --addr 127.0.0.1:8090
```

## HTTP API

| 方法与路径 | 说明 |
|---|---|
| `POST /v1/snapshots` | 扫描 `root` → 落块 → **逐块验证** → 提交。`finish:false`、`lose_chunks:N` 为故障演练参数 |
| `GET  /v1/snapshots` | 列出全部快照（含 failed，失败记录不删除） |
| `GET  /v1/snapshots/{id}` | 单个快照状态 |
| `GET  /v1/snapshots/{id}/missing` | **维护入口**：列出每个缺块的文件路径、SHA-256、期望磁盘路径与原因 |
| `GET  /v1/snapshots/{id}/errors` | 扫描/验证阶段的逐条错误（stage、rel_path、chunk_digest） |
| `POST /v1/snapshots/{id}/verify` | 对 pending 快照重新执行逐块验证并提交/判失败 |
| `POST /v1/snapshots/{id}/restore` | 恢复到**全新**目录，返回逐文件长度+摘要+块数报告 |
| `POST /v1/recover` | 复验所有 pending 快照（服务启动时也会自动执行） |
| `POST /v1/diffs` | 比较两个 **committed** 快照，生成可持久化差异报告（幂等：同一快照对返回同一报告） |
| `GET  /v1/diffs` | 列出全部差异报告 |
| `GET  /v1/diffs/{id}` | 报告状态与生成进度（`status`、`phase`、`progress`） |
| `GET  /v1/diffs/{id}/items` | 分类明细（可用 `?type=renamed` 等过滤） |
| `GET  /v1/diffs/{id}/missing` | 完整性失败清单：受影响路径、块摘要、原因 |

### 典型请求

```bash
curl -s -XPOST localhost:8090/v1/snapshots \
  -d '{"root":"/srv/data","message":"nightly"}'
# 201 {"snapshot_id":7,"status":"committed","chunks_new":1,"chunks_referenced":5}

curl -s localhost:8090/v1/snapshots/7/missing
# {"snapshot_id":7,"status":"failed","missing":[
#   {"rel_path":"app.log",
#    "chunk_digest":"d2a8d66b…",
#    "expected_blob_path":"/…/chunks/d2/a8d66b…",
#    "reason":"chunk blob missing or length mismatch in content store"}]}

curl -s -XPOST localhost:8090/v1/snapshots/7/restore \
  -d '{"target":"/restore/2026-09-29"}'

curl -s -XPOST localhost:8090/v1/diffs -d '{"base_id":3,"target_id":7}'
# 201 {"id":1,"status":"done","integrity":"complete",
#      "counts":{"content_changed":1,"renamed":1,"meta_changed":2,...},
#      "chunks_reused":7,"chunks_new":1,...}

curl -s 'localhost:8090/v1/diffs/1/items?type=content_changed'
# {"items":[{"change_type":"content_changed","kind":"file","rel_path":"app.log",
#   "old":{"size":163840,"digest":"…"},"new":{"size":163840,"digest":"…"},
#   "chunks_reused":4,"chunks_new":1}]}
```

## 快照差异报告

维护人员在恢复前需要知道两个快照之间到底变了什么。`POST /v1/diffs` 只接受两个
**committed** 快照（pending/failed 一律 `409 snapshot_not_committed`，且不留下报告行），
把比较请求、固定的快照对与生成状态全部存入 SQLite：

- **分类维度**：条目类型（file/dir/symlink）、整文件 SHA-256、块序列、权限位、uid/gid、
  mtime、符号链接目标。结论分为 `added`、`deleted`、`content_changed`、
  `meta_changed`（仅权限/属主/时间变化，字段名列在 `changed_fields`）、
  `renamed`、`ambiguous_rename`；未变化的条目不占明细，只计入 `counts.unchanged`。
  符号链接的目标是链接本身的“内容”：改目标判 `content_changed`，比较的是存储的
  目标字符串，全程不跟随链接。
- **重命名判定**：只有某文件摘要在“被删侧”和“新增侧”都唯一（1:1）时才标 `renamed`；
  同一摘要有多个候选时，所有涉及路径都标 `ambiguous_rename` 并列出候选，
  绝不虚构迁移关系。目录与符号链接没有内容摘要，不参与改名配对。
- **块统计**：报告级给出目标快照引用的去重块中有多少已被基线引用（`chunks_reused`）、
  多少是新内容（`chunks_new`）；`added`/`content_changed` 明细项同样给出该文件
  块序列的复用/新增计数——大文件中部改几个字节时表现为 `chunks_new:1`。
- **完整性**：生成报告时对两个快照引用的每个去重块做目录行存在性、blob 存在性+长度、
  **blob 内容 SHA-256 回读校验**。任一参与文件引用缺块或 blob 摘要不符，报告即为
  `integrity:incomplete` 并在 `/missing` 里逐条列出路径——即使该文件在两个快照间
  没有树级变化，也绝不会被说成“未变更”。比较只读快照与内容仓，两个原快照的状态
  和恢复能力不受影响。
- **幂等与续跑**：`(base_id, target_id)` 唯一，重复请求返回同一报告（不再重算）。
  生成分 classify → verify → finalize 三个阶段持久化进度；进程中断后报告保持
  `running`，下次同一请求（或服务重启时的自动续跑）从已持久化的逐块校验进度继续，
  不回溯、不改写原快照。

## 关键正确性保证

1. **完成前验证所有内容块**：`snapshots.status` 只有 pending→committed/failed。
   提交前 `FindMissingChunks` 同时检查（a）清单里是否有块行、（b）blob 是否存在且长度一致；
   每块在恢复读取时再做流式 SHA-256 校验，每个文件组装后比对整文件摘要与总长度。
2. **扫描中正在写入的文件**：读取前后比对 size+mtime，并增加读后置静窗口
   （防止小文件恰好在两次 append 之间被整文件读完）。检测到变化→整块重读（最多 3 次）；
   仍在变→快照 `failed`，错误明确点名文件，绝不猜测版本。
3. **权限与符号链接本身保留**：保存并恢复目录/文件权限位、属主（root 时）、mtime；
   符号链接存的是链接本身与目标字符串，扫描与恢复均不跟随。
4. **不越界**：恢复前校验清单路径无绝对路径/`..`；符号链接目标按词法解析，解析后必须仍在恢复根内；
   任何现存祖先目录是符号链接一律拒绝；Linux 下用 `O_EXCL|O_NOFOLLOW` 建文件。
5. **不覆盖**：恢复目标已存在（任何类型）直接 `409`；恢复中途失败自动删除半成品目录。
6. **空文件**：长度 0、整文件摘要 `e3b0c442…`、0 个内容块，正常备份与恢复。
7. **失败可定位**：failed/pending 快照永久保留，`/missing` 直接给出“哪个文件的哪个块该在哪个路径”，
   而不是只看到队列空了。
8. **差异报告不猜测**：只比较 committed 快照；同摘要多候选的“改名”一律标歧义；
   任一参与文件缺块或 blob 摘要不符，报告就是 `incomplete` 并列出路径，
   绝不把损坏说成“未变更”。

## 演示会依次证明

1. 基线快照 → 恢复到新目录，逐文件核对摘要与长度（含空文件、0750 脚本、符号链接）；
2. 在 160KB 文件中部改 8 字节：**新块=1，复用旧块=4**，恢复结果与源一致；
3. 恢复到已存在目录 → `409 target_exists`；
4. 写入在重读窗口内停止 → 重读后成功；持续写入 → 3 次重读后拒绝并点名；
5. `lose_chunks:1` 模拟提交中断 → `failed` + `/missing` 给出精确缺块，旧快照仍可恢复；
6. 指向根目录外的符号链接 → 恢复 `422`，半成品目录回滚，外部文件不被触及；
7. `finish:false` 制造 pending → 重启服务后自动复验为 committed；
8. 差异报告分类：大文件中部改 8 字节 → `content_changed` 且 `chunks_new:1`；
   单文件改名 → `renamed`；两个相同内容文件 → 全部 `ambiguous_rename`；
   仅改权限/mtime → `meta_changed`；改链接目标 → `content_changed`（不跟随链接）；
9. pending 快照参与比较 → `409`；覆写一个 blob 模拟存储损坏 → 报告
   `integrity:incomplete` 并点名 `big.bin`，两个原快照仍 committed、可恢复；
10. `stop_after_checks:2` 模拟比较中途崩溃 → 报告保持 `running` 且进度 2/9 已持久化，
    同一请求续跑到 `done`（9/9），报告 id 不变。
