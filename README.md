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
cmd/backupd/main.go          HTTP 服务（启动时自动复验 pending 快照、续跑中断的比较）
cmd/demo/main.go             端到端演示（走真实 HTTP API，含 39 项断言）
internal/repo/
  contentstore.go            内容寻址块仓（原子写、读时校验摘要、分片目录、实时重算摘要）
  manifest.go                SQLite schema 与快照状态机（pending/committed/failed）
  manifest_write.go          条目/块写入、缺块诊断查询
  diff.go                    差异报告持久化（diff_jobs/items/problems/chunk_checks）
  meta.go                    分块多项式持久化
internal/backup/
  scan.go                    不跟随链接的目录扫描、分块、整文件摘要、写入中重读
  engine.go                  快照编排、提交前逐块验证、恢复与全部安全约束
  diff.go                    差异计算：逐块校验、分类、重命名/歧义判定、断点续跑
  util_linux.go              O_EXCL|O_NOFOLLOW 建文件（阻止沿预置符号链接写出）
internal/api/server.go       HTTP 路由（快照、恢复、差异创建/进度/明细）
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
| `POST /v1/diffs` | 对**两个 committed 快照**创建可持久化差异报告（`base_snapshot_id`、`target_snapshot_id`） |
| `GET  /v1/diffs` | 列出全部比较请求（进行中/失败/已完成都保留） |
| `GET  /v1/diffs/{id}` | 比较进度与汇总：状态、校验进度、复用/新增/缺块数、各分类计数、`incomplete` 标记 |
| `GET  /v1/diffs/{id}/items` | 分类明细：可按 `change_type` 过滤、`limit/offset` 分页 |
| `GET  /v1/diffs/{id}/problems` | 不完整原因：缺块/摘要不符的快照 id、文件路径、块摘要与磁盘实际摘要 |

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

curl -s -XPOST localhost:8090/v1/diffs \
  -d '{"base_snapshot_id":6,"target_snapshot_id":7}'
# 202 {"id":3,"status":"queued", ...}   重复请求返回同一个 id
curl -s localhost:8090/v1/diffs/3
# {"id":3,"status":"complete","incomplete":false,
#  "chunks_total":9,"chunks_checked":9,"chunks_reused":7,"chunks_new":1,
#  "counts":{"added":1,"deleted":0,"changed":1,"metadata_changed":1,
#            "unchanged":12,"renamed":1,"ambiguous":0,"unverified":0}}
curl -s 'localhost:8090/v1/diffs/3/items?change_type=changed'
```

## 可持久化快照差异报告

恢复前，维护人员要区分两个已提交快照之间到底是**内容变更、仅元数据变更还是文件迁移**，
不能只看总字节数。差异报告全程基于已持久化的整文件摘要与内容定义块序列，比较过程
**只读、绝不改写两个原快照**，自身状态存进 SQLite，可中断续跑、可重复追溯。

**入口约束**：`POST /v1/diffs` 只接受两个 `committed` 快照；任一为 `pending`/`failed`
（或不存在）直接 `409 snapshot_not_committed` / `404`，不会建任务。

**固定快照对 + 生成状态持久化**：`(base_snapshot_id, target_snapshot_id)` 有唯一约束，
重复请求返回同一份报告（同一 `id`）。任务状态为 `queued → running → complete/failed`，
每校验一批块就更新 `chunks_total/chunks_checked` 进度；每个 blob 的校验结论存入
`diff_chunk_checks`，进程在中途被杀死后，重启自动续跑（`RecoverInterrupted`），
已校验块不重读，结论落库后不再改动。

**条目分类**（`GET /v1/diffs/{id}/items`，每条带 `changed_fields`）：

| change_type | 含义 |
|---|---|
| `added` / `deleted` | 仅在目标 / 仅在基线中存在 |
| `changed` | 同路径整文件摘要不同、条目类型变化，或（同摘要但）块序列不同 |
| `metadata_changed` | 身份相同，仅 `mode`/`owner`/`mtime` 变化；符号链接仅**目标字符串**变化也归此类，全程不跟随链接 |
| `unchanged` | 内容与元数据均相同 |
| `renamed` | 整文件摘要在基线与目标中**各恰好出现一次**（全局一对一），路径不同 |
| `ambiguous` | 该摘要在任一侧有多个候选，无法唯一配对——只列候选（`base_candidates`/`target_candidates`），**绝不虚构迁移关系** |
| `unverified` | 结构性分类已得出，但参与文件引用了缺块/摘要不符的 blob，不能声称未变更 |

**块复用统计**：报告级 `chunks_reused`（目标引用、基线也有的不同块数）与 `chunks_new`
（基线没有的块）；每个文件条目也带自己的 `chunks_reused/chunks_new`——大文件中部改少量
字节时，只会有 1 个新块，其余块全部计为复用。

**完整性是硬门槛**：比较会对两个快照引用的**每一个不同 blob** 逐一做（a）`chunks` 行存在、
（b）blob 存在且长度一致、（c）流式重算 SHA-256 三项核对。任一参与文件缺块或 blob 摘要不符：

- 任务照样 `complete`，但 `incomplete=true`，`chunks_missing/chunks_mismatch/affected_files`
  计数非零；
- `/problems` 列出快照 id、受影响**文件路径**、块摘要、声明长度与磁盘实际摘要；
- 相关条目降级为 `unverified`（即使摘要字面上相等，也**不得**标成 unchanged/metadata）；
- 两个原快照的 `status` 与恢复能力不被比较动作改动。诊断只读，不修复、不判失败原快照。

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

## 演示会依次证明

1. 基线快照 → 恢复到新目录，逐文件核对摘要与长度（含空文件、0750 脚本、符号链接）；
2. 在 160KB 文件中部改 8 字节：**新块=1，复用旧块=4**，恢复结果与源一致；
3. 恢复到已存在目录 → `409 target_exists`；
4. 写入在重读窗口内停止 → 重读后成功；持续写入 → 3 次重读后拒绝并点名；
5. `lose_chunks:1` 模拟提交中断 → `failed` + `/missing` 给出精确缺块，旧快照仍可恢复；
6. 指向根目录外的符号链接 → 恢复 `422`，半成品目录回滚，外部文件不被触及；
7. `finish:false` 制造 pending → 重启服务后自动复验为 committed；
8. 快照差异报告：中部小改→`changed`+块复用；唯一改名→`renamed`、同内容多文件→`ambiguous`；
   仅权限/链接目标→`metadata_changed`（不跟随链接）；pending 被拒；比较中发现缺块→
   `incomplete` 报告列出路径，两个原快照状态与恢复能力不变；比较中断后续跑、重复请求返回同一报告。
