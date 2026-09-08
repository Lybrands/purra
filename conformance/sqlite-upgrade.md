# SQLite upgrade backup and rollback

The repository's `scripts/backup_sqlite.py` is a standalone Python 3.11+ utility
for a POSIX host (Python or TypeScript databases). It uses SQLite's backup API,
including committed WAL content, then checks SQLite integrity, foreign keys and
the PurrA v4/v5 format marker. It does not import either SDK or reinterpret its
state. A successful copy does not certify semantic Run validity or resume safety.

## Offline procedure

1. Stop all host writers/workers and retain the original runtime. On the open
   adapter, inspect upgrade readiness. Finish active work and reconcile unknown
   effects through the original runtime; preflight never authorizes execution.
2. Create a new backup file in a trusted, private directory:

   ```sh
   python3 scripts/backup_sqlite.py /absolute/source.db /absolute/backup.db
   ```

   Source is opened read-only. The destination must not exist, even as a symlink.
   A private staging file is validated and fsynced, then published with a hard link
   that cannot overwrite an existing path. The containing directory is fsynced.
   `--timeout-seconds 30` sets the default cooperative deadline for copying,
   validation and hashing before publication. Busy sources do not wait indefinitely.
   Expiration closes SQLite connections and removes staging without publishing a
   destination. Individual filesystem calls (including fsync) cannot be forcibly
   interrupted; this is not a hard real-time deadline. After publication, a directory
   fsync error can leave the completed destination present; preserve and inspect it
   rather than overwriting it on retry.
   The JSON report contains the format, size and SHA-256, with `resumeAuthority: none`.
   It contains no database rows or source paths. Protect the backup itself: it
   contains the original private data and is not encrypted by this utility.
3. Independently verify historical reads from a disposable copy of the backup
   using the matching SDK. Then explicitly activate v5 on the intended database.
   Activation rechecks the database in its writer transaction; an earlier ready
   report can become stale. Do not restart an old writer against the upgraded file.
4. If rollback is needed, stop writers again. Preserve the upgraded database and
   its WAL/SHM files for reconciliation. Use the same command to copy the backup
   to a **new** database path; select that path in an isolated matching-runtime
   environment and verify history before choosing whether any work may continue.

The command never overwrites the original file, replaces a running database,
removes WAL/SHM files, or downgrades v5 rows in place. A backup made before an
external write cannot undo that write. Restoring old state can lose newer approval
and receipt knowledge and cause duplicate effects if work is blindly replayed.
Keep the newer database and reconcile its effects before resuming business work.

A backup of active work is a consistent snapshot, not an offline-upgrade approval.
The utility is currently validated locally on macOS; this does not claim Windows,
remote-filesystem, remote-worker, or production migration acceptance. Copying a
TypeScript database does not make it readable by the Python SDK or vice versa.

## 升级与回滚边界

此命令用 SQLite 备份 API 保留已提交的 WAL 数据，不直接复制主数据库文件。升级前停止
所有宿主 writer／worker，保留旧运行时，完成活跃任务并处理未知效果；备份到不存在的
新路径，校验报告与实际历史读取后才显式激活 v5。备份文件包含原私有数据，须妥善保管。

回滚时保留升级后的库和外部效果证据，将旧备份恢复到另一个新路径，使用匹配 SDK 验证。
不得直接覆盖生产库、删除 WAL／SHM、原地降级或盲目重跑旧 Run。旧快照不能撤销实际
发生的远端写入，也可能缺少新审批／回执；先对账再决定能否恢复业务执行。
本切片完成备份与新路径恢复工具，不代表通用历史格式迁移、活跃 Run 跨版本续跑或生产
回滚验收已完成。
