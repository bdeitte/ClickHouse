---
description: 'Implementation plan for crash-safe EXCHANGE in Atomic databases on filesystems without renameat2(RENAME_EXCHANGE)'
sidebarTitle: 'NFS atomic exchange plan'
slug: /superpowers/plans/2026-10-22-nfs-atomic-exchange
title: 'Crash-safe EXCHANGE implementation plan'
doc_type: 'guide'
---

# Crash-safe `EXCHANGE` on filesystems without atomic exchange: implementation plan {#plan}

> For agentic workers: REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Goal: make `EXCHANGE`, `CREATE OR REPLACE`, `REPLACE` and refreshable materialized views work, crash-safely, in `Atomic` and `Replicated` databases whose metadata is on a `DiskLocal` disk that cannot do `renameat2(RENAME_EXCHANGE)`, such as NFS.

Architecture: `DatabaseAtomic` probes once per database whether its metadata directory can exchange atomically. When it can't, `renameTable` writes a durable record, does three moves, and rolls forward from the record if a move fails. `loadTablesMetadata` finishes any exchange a crash interrupted. The record format and loader rules are ported from https://github.com/ClickHouse/ClickHouse/pull/124937, whose diff is saved at `tmp/pr124937.diff`.

Tech stack: C++ in `src/Databases`, ClickHouse failpoints (`src/Common/FailPoint.cpp`), stateless `.sh` tests in `tests/queries/0_stateless`.

Spec: `docs/superpowers/specs/2026-10-22-nfs-atomic-exchange-design.md`. Read it before starting any task. This plan does not repeat its reasoning.

## Global constraints {#global-constraints}

- Branch `nfs-atomic-exchange`. Add commits, never rebase or amend. Never commit to `master`.
- Allman braces. Literal names in comments and commit messages go in backticks. Write functions as `f`, not `f()`.
- No fallback that hides an error. No `sleep` to fix races in C++. Tests wait with bounded polling loops that pause briefly between attempts, never with one fixed sleep.
- Record and recovered-metadata writes honor `fsync_metadata` through `writeMetadataFile(disk, path, content, fsync)` from `src/Databases/DatabasesCommon.h`.
- Only `DiskLocal` database disks change. Object storage database disks keep master behavior exactly, including the `supportsAtomicRename` gates.
- No new setting, no protocol or format change, no docs change.
- Record file `.<file>.exchange` next to the metadata file, header line `exchange v1`, temporary file `old_path + ".tmp_exchange"`, probe files prefixed `.exchange_probe_`.
- Environment for every build and test command: `source ~/.clickhouse-dev/env.sh`, run from the repository root.
- Build: `ninja -C build clickhouse > build/build.log 2>&1`, with no `-j`. Have a subagent read `build/build.log` and return a short summary.
- Server for tests: after each build, stop any running dev server and start a new one in the background with `cd programs/server && ../../build/programs/clickhouse server > ../../build/server.log 2>&1`.
- Tests: `tests/clickhouse-test <name> > build/test_<name>.log 2>&1`. Have a subagent read the log and return a short summary.
- Create each new test with `./tests/queries/0_stateless/add-test <name>.sh`. Tests below are referred to by name without the number prefix.
- Every test database is created with `SETTINGS disk = 'default'`, so its metadata stays on `DiskLocal` in the `db disk` CI mode.

## Review focus {#review-focus}

These inputs are implied by the spec but no task's main tests exercise them. Each has a test added to the task that owns the code.

1. Table names that `escapeForFileName` changes, such as `` `t a` `` and `` `t.b` ``: the exchange through a record and its recovery work on the escaped file names. Test in Task 2.
2. A record left behind after a completed exchange, followed by an `ALTER` that changes one of the files, then a reload: the database loads, the record is removed, and nothing throws `CORRUPTED_DATA`. Test in Task 1.
3. The same pair exchanged twice in a row: tables are back where they started, and the second exchange overwrote the first record without conflict. Test in Task 2.
4. A refreshable materialized view refreshed several times: no record and no `_tmp_replace_*` table is left. Test in Task 2.
5. A cross-database crash in which the database holding the copy of the record is attached first: that database finishes the exchange, and the other attaches cleanly afterwards. Test in Task 1.

---

### Task 1: Finish interrupted exchanges at load {#task-1}

Files:
- Create: `src/Databases/MetadataExchange.h`, `src/Databases/MetadataExchange.cpp`
- Modify: `src/Databases/DatabaseAtomic.h`, `src/Databases/DatabaseAtomic.cpp`
- Test: `tests/queries/0_stateless/<N>_atomic_exchange_recovery_at_load.sh` and `.reference`

Interfaces:
- Consumes: `readMetadataFile`, `writeMetadataFile` from `src/Databases/DatabasesCommon.h`. Serialization and loader code from `tmp/pr124937.diff` (`serializeExchangeRecord`, `parseExchangeRecord`, `finishInterruptedExchanges`).
- Produces, in `MetadataExchange.h`, namespace `DB`:
  - `struct MetadataExchangeRecord { String old_path; String new_path; String old_path_content; String new_path_content; };` The two contents are what each path holds after the exchange. Paths are relative to the disk.
  - `String metadataExchangeRecordPath(const String & metadata_file_path);` maps `dir/a.sql` to `dir/.a.sql.exchange`.
  - `String metadataExchangeTmpPath(const String & old_path);` returns `old_path + ".tmp_exchange"`.
  - `inline constexpr auto metadata_exchange_probe_prefix = ".exchange_probe_";`
  - `String serializeMetadataExchangeRecord(const MetadataExchangeRecord & record);`
  - `std::optional<MetadataExchangeRecord> parseMetadataExchangeRecord(const String & content);`
  - `Strings writeMetadataExchangeRecords(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);` writes the primary next to `old_path`, plus a copy next to `new_path` when the parent directories differ. Each is written as `<record>.tmp`, then put in place with `replaceFile`. Returns the paths written, primary first.
  - `void tryRemoveMetadataExchangeRecords(const DiskPtr & disk, const Strings & record_paths, const LoggerPtr & log) noexcept;` is best effort and logs failures.
  - `enum class MetadataExchangeState { BeforeExchange, Interrupted, AfterExchange, Inconsistent };`
  - `MetadataExchangeState inspectMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record);`
  - `void finishMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);` writes both recorded contents through `.tmp` and `replaceFile`, then removes the temporary file with `removeFileIfExists`.
  - `void finishInterruptedMetadataExchanges(const DiskPtr & disk, const String & metadata_dir, bool fsync, const LoggerPtr & log);`
  - `DatabaseAtomic::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup) override;`
  - `static bool DatabaseAtomic::canEmulateExchange(const DiskPtr & disk);` returns `dynamic_cast<const DiskLocal *>(disk.get()) != nullptr`.

`inspectMetadataExchange` classifies the files as follows. Tasks 1 and 4 both depend on it:

```text
tmp exists:
    tmp holds record.new_path_content, and each of old_path and new_path is absent
    or holds old_path_content or new_path_content          -> Interrupted
    otherwise                                               -> Inconsistent
tmp absent:
    old_path holds old_path_content and new_path holds new_path_content -> AfterExchange
    old_path holds new_path_content and new_path holds old_path_content -> BeforeExchange
    otherwise (including a missing file)                                -> Inconsistent
```

`finishInterruptedMetadataExchanges` follows the spec's "Finishing an exchange at load" section, under a function-local `static std::mutex` that it holds for the whole call, including the directory listing:
1. Remove every `.*.sql.exchange.tmp` file and every file starting with `metadata_exchange_probe_prefix`.
2. For each `.*.sql.exchange` file:
   - If it doesn't parse, or its name is neither `metadataExchangeRecordPath(old_path)` nor `metadataExchangeRecordPath(new_path)`, log a warning and leave it.
   - If it is a copy, act on it only while the primary exists with identical content. Otherwise remove the copy and continue.
   - If the temporary file is absent, remove the records and do nothing else. Don't check the contents here: Review Focus 2 shows a legitimate state where they differ.
   - If the temporary file is present, `inspectMetadataExchange` must return `Interrupted`. In that case call `finishMetadataExchange` and log the warning `Finished the interrupted exchange of {} and {}`. Otherwise throw `CORRUPTED_DATA` and leave every file as it is.
   - Remove the primary, and the copy if its content equals the primary's.

`DatabaseAtomic::loadTablesMetadata` calls `finishInterruptedMetadataExchanges(getDisk(), getMetadataPath(), fsync, log)` only when `canEmulateExchange(getDisk())` holds, the disk is not read-only, and the directory exists. It then calls `DatabaseOrdinary::loadTablesMetadata`. Take `fsync` from the global context's `fsync_metadata`.

- [ ] Step 1: Write the failing test `atomic_exchange_recovery_at_load.sh`

  The test needs no failpoint and no `no-parallel` tag. Setup:
  - Create database `${CLICKHOUSE_DATABASE}_r ENGINE = Atomic SETTINGS disk = 'default'`.
  - Create tables `a (x UInt8)` and `b (x UInt8, y UInt8)` as `MergeTree ORDER BY x`, and insert one row into each.
  - Read `metadata_path` from `system.databases` before any `DETACH`. Derive disk-relative paths by stripping the `path` of disk `default` from `system.disks`. Save the contents of `a.sql` (`A`) and `b.sql` (`B`).
  - Build records with `clickhouse-local` as `SELECT arrayJoin(['exchange v1', old_path, new_path, old_content, new_content]) FORMAT TSV`. TSV escaping is the same as `writeEscapedString`.

  Each case runs `DETACH DATABASE`, writes files, runs `ATTACH DATABASE`, and then prints three things: `exchanged` or `not exchanged` (from whether `system.columns` shows `y` in table `a`), a sorted list of the dot-files left in the directory, and an empty line. The baseline is restored before the next case. Cases and expected results:
  1. Record only, files from before the exchange: `not exchanged`, no dot-files.
  2. Record, `a.sql` absent, temporary file holding `A`, `b.sql` holding `B`: `exchanged`, no dot-files.
  3. Record, `a.sql` holding `B`, temporary file holding `A`, `b.sql` absent: `exchanged`, no dot-files.
  4. Record, `a.sql` holding `B`, `b.sql` holding `A`: `exchanged`, no dot-files.
  5. A leftover `.a.sql.exchange.tmp` with arbitrary content plus a file `.exchange_probe_test_1`: `not exchanged`, no dot-files.
  6. Review Focus 2: a record for `a` and `b`, no temporary file, `a.sql` holding `A` with one extra column added: loads, `not exchanged`, no dot-files.
  7. Record, temporary file holding `A`, `a.sql` holding a create query with a different UUID: `ATTACH DATABASE` fails, its error output contains `CORRUPTED_DATA`, and the record and the temporary file are still present. Restore the baseline afterwards.
  8. Review Focus 5: two databases `_r1` and `_r2`, a cross-database record for `_r1.a` and `_r2.b`, the primary in `_r1`'s directory, the copy in `_r2`'s directory, the temporary file in `_r1`'s directory, both databases detached. Attach `_r2` first, then `_r1`: `exchanged`, no dot-files in either directory.
  9. A stale copy: the copy for `_r1.a` and `_r2.b` in `_r2`'s directory, and in `_r1`'s directory a primary for a newer same-directory exchange of `_r1.a` and `_r1.c`, interrupted after the first move. Attach `_r2` first, then `_r1`: `_r1.a` and `_r1.c` are exchanged, `_r2.b` is unchanged, and no dot-files remain.

- [ ] Step 2: Run the test on a build of the current branch and confirm it fails

  Run: `tests/clickhouse-test atomic_exchange_recovery_at_load > build/test_atomic_exchange_recovery_at_load.log 2>&1`

  Expected: FAIL. Cases 2 and 3 fail with `INCORRECT_FILE_NAME` on `a.sql.tmp_exchange`, and dot-files are left in several cases.

- [ ] Step 3: Port the record helpers into `MetadataExchange.h` and `MetadataExchange.cpp` with the signatures above, and add `inspectMetadataExchange` and `finishMetadataExchange`

- [ ] Step 4: Implement `finishInterruptedMetadataExchanges` and the `DatabaseAtomic::loadTablesMetadata` override

- [ ] Step 5: Build, restart the server, and run the test

  Expected: PASS. Also run `tests/clickhouse-test 01107_atomic_db_detach_attach 01109_exchange_tables 00002_system_numbers > build/test_task1_regressions.log 2>&1` and expect PASS.

- [ ] Step 6: Commit

  ```bash
  git add src/Databases/MetadataExchange.h src/Databases/MetadataExchange.cpp src/Databases/DatabaseAtomic.h src/Databases/DatabaseAtomic.cpp tests/queries/0_stateless/*_atomic_exchange_recovery_at_load.*
  git commit -m "Finish interrupted metadata exchanges when loading an \`Atomic\` database"
  ```

### Task 2: Probe, exchange with a record, and the gates {#task-2}

Files:
- Modify: `src/Databases/DatabaseAtomic.h`, `src/Databases/DatabaseAtomic.cpp`, `src/Interpreters/InterpreterCreateQuery.cpp`, `src/Common/FailPoint.cpp`
- Test: `tests/queries/0_stateless/<N>_atomic_exchange_with_record.sh` and `.reference`

Interfaces:
- Consumes: everything Task 1 produces.
- Produces:
  - `bool DatabaseAtomic::canExchangeAtomically();` (private) runs the probe at most once, under `std::mutex exchange_probe_mutex`, and caches the answer in `std::optional<bool> exchange_is_atomic TSA_GUARDED_BY(exchange_probe_mutex)`. If the probe throws, nothing is cached. The probe writes two files `metadata_exchange_probe_prefix + <random hex> + "_1"` and `"_2"` in `getMetadataPath()` with `writeMetadataFile(..., false)`, calls `getDisk()->renameExchangeIfSupported` on them, and removes both with `removeFileIfExists` in a `SCOPE_EXIT`.
  - Failpoint `REGULAR(database_atomic_exchange_with_record)`. It is checked after both probes have run, and forces the record path on `DiskLocal`.
  - Debug log line `Exchanging {} and {} with an exchange record`. Tests check it in `system.text_log` to prove the record path ran.

In `DatabaseAtomic::renameTable`:
- Change the gate to `if (exchange && !canEmulateExchange(getDisk()) && !supportsAtomicRename(&message))`.
- After both `waitDatabaseStarted` calls and before the locks, when `exchange && canEmulateExchange(getDisk())`, compute `use_record` as follows: true if `!canExchangeAtomically()`, or if the exchange is cross-database and `!other_db.canExchangeAtomically()`, or if the failpoint is enabled.
- Under the locks, just before the Keeper commit, when `use_record`, build the `MetadataExchangeRecord` from `readMetadataFile` of both paths, with the contents swapped. Write it with `writeMetadataExchangeRecords`.
- Wrap the Keeper commit. If it throws and `use_record`, call `tryRemoveMetadataExchangeRecords` and rethrow the original exception.
- When `use_record`, call the new private `void exchangeMetadataFilesWithRecord(DatabaseAtomic & other_db, const DiskPtr & disk, const MetadataExchangeRecord & record, const Strings & record_paths, bool fsync) TSA_NO_THREAD_SAFETY_ANALYSIS;` in place of `db_disk->renameExchange`. In this task it logs the debug line, performs the three `moveFile` calls from the spec, and then calls `tryRemoveMetadataExchangeRecords`. Task 3 adds the failure handling.

In `InterpreterCreateQuery.cpp`, change the refreshable materialized view gate to skip `supportsAtomicRename` when `database && DatabaseAtomic::canEmulateExchange(database->getDisk())`.

- [ ] Step 1: Write the failing test `atomic_exchange_with_record.sh`

  Tags: `zookeeper, no-parallel`, because failpoints are server-wide and would change other tests' exchanges. The test enables `database_atomic_exchange_with_record` at the start and disables it in a `trap` on exit. It runs everything below in an `Atomic` database and in a `Replicated` database, both with `SETTINGS disk = 'default'`. The `Replicated` path is under `/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/`.

  After each statement it prints which table has column `y`, prints the count of dot-files in the metadata directory (expected `0`), and checks that `system.text_log` has at least one new `Exchanging ... with an exchange record` line for the database. Statements:
  - `EXCHANGE TABLES a AND b`.
  - `EXCHANGE DICTIONARIES d1 AND d2`.
  - `CREATE OR REPLACE TABLE a ... AS SELECT` over an existing `a`.
  - `REPLACE TABLE a ...`.
  - Review Focus 4: a refreshable materialized view `REFRESH EVERY 1 YEAR`, refreshed three times with `SYSTEM REFRESH VIEW` and `SYSTEM WAIT VIEW`. Then `system.tables` has no `_tmp_replace_%` table in the database.
  - A cross-database `EXCHANGE TABLES db1.a AND db2.b`, in `Atomic` only.
  - Review Focus 3: `EXCHANGE TABLES a AND b` twice in a row, after which the original layout is back.
  - Review Focus 1: `EXCHANGE TABLES \`t a\` AND \`t.b\``.
  - Concurrency: in a freshly created database, two `EXCHANGE TABLES` on disjoint pairs, started in the background with `&` and joined with `wait`. Both succeed, and both pairs are exchanged.

- [ ] Step 2: Run the test and confirm it fails

  Expected: FAIL, because the failpoint does not exist yet (`SYSTEM ENABLE FAILPOINT` reports an unknown failpoint).

- [ ] Step 3: Add the failpoint, `canEmulateExchange` use, `canExchangeAtomically`, the record write, the Keeper-commit wrapper, `exchangeMetadataFilesWithRecord` (success path only), and both gate changes

- [ ] Step 4: Build, restart the server, and run the test

  Expected: PASS. Also run `tests/clickhouse-test 01109_exchange_tables 02932_refreshable_materialized_views atomic_exchange_recovery_at_load > build/test_task2_regressions.log 2>&1`, which also selects `02932_refreshable_materialized_views_1` and `_2`. Expect PASS.

- [ ] Step 5: Commit

  ```bash
  git add src/Databases/DatabaseAtomic.h src/Databases/DatabaseAtomic.cpp src/Interpreters/InterpreterCreateQuery.cpp src/Common/FailPoint.cpp tests/queries/0_stateless/*_atomic_exchange_with_record.*
  git commit -m "Exchange metadata files with a record when the filesystem cannot exchange them atomically"
  ```

### Task 3: Roll forward when a move fails, and the fence {#task-3}

Files:
- Modify: `src/Databases/DatabaseAtomic.h`, `src/Databases/DatabaseAtomic.cpp`, `src/Common/FailPoint.cpp`
- Test: `tests/queries/0_stateless/<N>_atomic_exchange_with_record_failures.sh` and `.reference`

Interfaces:
- Consumes: `inspectMetadataExchange`, `finishMetadataExchange`, `tryRemoveMetadataExchangeRecords` (Task 1), and `exchangeMetadataFilesWithRecord` (Task 2).
- Produces:
  - Failpoints `ONCE(database_atomic_exchange_fail_before_first_move)`, `ONCE(database_atomic_exchange_fail_after_first_move)`, `ONCE(database_atomic_exchange_fail_after_second_move)` and `ONCE(database_atomic_exchange_fail_after_third_move)`, each throwing `FAULT_INJECTED` at that point in `exchangeMetadataFilesWithRecord`. The first one is this plan's addition. It is the only way to reach the "nothing moved" branch.
  - `String unfinished_exchange TSA_GUARDED_BY(mutex);` and `void assertNoUnfinishedExchangeUnlocked() const TSA_REQUIRES(mutex);`, ported from `tmp/pr124937.diff` with the same call sites: `detachTable`, `dropTableImpl`, `renameTable` (both databases), `commitCreateTable` and `commitAlterTable`. It throws `UNFINISHED`.
  - Warning `Finished the exchange of {} and {} after an error: {}` for `Interrupted`, and warning `The exchange of {} and {} completed despite an error: {}` for `AfterExchange`.

When a move throws, `exchangeMetadataFilesWithRecord` saves the exception and calls `inspectMetadataExchange`:
- `Interrupted`: call `finishMetadataExchange`, log the first warning, remove the records, and return normally.
- `AfterExchange`: log the second warning, remove the records, and return normally.
- `BeforeExchange`: remove the records and rethrow the saved exception.
- `Inconsistent`, or any exception from the inspection or the roll forward: set `unfinished_exchange` on both databases, keep the records, and rethrow the saved exception.

- [ ] Step 1: Write the failing test `atomic_exchange_with_record_failures.sh`

  Tags: `zookeeper, no-parallel`. Run each case in `Atomic` and in `Replicated`, with `database_atomic_exchange_with_record` enabled:
  - With `..._after_first_move`, `..._after_second_move` or `..._after_third_move` enabled, `EXCHANGE TABLES a AND b` succeeds. The tables are exchanged, no dot-files remain, and `system.text_log` has the matching warning for the database: the first warning for the first two failpoints, the second warning for the third.
  - With `..._before_first_move` enabled, `EXCHANGE TABLES a AND b` fails with `FAULT_INJECTED`. The tables are not exchanged, no dot-files remain, and a following `CREATE TABLE c (x UInt8) ENGINE = Memory` succeeds, which shows that no fence was set.

- [ ] Step 2: Run the test and confirm it fails

  Expected: FAIL, because the new failpoints do not exist.

- [ ] Step 3: Add the failpoints, the failure handling in `exchangeMetadataFilesWithRecord`, and the ported fence

- [ ] Step 4: Build, restart the server, and run the new test together with `atomic_exchange_with_record` and `atomic_exchange_recovery_at_load`

  Expected: PASS for all three.

- [ ] Step 5: Commit

  ```bash
  git add src/Databases/DatabaseAtomic.h src/Databases/DatabaseAtomic.cpp src/Common/FailPoint.cpp tests/queries/0_stateless/*_atomic_exchange_with_record_failures.*
  git commit -m "Roll a metadata exchange forward from its record when a move fails"
  ```

### Task 4: `Replicated` crash states agree with Keeper {#task-4}

Files:
- Test: `tests/queries/0_stateless/<N>_replicated_exchange_recovery_at_load.sh` and `.reference`

Interfaces:
- Consumes: the failpoint `database_atomic_exchange_with_record` (Task 2), the recovery at load (Task 1), and the record format through `clickhouse-local`, as in Task 1's test.

- [ ] Step 1: Write the test `replicated_exchange_recovery_at_load.sh`

  Tags: `zookeeper, no-parallel`. The steps follow the spec's `Replicated` bullets:
  - Create the `Replicated` database with `SETTINGS disk = 'default'` and tables `a` and `b`. Read `metadata_path`.
  - Run a real `EXCHANGE TABLES a AND b` with `database_atomic_exchange_with_record` enabled, then disable it. Save the post-exchange contents of `a.sql` (`B`) and `b.sql` (`A`).
  - For each state, run `DETACH DATABASE` and write the state:
    - after the first move: `a.sql` absent, temporary file holding `A`, `b.sql` holding `B`;
    - after the second move: `a.sql` holding `B`, temporary file holding `A`, `b.sql` absent;
    - after the third move: both files as the exchange left them, plus the record.

    Each state includes the record for the exchange.
  - Take `SELECT now64(6)` as the bound, then run `ATTACH DATABASE`.
  - Poll up to 120 times, with `sleep 0.5` between attempts. Each attempt runs `SYSTEM FLUSH LOGS text_log` and queries `system.text_log`. Stop when a `Trying to initialize replication` message whose `logger_name` contains the database name has `event_time_microseconds` after the bound. If the loop reaches its limit, print `timeout`, which fails the test.
  - Print three results: `local_digest` equals `zk_digest` (`1`), the count of `Replica seems to be lost` messages for the database after the bound (`0`), and `exchanged`.

- [ ] Step 2: Run the test

  Expected: PASS, because the code already exists. Then check that the test is not vacuous. Comment out the `finishInterruptedMetadataExchanges` call in `loadTablesMetadata`, rebuild, restart, and rerun. Expected: FAIL on the digest or `Replica seems to be lost` line, or on `INCORRECT_FILE_NAME`. Restore the call, rebuild, and restart.

- [ ] Step 3: Commit

  ```bash
  git add tests/queries/0_stateless/*_replicated_exchange_recovery_at_load.*
  git commit -m "Test recovery of interrupted exchanges in \`Replicated\` databases"
  ```

### Task 5: Style check, the full new test set, and review {#task-5}

Files:
- No new files.

- [ ] Step 1: Run the style check on the changed files

  Run: `ci/jobs/scripts/check_style/check_cpp.sh > build/style.log 2>&1`. Expected: no findings in the files this branch changed.

- [ ] Step 2: Run the four new tests together, five times, to catch flakiness

  Run: `tests/clickhouse-test atomic_exchange_ replicated_exchange_recovery_at_load --test-runs 5 > build/test_all_new.log 2>&1`. Expected: every run passes.

- [ ] Step 3: Commit any style fixes

  ```bash
  git add -u
  git commit -m "Fix style in the metadata exchange changes"
  ```

### Task 6: Manual check against NFS behavior in Docker {#task-6}

This task needs a Linux build of the branch, which comes from CI once a draft PR exists. Ask the user before pushing the branch or opening the PR. Keep the scripts in `tmp/` and don't commit them.

- [ ] Step 1: After the user approves, push the branch and open a draft PR whose body follows `.github/PULL_REQUEST_TEMPLATE.md`. Include `Closes: https://github.com/ClickHouse/ClickHouse/issues/96835`, `Related:` lines for https://github.com/ClickHouse/ClickHouse/pull/124937, https://github.com/ClickHouse/ClickHouse/pull/113177 and https://github.com/ClickHouse/ClickHouse/issues/83524, and a sentence on which code came from #124937.

- [ ] Step 2: Get the arm64 Linux binary from the PR's build with `node .claude/tools/fetch_ci_report.js "<PR URL>" --links`, and put it in a new empty directory under `tmp/`

- [ ] Step 3: Write `tmp/nfs-seccomp.json`

  ```json
  {"defaultAction": "SCMP_ACT_ALLOW", "syscalls": [{"names": ["renameat2"], "action": "SCMP_ACT_ERRNO", "errnoRet": 22, "args": [{"index": 4, "value": 0, "op": "SCMP_CMP_NE"}]}]}
  ```

- [ ] Step 4: Run the server in an `ubuntu` container with `--security-opt seccomp=tmp/nfs-seccomp.json`

  Run `EXCHANGE TABLES`, `CREATE OR REPLACE TABLE` and a refreshable view refresh. Expected: all succeed, and the server log shows `Exchanging ... with an exchange record` without the failpoint.

- [ ] Step 5: Run a `kill -9` loop

  Run 20 iterations. In each, a client loops `EXCHANGE TABLES a AND b`, the server gets `kill -9` after a random delay of 0 to 500 ms, and the server is restarted. Expected: all 20 restarts succeed, both tables exist, and their UUIDs are distinct.

- [ ] Step 6: Repeat Step 4 without the seccomp profile, with the data directory on a Docker Desktop bind mount from macOS

  Record whether #83524 reproduces on master's binary and whether this branch fixes it. If it does, change the PR's `Related:` line for #83524 to `Closes:`.

### Task 7: Finish the branch {#task-7}

- [ ] Step 1: Remove the plan and the spec, as the user's instructions require at the end of a superpowers plan

  ```bash
  git rm docs/superpowers/plans/2026-10-22-nfs-atomic-exchange.md docs/superpowers/specs/2026-10-22-nfs-atomic-exchange-design.md
  git commit -m "Remove the design and plan for the crash-safe metadata exchange"
  ```

- [ ] Step 2: Tell the user that the PR is ready for their comment on #124937, which notes what was taken from it and offers to merge after it
