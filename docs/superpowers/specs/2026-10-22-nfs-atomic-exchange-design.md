---
description: 'Design for crash-safe EXCHANGE in Atomic databases on filesystems without renameat2(RENAME_EXCHANGE), such as NFS'
sidebarTitle: 'NFS atomic exchange design'
slug: /superpowers/specs/2026-10-22-nfs-atomic-exchange-design
title: 'Crash-safe EXCHANGE on filesystems without atomic exchange'
doc_type: 'guide'
---

# Crash-safe `EXCHANGE` on filesystems without atomic exchange {#crash-safe-exchange}

Date: 2026-10-22. Branch: `nfs-atomic-exchange`. Issue: https://github.com/ClickHouse/ClickHouse/issues/96835

## Intent {#intent}

`EXCHANGE TABLES`, `EXCHANGE DICTIONARIES`, `CREATE OR REPLACE` and `REPLACE` over an existing table, and refreshes of refreshable materialized views all fail in `Atomic` and `Replicated` databases when the metadata directory is on NFS, EFS, CephFS, WEKA, many FUSE filesystems, or ZFS before 2.2. The error is `System call renameat2() is not supported. (UNSUPPORTED_METHOD)`. Users on Kubernetes with network storage cannot use these statements at all, and third-party tools issue `CREATE OR REPLACE` routinely.

The fix makes these statements work on such filesystems while keeping the guarantee that the docs state: the exchange is atomic. On a filesystem without an atomic exchange, ClickHouse emulates it with three renames and a durable record that lets it finish an interrupted exchange, both during the query and at the next load. A crash at any point leaves either the state before the exchange or the state after it, never a mix.

Alexey Milovidov set this direction on the issue on 2026-02-13: "I don't like falling back to a non-atomic version, because it can introduce bugs in production systems. Instead, we can emulate it with a heavy but still atomic sequence of operations, involving an ad-hoc undo/redo log."

## Background {#background}

### Why the exchange fails {#why-it-fails}

`DatabaseAtomic::renameTable` (`src/Databases/DatabaseAtomic.cpp`) exchanges the two `.sql` metadata files with `IDisk::renameExchange`. `DiskLocal` calls `DB::renameExchange` in `src/Common/atomicRename.cpp`, which calls `renameat2(..., RENAME_EXCHANGE)` and throws `UNSUPPORTED_METHOD` when the call returns `EINVAL` or `ENOSYS`.

The Linux NFS client rejects every `renameat2` flag: `fs/nfs/dir.c` in current Linux master has `if (flags) return -EINVAL;` at the top of `nfs_rename`. The NFS protocol's `RENAME` has no exclusive or exchange variant, so this cannot change on the client alone. A plain replacing `rename` is atomic on NFS.

`supportsAtomicRename` only checks the kernel version (3.15 or later). It passes on NFS, so the failure happens at the syscall.

The table name in `Atomic` metadata is the file name. The file contents are `ATTACH TABLE _ UUID '...'`. So after two of the three renames, the second table's original name is recorded in no file, and a loader rule alone cannot repair the state. A record written before the renames is required.

### Related work {#related-work}

- https://github.com/ClickHouse/ClickHouse/pull/124937 (`groeneai`, open, changes requested) writes such a record and replays it at load, but only for object storage database disks. Reviewer `Michicosun` asked that disks not know about tables or exchanges, so the logic lives in `DatabaseAtomic`. This design takes its record format, its loader, and its fence, and differs in the gate and in how a failed move is handled.
- https://github.com/ClickHouse/ClickHouse/pull/113177 (`nickitat`, open, no human review since 2026-09-04) does three renames with rollback and no crash recovery. The AI reviewer blocked it for weakening the documented atomicity. This design does not build on it.
- https://github.com/ClickHouse/ClickHouse/pull/115725 (open) wants a startup check that reflects what the filesystem supports. https://github.com/ClickHouse/ClickHouse/pull/111405 (open) adds directory fsync after DDL renames.
- https://github.com/ClickHouse/ClickHouse/issues/83524 reports the same failure under Docker Desktop on macOS with a bind mount. This change may fix it too. Verify before claiming it.

## Goals {#goals}

- The statements listed in the intent work in `Atomic` and `Replicated` databases whose metadata is on a `DiskLocal` disk that cannot exchange atomically.
- A crash or a `kill -9` at any point leaves the two tables either both exchanged or both not exchanged after the next load.
- No new setting. The emulation is on whenever the filesystem needs it.
- Filesystems that support `RENAME_EXCHANGE` keep using the single syscall, with no extra I/O per exchange beyond a one-time probe.

## Non-goals {#non-goals}

- Object storage database disks. They keep master behavior, and #124937 covers them.
- `renameNoReplace` and its racy `fs::exists` + `fs::rename` fallback. Every DDL rename runs under the database mutex, and several servers writing one metadata directory is unsupported, so the race does not affect this case. A `linkat`-based rewrite can be a later PR.
- `FormatSchemaInfo::storeSchemaOnDisk`, the other caller of `DB::renameExchange`.
- Atomicity against processes other than this server. Exchanges within one server are already serialized by the database mutex.
- Changing which errno values count as "unsupported" in `atomicRename.cpp`.

## Anti-goals {#anti-goals}

- A non-atomic exchange in any form, including one that is opt-in. That is the approach Alexey and the AI reviewer rejected.
- A fallback that hides an error. A probe that fails for any reason other than "unsupported" fails the query.
- Changing the semantics of the shared helpers in `atomicRename.cpp`. `DB::renameExchange` stays fail-closed for its other callers.
- An integration test, if a stateless test can cover the behavior.

## Constraints {#constraints}

- Follow the repository's `CLAUDE.md` rules: Allman braces, no fallback paths that hide errors, no sleeps for races, prefer stateless tests, merge rather than rebase.
- The record is written durably before the `Replicated` Keeper commit point in `renameTable`, so nothing new opens between the commit and the renames.
- Writes of the record and of recovered metadata honor `fsync_metadata`. If #111405 merges first, follow its directory-fsync pattern.
- The doc page `docs/reference/engines/database-engines/atomic.mdx` is autogenerated and promises an atomic exchange. That promise remains true, so no docs change is needed.

## Design {#design}

### Detecting a filesystem without atomic exchange {#probe}

Decided: a probe, not "try the syscall first and fall back". Trying first would put the record after the Keeper commit.

- The probe runs per database, in the database's metadata directory, the first time that database performs an exchange. It runs before `renameTable` takes the database mutexes. The answer is cached for the lifetime of the database object. A per-database probe is more precise than a per-disk one because a metadata directory can be on a different mount from the disk root.
- The probe creates two small dot-prefixed scratch files in the metadata directory, calls `IDisk::renameExchangeIfSupported` on them, and removes them. It removes leftovers from an earlier crashed probe first. `DatabaseOnDisk::iterateMetadataFiles` skips dot-prefixed names on every disk, so a leftover never breaks loading.
- `renameExchangeIfSupported` already returns `false` for "unsupported" and throws for every other error. The probe lets those errors propagate.
- The probe runs only when the database disk is a `DiskLocal`. Other disks keep master behavior.
- For an exchange between two databases, both databases are probed. The record path is used if either one reports "unsupported".
- A new `REGULAR` failpoint makes the probe report "unsupported" whatever the cached answer is. It exists for tests.

### Exchanging with a record {#exchange-with-record}

When the probe reports "unsupported", `renameTable` performs these steps in place of `db_disk->renameExchange`:

1. Before the Keeper commit, it writes the record `.<file>.exchange` next to `old_path`, plus a copy next to `new_path` when the two databases' directories differ. The format is #124937's: the header line `exchange v1`, then `old_path`, `new_path`, and the contents each path will have after the exchange, each escaped and on its own line. Each record is written as `<record>.tmp` and then put in place with `replaceFile`.
2. The existing Keeper commit runs.
3. Three moves follow through `db_disk->moveFile`: `old_path` to `old_path + ".tmp_exchange"`, `new_path` to `old_path`, and the temporary file to `new_path`.
4. The records are removed. This step is best effort. A record without its temporary file is harmless and gets removed at the next load.

### A failure during the moves {#failure-during-moves}

This is where the design differs from #124937, which undoes the moves. After the Keeper commit, rolling forward is always the right direction, because that is what Keeper has recorded and what a restart produces. On NFS a rename can also report an error after it succeeded, when the server handles a retransmitted request. So when any move throws, `renameTable` reads the files and decides by their state:

- The temporary file exists: roll forward from the record. Write both recorded contents with `.tmp` files and `replaceFile`, remove the temporary file, and log a warning that includes the original exception. The exchange counts as done, and `renameTable` continues with the in-memory swap.
- No temporary file, and both files hold the contents after the exchange: the renames succeeded despite the error. Log a warning and continue as done.
- No temporary file, and both files hold the contents before the exchange: nothing moved. Remove the records and rethrow the original exception. This matches master, where `renameExchange` throwing after the commit fails the query.
- Anything else, including a failure while reading the files or rolling forward: keep the records, set #124937's in-memory fence on both databases so DDL on them fails with `UNFINISHED` until a restart, and rethrow. The fence is reachable only when the recovery itself fails. Align its details with whatever #124937 settles on.

### Finishing an exchange at load {#recovery-at-load}

`DatabaseAtomic` overrides `loadTablesMetadata` to run `finishInterruptedExchanges` before `DatabaseOrdinary::loadTablesMetadata`, as #124937 does. The rules are the same as theirs:

- A leftover `<record>.tmp` belongs to an exchange that never began. Remove it.
- For each record, if the temporary file exists, check that each of `old_path` and `new_path` is either absent or holds one of the two recorded contents. If one holds neither, throw `CORRUPTED_DATA`. Otherwise write both recorded contents, remove the temporary file, and log the warning `Finished the interrupted exchange of ...`.
- Remove the record and its copy.

With `async_load_databases = true`, the two databases of a cross-database exchange can load at the same time. The AI review on #124937 found that their version races in that case. Serialize the recovery with a process-wide mutex, and make each step tolerate a record or temporary file that the other database's recovery already removed.

The rules for a failure during the moves and for recovery at load share their logic. Write it once.

### Removing the kernel-version gates {#remove-gates}

The probe answers the question that `supportsAtomicRename` approximates. Remove these two checks:

- The `NOT_IMPLEMENTED` throw for `exchange` in `DatabaseAtomic::renameTable`.
- The `NOT_IMPLEMENTED` throw for refreshable materialized views in `InterpreterCreateQuery` (search for "Can't create refreshable materialized view because exchanging files is not supported").

On an old kernel or a platform other than Linux or macOS, `renameExchangeIfSupported` returns `false`, so those systems use the record path too. Keep `supportsAtomicRename` itself.

### Compatibility {#compatibility}

No setting, protocol, or wire format changes. An older server ignores the dot-prefixed records. After a downgrade that follows a crash during an exchange, an older server fails to load on the leftover `<file>.tmp_exchange` with `INCORRECT_FILE_NAME`. That is the same failure master has today on object storage disks, and it is accepted.

## Testing and verification {#testing}

There is no real NFS or EFS mount available. Verification relies on failpoints and on a filesystem that rejects the flags.

- Failpoints: the probe failpoint above, and `ONCE` failpoints that throw after the first, second, and third move. The third one simulates an NFS false error after all renames succeeded.
- Stateless `.sh` tests, created with `tests/queries/0_stateless/add-test`, with the probe failpoint enabled:
  - Each statement in the intent works and leaves no record behind. Run them in an `Atomic` database, in a `Replicated` database, and across two databases.
  - Each move failpoint leaves the query successful with the tables exchanged.
  - Each crash state recovers on `DETACH DATABASE` and `ATTACH DATABASE`: record only, record with the temporary file after each move, a leftover `<record>.tmp`, and a file matching neither content. Build the states by hand as files, using #124937's `05340` test as the model.
- These tests run on the native macOS build, because the failpoint forces the emulated path. APFS supports the exchange natively, so the path never runs there without the failpoint.
- A manual check before the PR leaves draft: run a CI-built Linux binary in Docker on the Mac with a seccomp profile that returns `EINVAL` for `renameat2` with any non-zero flags, which reproduces NFS behavior for both `RENAME_EXCHANGE` and `RENAME_NOREPLACE`. In that container, run the statements, then a `kill -9` loop during `EXCHANGE TABLES` with a restart after each kill, and confirm that every restart succeeds. Also try a Docker Desktop bind mount to see whether #83524 is fixed.

## PR {#pr}

- Use `.github/PULL_REQUEST_TEMPLATE.md`, with `Closes: https://github.com/ClickHouse/ClickHouse/issues/96835`, and `Related:` lines for #124937, #113177, and #83524. Use `Closes:` for #83524 instead if the manual check shows it is fixed.
- The PR description says which code came from #124937.
- After the PR exists, the user comments on #124937, noting what was taken and offering to merge after it.

## Left to the builder {#left-to-builder}

- Names of the failpoints, the probe files, and new methods.
- How the `DiskLocal` check is written.
- Where the cached probe answer lives in `DatabaseAtomic`, and how it is synchronized.
- How tests inspect metadata files on disk, and how the tests are split.
- The changelog category.

## Decisions {#decisions}

Decided by the user:
- Detect the filesystem with a probe.
- A new PR, building on #124937 where useful. The user will comment on #124937.
- Verify without a real NFS mount.

Calls made in this document, for the user to check:
- Probe per database in its metadata directory, lazily, cached for the life of the database object.
- Only `DiskLocal`. Object storage stays as on master.
- Roll forward on a failed move, instead of #124937's undo. The fence remains only for a failed roll forward.
- No new setting.
- Remove the two kernel-version gates.
- `renameNoReplace`, `FormatSchemaInfo`, and errno changes are out of scope.
- Process-wide mutex for recovery at load.
- A seccomp profile in Docker as the manual stand-in for NFS.
