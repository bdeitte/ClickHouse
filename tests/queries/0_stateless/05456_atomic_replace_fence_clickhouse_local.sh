#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In `clickhouse-local`, `default` is an `Overlay` database over an `Atomic` one. When a swap of metadata files with a record
# fails and cannot be finished, the fence on its two tables must be checked through the overlay too. The failpoint
# `database_atomic_exchange_not_supported` makes the filesystem report that it cannot exchange files.
# The failpoints are local to each `clickhouse-local` process. Without `--path`, it works in a temporary directory that it
# removes at exit, so the test leaves no files.

echo "with the setting on"
# The first move fails, and so does the finish from the record, so the two tables are fenced.
# A failed `DROP DATABASE` must not have prepared the other tables for shutdown: `OPTIMIZE` of such a table fails with `ABORTED`
# when it has a part to merge.
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE u (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO u VALUES (1);
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_not_supported;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_after_first_move;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_to_finish;
    CREATE OR REPLACE TABLE t (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError FAULT_INJECTED }
    SELECT 'warnings: ' || toString(count()) FROM system.warnings WHERE message LIKE '%could not be finished%';
    CREATE TABLE IF NOT EXISTS t (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    DROP DATABASE default; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    OPTIMIZE TABLE u FINAL;
    SELECT 'u is not shut down';
" -- --database_atomic_allow_exchange_emulation=1

echo "with the setting off"
# The swap is not emulated, and the error mentions the setting.
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_not_supported;
    CREATE OR REPLACE TABLE t (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
" 2>&1 | grep -o -m1 -e 'database_atomic_allow_exchange_emulation' -e 'UNSUPPORTED_METHOD'
