#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A swap of metadata files with a record renames the tables in memory only after its record is removed, so the load at the restart
# undoes every swap whose record is left, with or without the temporary file. When a swap fails after its files were exchanged,
# or cannot remove its record, it fences its two tables and leaves the record, and the restart undoes it. A record left by a
# failed write is removed, and that swap fails without a fence. When the removal of the record cannot be made durable, it is
# not known whether the record is there at the restart, and a restart that does not find it keeps the files exchanged.
# Each run of `clickhouse-local` on the same path is a start of the server, and its failpoints are local to it.
# The failpoint `database_atomic_exchange_not_supported` makes the filesystem report that it cannot exchange files.

LOCAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
# With a fixed UUID, the metadata directory of the database is `store/<first three characters of the UUID>/<UUID>/`.
UUID="5458a70c-0000-4000-8000-000000000001"
# The database of the swap whose record may be gone at the restart, separate so that its temporary table is told apart.
OTHER_UUID="5458a70c-0000-4000-8000-000000000002"
DIR="${LOCAL_PATH}/store/${UUID:0:3}/${UUID}"
OTHER_DIR="${LOCAL_PATH}/store/${OTHER_UUID:0:3}/${OTHER_UUID}"
if [ -z "$CLICKHOUSE_TMP" ] || [ -z "$CLICKHOUSE_TEST_UNIQUE_NAME" ] \
    || [[ "$DIR" != "$CLICKHOUSE_TMP"/*/store/* ]] || [[ "$OTHER_DIR" != "$CLICKHOUSE_TMP"/*/store/* ]]
then
    echo "Unexpected metadata directories '$DIR' and '$OTHER_DIR'" >&2
    exit 1
fi

# Runs the queries $2 in a start with the server setting `database_atomic_allow_exchange_emulation` set to $1.
function local_query()
{
    $CLICKHOUSE_LOCAL --path "$LOCAL_PATH" -q "$2" -- --database_atomic_allow_exchange_emulation="$1"
}

# Prints the number of dot-files, such as records of swaps, and of temporary files of swaps in the metadata directory $1.
function print_swap_files()
{
    # shellcheck disable=SC2010
    echo "dot-files: $(ls -A "$1" | grep -c '^\.')"
    # shellcheck disable=SC2010
    echo "temporary files: $(ls "$1" | grep -c '\.tmp_exchange$')"
}

echo "the start that fences"
local_query 1 "
    CREATE DATABASE d UUID '$UUID' ENGINE = Atomic;
    CREATE TABLE d.t1 (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.t2 (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.t3 (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.t4 (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO d.t1 VALUES (1);
    INSERT INTO d.t2 VALUES (1);
    INSERT INTO d.t3 VALUES (1);
    INSERT INTO d.t4 VALUES (1);
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_not_supported;

    -- The first move fails, and the swap is finished from the record, but an error after the finish, as when the removal
    -- of the temporary file reports an error after it succeeded, fences the swap without the temporary file.
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_after_first_move;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_after_finish;
    CREATE OR REPLACE TABLE d.t1 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 2, 3; -- { serverError FAULT_INJECTED }
    INSERT INTO d.t1 VALUES (5);
    ALTER TABLE d.t1 ADD COLUMN z UInt8; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    SELECT 't1: the error says what the restart does: '
        || toString(last_error_message LIKE '%The record % is left, so the next load undoes the exchange%')
    FROM system.errors WHERE name = 'UNFINISHED_METADATA_EXCHANGE';

    -- The moves complete, but the record cannot be removed.
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_to_remove_record;
    CREATE OR REPLACE TABLE d.t2 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 2, 3; -- { serverError FAULT_INJECTED }
    INSERT INTO d.t2 VALUES (5);
    ALTER TABLE d.t2 ADD COLUMN z UInt8; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    SELECT 't2: the error says what the restart does: '
        || toString(last_error_message LIKE '%The two files were exchanged, but their record % could not be removed, so the next load undoes the exchange%')
    FROM system.errors WHERE name = 'UNFINISHED_METADATA_EXCHANGE';

    -- The moves fail twice, so the swap fails without a change to the files, but the record cannot be removed either.
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move;
    CREATE OR REPLACE TABLE d.t3 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 2, 3; -- { serverError FAULT_INJECTED }
    SYSTEM DISABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move;
    SYSTEM DISABLE FAILPOINT database_atomic_exchange_fail_to_remove_record;
    INSERT INTO d.t3 VALUES (5);
    ALTER TABLE d.t3 ADD COLUMN z UInt8; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    SELECT 't3: the error says what the restart does: '
        || toString(last_error_message LIKE '%The two files were not exchanged, but their record % could not be removed, so the next load undoes the exchange%')
    FROM system.errors WHERE name = 'UNFINISHED_METADATA_EXCHANGE';

    -- The write of the record fails after the record is in place. The swap removes the record, so it fails without a fence.
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_after_writing_record;
    CREATE OR REPLACE TABLE d.t4 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 2, 3; -- { serverError FAULT_INJECTED }
    ALTER TABLE d.t4 ADD COLUMN z UInt8;
    SELECT 't4 is not fenced';

    -- The moves complete and the record is removed, but the removal cannot be made durable, so it is not known whether the
    -- record is there at the restart. The sync runs only with fsync_metadata, which the test runner randomizes.
    SET fsync_metadata = 1;
    CREATE DATABASE e UUID '$OTHER_UUID' ENGINE = Atomic;
    CREATE TABLE e.t5 (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO e.t5 VALUES (1);
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_to_sync_removed_record;
    CREATE OR REPLACE TABLE e.t5 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 2, 3; -- { serverError FAULT_INJECTED }
    INSERT INTO e.t5 VALUES (5);
    ALTER TABLE e.t5 ADD COLUMN z UInt8; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    SELECT 't5: the error says what the restart does: '
        || toString(last_error_message LIKE '%it is not known whether their record % is there at the restart%If it is not, the next load keeps the two files exchanged%')
    FROM system.errors WHERE name = 'UNFINISHED_METADATA_EXCHANGE';
"
# The three fenced swaps of `d` left their records, and none left a temporary file. The swap of `e` removed its record.
print_swap_files "$DIR"
print_swap_files "$OTHER_DIR"

echo
echo "the start that undoes the swaps, with the setting off"
local_query 0 "
    SELECT 'warnings: ' || toString(count()) FROM system.warnings WHERE message LIKE '%could not be finished%';
    -- Each table is the one from before its swap, with the row inserted under the fence.
    SELECT table || ': ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ',')
    FROM system.columns WHERE database = 'd' AND table IN ('t1', 't2', 't3', 't4') GROUP BY table ORDER BY table;
    SELECT 't1 rows: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT x FROM d.t1 ORDER BY x);
    SELECT 't2 rows: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT x FROM d.t2 ORDER BY x);
    SELECT 't3 rows: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT x FROM d.t3 ORDER BY x);
    SELECT 't4 rows: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT x FROM d.t4 ORDER BY x);
    -- The fences refused the cleanup DROP of the temporary tables, so the replacement tables of the three fenced swaps are
    -- left under their temporary names, with the row of their AS SELECT. The temporary table of the swap of t4 was dropped.
    SELECT 'replacement tables: ' || toString(count()) FROM system.tables WHERE database = 'd' AND startsWith(name, '_tmp_replace_');
    SELECT 'replacement rows: ' || arrayStringConcat(groupArray(r), ' ')
    FROM (SELECT formatRowNoNewline('CSV', x, y) AS r FROM merge('d', '^_tmp_replace_'));
    -- The fences are gone.
    ALTER TABLE d.t1 ADD COLUMN z UInt8;
    ALTER TABLE d.t2 ADD COLUMN z UInt8;
    ALTER TABLE d.t3 ADD COLUMN z UInt8;
    SELECT 'altered: ' || toString(count()) FROM system.columns WHERE database = 'd' AND table IN ('t1', 't2', 't3') AND name = 'z';
    -- The record of the swap of t5 is gone, so the load kept the files exchanged: t5 is the replacement table, and the row
    -- inserted under the fence is in the replaced table under its temporary name, as the error of the fence says.
    SELECT 't5: ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ',')
    FROM system.columns WHERE database = 'e' AND table = 't5';
    SELECT 't5 rows: ' || formatRowNoNewline('CSV', *) FROM e.t5;
    SELECT 'replaced table rows: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT x FROM merge('e', '^_tmp_replace_') ORDER BY x);
    ALTER TABLE e.t5 ADD COLUMN z UInt8;
"
print_swap_files "$DIR"
print_swap_files "$OTHER_DIR"
