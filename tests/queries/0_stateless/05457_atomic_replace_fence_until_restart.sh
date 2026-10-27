#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a swap of metadata files with a record fails and cannot be finished, DDL on its two tables and reads of their
# definitions are fenced until the restart, and the load at the restart undoes the swap whatever the server setting is.
# A last start with the setting off checks that a swap is then not emulated and leaves nothing behind.
# Each run of `clickhouse-local` on the same path is a start of the server, and its failpoints are local to it.
# The failpoint `database_atomic_exchange_not_supported` makes the filesystem report that it cannot exchange files.
# `stop_refreshable_materialized_views_on_startup` keeps the view from refreshing when a start loads it, because its
# definition does not keep `EMPTY`.

LOCAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
# With fixed UUIDs, the metadata directory of the database is `store/<first three characters of the UUID>/<UUID>/`,
# and the inner table of the view is `.inner_id.<UUID of the view>`.
UUID="5457a70c-0000-4000-8000-000000000001"
VIEW_UUID="5457a70c-0000-4000-8000-000000000002"
# The database of the start with the setting off, separate so that the tables left by the fence do not count.
OFF_UUID="5457a70c-0000-4000-8000-000000000003"
INNER=".inner_id.${VIEW_UUID}"
TMP_INNER=".tmp.inner_id.${VIEW_UUID}"
DIR="${LOCAL_PATH}/store/${UUID:0:3}/${UUID}"
OFF_DIR="${LOCAL_PATH}/store/${OFF_UUID:0:3}/${OFF_UUID}"
ERRORS="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_errors"
if [ -z "$CLICKHOUSE_TMP" ] || [ -z "$CLICKHOUSE_TEST_UNIQUE_NAME" ] \
    || [[ "$DIR" != "$CLICKHOUSE_TMP"/*/store/* ]] || [[ "$OFF_DIR" != "$CLICKHOUSE_TMP"/*/store/* ]]
then
    echo "Unexpected metadata directories '$DIR' and '$OFF_DIR'" >&2
    exit 1
fi

# Runs the queries $2 in a start with the server setting `database_atomic_allow_exchange_emulation` set to $1.
function local_query()
{
    $CLICKHOUSE_LOCAL --path "$LOCAL_PATH" --stop_refreshable_materialized_views_on_startup=1 "${@:3}" -q "$2" \
        -- --database_atomic_allow_exchange_emulation="$1"
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
# The first move fails, and so does the finish from the record, so the two tables of each swap are fenced.
# `--ignore-error` keeps going after the error of the swap, which is the only query without an expected error,
# so that the error is checked below.
local_query 1 "
    CREATE DATABASE d UUID '$UUID' ENGINE = Atomic;
    CREATE TABLE d.t (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO d.t VALUES (1);
    -- Not part of a swap: the fence does not cover it.
    CREATE TABLE d.u (x UInt8) ENGINE = MergeTree ORDER BY x;
    -- A refresh of a view with an inner table that is not APPEND swaps the .tmp table it fills with the inner table.
    -- EMPTY skips the first refresh. Started, the view schedules its next refresh at the start of the next year.
    CREATE TABLE d.src (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO d.src VALUES (7);
    CREATE MATERIALIZED VIEW d.mv UUID '$VIEW_UUID' REFRESH EVERY 1 YEAR SETTINGS refresh_retries = 0
        (x UInt8) ENGINE = MergeTree ORDER BY x EMPTY AS SELECT x FROM d.src;

    SYSTEM ENABLE FAILPOINT database_atomic_exchange_not_supported;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_after_first_move;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_to_finish;
    CREATE OR REPLACE TABLE d.t (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 2, 3;

    -- The fence is also shown to queries other than the one that hit it.
    SELECT 'warnings: ' || toString(count()) FROM system.warnings WHERE message LIKE '%could not be finished%';
    DROP TABLE d.t; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    ALTER TABLE d.t ADD COLUMN z UInt8; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    SHOW CREATE TABLE d.t; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    BACKUP TABLE d.t TO Memory('fenced'); -- { serverError UNFINISHED_METADATA_EXCHANGE }
    -- The other name of the swap is the temporary table of CREATE OR REPLACE, whose name is random.
    TRUNCATE TABLES FROM d LIKE '_tmp_replace_%'; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    DROP DATABASE d; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    DETACH DATABASE d; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    TRUNCATE DATABASE d; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    SELECT 'the error of the fence refers to the error of the swap: '
        || toString(last_error_message LIKE '%until the server restarts%The error of the query that ran the exchange%')
    FROM system.errors WHERE name = 'UNFINISHED_METADATA_EXCHANGE';
    -- The definitions of the two tables cannot be read.
    SELECT 'unreadable definitions: ' || toString(count()) FROM system.tables
    WHERE database = 'd' AND (name = 't' OR name LIKE '_tmp_replace_%') AND create_table_query = '';

    -- The fence does not refuse inserts, and DDL on the other tables of the database goes on.
    INSERT INTO d.t VALUES (5);
    ALTER TABLE d.u ADD COLUMN z UInt8;
    -- The guard refused the DROP TABLE of t before it shut t down: a MergeTree table that was prepared for shutdown
    -- refuses OPTIMIZE with ABORTED.
    OPTIMIZE TABLE d.t FINAL;

    SYSTEM START VIEW d.mv;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_fail_after_first_move;
    SYSTEM REFRESH VIEW d.mv;
    SYSTEM WAIT VIEW d.mv; -- { serverError REFRESH_FAILED }
    SELECT 'the error of the refresh says what the restart does: ' || toString(exception LIKE '%until the server restarts%')
    FROM system.view_refreshes WHERE database = 'd' AND view = 'mv';
    -- The fence is on the two inner tables, not on the view, but DROP VIEW drops the inner tables, so it is refused
    -- before it shuts the view down. A view that DROP shut down is gone from system.view_refreshes.
    DROP TABLE d.\"$INNER\"; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    DROP TABLE d.\"$TMP_INNER\"; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    DROP VIEW d.mv; -- { serverError UNFINISHED_METADATA_EXCHANGE }
    SELECT 'refreshing views: ' || toString(count()) FROM system.view_refreshes WHERE database = 'd' AND view = 'mv';
" --ignore-error 2>"$ERRORS"
echo "errors without an expected code: $(grep -c '^Code: ' "$ERRORS")"
echo "the error of the swap is FAULT_INJECTED: $(grep -c 'FAULT_INJECTED' "$ERRORS")"
echo "the error of the swap says what the restart does: $(grep -c 'until the server restarts.*the next load undoes the exchange' "$ERRORS")"
# Each of the two swaps stopped after its first move, and left its temporary file and its record.
print_swap_files "$DIR"

echo
echo "the start that undoes the swaps, with the setting off"
local_query 0 "
    SELECT 'warnings: ' || toString(count()) FROM system.warnings WHERE message LIKE '%could not be finished%';
    -- The load undid the swap of CREATE OR REPLACE, so t is the table from before it, with the row inserted under the fence.
    SELECT 't: ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ',')
    FROM system.columns WHERE database = 'd' AND table = 't';
    SELECT 't rows: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT x FROM d.t ORDER BY x);
    -- The fence refused the cleanup DROP of the temporary table, so the replacement table is still there under the temporary
    -- name, with the row of its AS SELECT.
    SELECT 'replacement table: ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ',')
    FROM system.columns WHERE database = 'd' AND startsWith(table, '_tmp_replace_');
    SELECT 'replacement table rows: ' || formatRowNoNewline('CSV', *) FROM merge('d', '^_tmp_replace_');
    -- The fence is gone.
    ALTER TABLE d.t ADD COLUMN z UInt8;
    TRUNCATE TABLES FROM d LIKE '_tmp_replace_%';
    SELECT 'replacement table rows after TRUNCATE: ' || toString(count()) FROM merge('d', '^_tmp_replace_');
    -- The load also undid the swap of the refresh: the view still reads its inner table from before the refresh, which was
    -- empty, and the .tmp table that the fence kept holds the refreshed row.
    SELECT 'mv rows: ' || toString(count()) FROM d.mv;
    SELECT 'leftover .tmp table rows: ' || toString(count()) FROM d.\"$TMP_INNER\";
    -- The first refresh after the restart replaces the leftover .tmp table, as the error of the fence says.
    SYSTEM START VIEW d.mv;
    SYSTEM REFRESH VIEW d.mv;
    SYSTEM WAIT VIEW d.mv;
    SELECT 'mv rows after a refresh: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM d.mv;
    -- DROP VIEW drops the inner table, and the .tmp table if it is left.
    DROP VIEW d.mv SYNC;
    SELECT 'inner tables: ' || toString(count()) FROM system.tables WHERE database = 'd' AND startsWith(name, '.');
"
print_swap_files "$DIR"

echo
echo "a start with the setting off, where a swap is not emulated"
# On a filesystem without an atomic exchange, CREATE OR REPLACE fails, mentions the setting, and leaves nothing behind.
local_query 0 "
    CREATE DATABASE e UUID '$OFF_UUID' ENGINE = Atomic;
    CREATE TABLE e.v (x UInt8) ENGINE = MergeTree ORDER BY x;
    SYSTEM ENABLE FAILPOINT database_atomic_exchange_not_supported;
    CREATE OR REPLACE TABLE e.v (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 2, 3; -- { serverError UNSUPPORTED_METHOD }
    SELECT 'the error mentions the setting: ' || toString(last_error_message LIKE '%database_atomic_allow_exchange_emulation%')
    FROM system.errors WHERE name = 'UNSUPPORTED_METHOD';
    -- That message is only for the setting on.
    SELECT 'the error says the swap is not emulated: ' || toString(last_error_message LIKE '%are not emulated%')
    FROM system.errors WHERE name = 'UNSUPPORTED_METHOD';
    SELECT 'v: ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ',')
    FROM system.columns WHERE database = 'e' AND table = 'v';
    SELECT 'temporary tables: ' || toString(count()) FROM system.tables WHERE database = 'e' AND name LIKE '_tmp_replace_%';
"
print_swap_files "$OFF_DIR"
