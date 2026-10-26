#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the failpoints are server-wide and would change the swaps of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a move of a swap with a record fails, the swap reads the files and compares them with the record:
# it finishes an interrupted swap, accepts one that completed despite the error, and runs the moves once more
# when nothing moved. A failed retry fails the query, and it leaves nothing behind.
# The failpoint `database_atomic_exchange_not_supported` makes the filesystem report that it cannot exchange files.

DB="${CLICKHOUSE_DATABASE}_atomic"

# The warnings of the swaps are checked in `system.text_log`, so they are not sent to the client.
CLIENT="$CLICKHOUSE_CLIENT --send_logs_level=error"

FAILPOINTS=(
    database_atomic_exchange_not_supported
    database_atomic_exchange_fail_before_first_move
    database_atomic_exchange_fail_after_first_move
    database_atomic_exchange_fail_after_second_move
    database_atomic_exchange_fail_after_third_move
    database_atomic_exchange_always_fail_before_first_move
)

function cleanup()
{
    for failpoint in "${FAILPOINTS[@]}"
    do
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"
    done
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_not_supported"

DISK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")

# Sets the variable named $2 to the absolute metadata directory of database $1.
# Exits when it is not a database directory, so that no path below can be empty.
function set_metadata_dir()
{
    local rel
    rel=$($CLICKHOUSE_CLIENT -q "SELECT metadata_path FROM system.databases WHERE name = '$1'")
    local abs="${DISK_PATH%/}/${rel}"
    if [ -z "$DISK_PATH" ] || [ -z "$rel" ] || [[ "$abs" != */store/* ]]
    then
        echo "Cannot find the metadata directory of $1: disk path '$DISK_PATH', metadata path '$rel'" >&2
        exit 1
    fi
    printf -v "$2" '%s' "$abs"
}

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $DB ENGINE = Atomic;
    CREATE TABLE $DB.t (x UInt8) ENGINE = MergeTree ORDER BY x;
"
set_metadata_dir "$DB" DIR

# Swaps never use a record when the server setting turns the emulation off, or when the metadata of the database
# is not on the local filesystem (for example, a `database_disk` on object storage). The directory of a database exists
# once it has a table.
EMULATION=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'database_atomic_allow_exchange_emulation'")
if [ "$EMULATION" != "1" ] || [ ! -d "$DIR" ]
then
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

function now_us()
{
    $CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))"
}

# Prints the columns of `t`.
function print_columns()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT 't columns: ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ', ')
        FROM system.columns WHERE database = '$DB' AND table = 't'"
}

# Prints whether the metadata file `t.sql` on disk defines column `y`.
function print_file_has_y()
{
    if grep -q '`y` UInt8' "$DIR/t.sql"
    then
        echo "on disk has y: yes"
    else
        echo "on disk has y: no"
    fi
}

# Prints the number of dot-files, such as records of swaps, and of files that are not metadata files,
# such as a temporary file of a swap, in the metadata directory, and the number of temporary tables of `CREATE OR REPLACE`.
function print_leftovers()
{
    # shellcheck disable=SC2010
    echo "dot-files: $(ls -A "$DIR" | grep -c '^\.')"
    # shellcheck disable=SC2010
    echo "other files: $(ls "$DIR" | grep -vc '\.sql$')"
    $CLICKHOUSE_CLIENT -q "SELECT 'temporary tables: ' || toString(count()) FROM system.tables WHERE database = '$DB' AND name LIKE '_tmp_replace_%'"
}

# Prints whether the database logged a message like $1 since the time $2.
function print_warning()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT if(count() > 0, 'logged: ', 'not logged: ') || '$1'
        FROM system.text_log
        WHERE event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) >= $2
            AND endsWith(logger_name, '($DB)') AND level = 'Warning' AND message LIKE '$1'"
}

HAS_Y=0

# The columns that `REPLACE TABLE` gives `t` when it toggles column `y`.
function replacement_columns()
{
    if [ "$HAS_Y" = "0" ]
    then
        echo "x UInt8, y UInt8"
    else
        echo "x UInt8"
    fi
}

# Runs `REPLACE TABLE` of `t` while the failpoint $1 is enabled, and checks that it succeeded with the warning $2.
function replace_with_failpoint()
{
    echo "$1"
    local since
    since=$(now_us)
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $1"
    $CLIENT -q "REPLACE TABLE $DB.t ($(replacement_columns)) ENGINE = MergeTree ORDER BY x"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $1"
    HAS_Y=$((1 - HAS_Y))
    print_columns
    print_file_has_y
    print_leftovers
    print_warning "$2" "$since"
}

FINISHED='Finished the exchange of % and % after an error: %FAULT_INJECTED%'
COMPLETED='The exchange of % and % completed despite an error: %FAULT_INJECTED%'
RETRYING='Retrying the exchange of % and % after an error: %FAULT_INJECTED%'

replace_with_failpoint database_atomic_exchange_fail_after_first_move "$FINISHED"
replace_with_failpoint database_atomic_exchange_fail_after_second_move "$FINISHED"
replace_with_failpoint database_atomic_exchange_fail_after_third_move "$COMPLETED"
replace_with_failpoint database_atomic_exchange_fail_before_first_move "$RETRYING"

echo "database_atomic_exchange_always_fail_before_first_move"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move"
$CLIENT -q "REPLACE TABLE $DB.t ($(replacement_columns)) ENGINE = MergeTree ORDER BY x" 2>&1 | grep -o 'FAULT_INJECTED' | head -n1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move"
print_columns
print_file_has_y
print_leftovers
print_warning "$RETRYING" "$since"
# No fence was raised.
$CLIENT -q "CREATE TABLE $DB.c (x UInt8) ENGINE = Memory" && echo "created c"
