#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# Tag no-parallel: the failpoints are server-wide and would change the exchanges of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a move of an exchange with a record fails, the exchange reads the files and rolls forward from the record:
# it finishes an interrupted exchange, accepts one that completed despite the error, and runs the moves once more
# when nothing moved. Only a failed retry in a plain `Atomic` database fails the query, and it leaves nothing behind.
# The failpoint `database_atomic_exchange_with_record` forces the record path.

DB_ATOMIC="${CLICKHOUSE_DATABASE}_atomic"
DB_REPLICATED="${CLICKHOUSE_DATABASE}_replicated"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/failures"

# The warnings of the exchanges are checked in `system.text_log`, so they are not sent to the client.
CLIENT="$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --send_logs_level=error"

FAILPOINTS=(
    database_atomic_exchange_with_record
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
    for db in "$DB_ATOMIC" "$DB_REPLICATED"
    do
        $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
    done
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_with_record"

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

$CLIENT -q "
    CREATE DATABASE $DB_ATOMIC ENGINE = Atomic;
    CREATE TABLE $DB_ATOMIC.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB_ATOMIC.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
"
set_metadata_dir "$DB_ATOMIC" DIR_ATOMIC

# The metadata of the databases is not on the local filesystem (for example, a `database_disk` on object storage),
# where exchanges never use a record. The directory of a database exists once it has a table.
if [ ! -d "$DIR_ATOMIC" ]
then
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

function now_us()
{
    $CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))"
}

# Prints the tables of database $1 that have column `y`.
function print_tables_with_y()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT 'has y: ' || arrayStringConcat(arraySort(groupArray(table)), ', ')
        FROM system.columns WHERE database = '$1' AND name = 'y'"
}

# Prints the number of dot-files, such as exchange records, and of files that are not metadata files,
# such as a temporary file of an exchange, in the metadata directory $1.
function print_leftover_files()
{
    # shellcheck disable=SC2010
    echo "dot-files: $(ls -A "$1" | grep -c '^\.')"
    # shellcheck disable=SC2010
    echo "other files: $(ls "$1" | grep -vc '\.sql$')"
}

# Prints whether database $1 logged a message like $2 since the time $3.
function print_warning()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT if(count() > 0, 'logged: ', 'not logged: ') || '$2'
        FROM system.text_log
        WHERE event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) >= $3
            AND endsWith(logger_name, '($1)') AND level = 'Warning' AND message LIKE '$2'"
}

# Exchanges `a` and `b` in database $1 with the metadata directory $2 while the failpoint $3 is enabled,
# and checks that the exchange succeeded with the warning $4.
function exchange_with_failpoint()
{
    echo "$3"
    local since
    since=$(now_us)
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $3"
    $CLIENT -q "EXCHANGE TABLES $1.a AND $1.b"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $3"
    print_tables_with_y "$1"
    print_leftover_files "$2"
    print_warning "$1" "$4" "$since"
}

FINISHED='Finished the exchange of % and % after an error: %FAULT_INJECTED%'
COMPLETED='The exchange of % and % completed despite an error: %FAULT_INJECTED%'
RETRYING='Retrying the exchange of % and % after an error: %FAULT_INJECTED%'

# Runs the cases that succeed in database $1 with the metadata directory $2.
function run_cases()
{
    exchange_with_failpoint "$1" "$2" database_atomic_exchange_fail_after_first_move "$FINISHED"
    exchange_with_failpoint "$1" "$2" database_atomic_exchange_fail_after_second_move "$FINISHED"
    exchange_with_failpoint "$1" "$2" database_atomic_exchange_fail_after_third_move "$COMPLETED"
    exchange_with_failpoint "$1" "$2" database_atomic_exchange_fail_before_first_move "$RETRYING"
    # No fence was raised.
    $CLIENT -q "CREATE TABLE $1.c (x UInt8) ENGINE = Memory" && echo "created c"
}

echo "Atomic"
run_cases "$DB_ATOMIC" "$DIR_ATOMIC"

echo "database_atomic_exchange_always_fail_before_first_move"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move"
$CLIENT -q "EXCHANGE TABLES $DB_ATOMIC.a AND $DB_ATOMIC.b" 2>&1 | grep -o 'FAULT_INJECTED' | head -n1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move"
print_tables_with_y "$DB_ATOMIC"
print_leftover_files "$DIR_ATOMIC"
# No fence was raised.
$CLIENT -q "CREATE TABLE $DB_ATOMIC.d (x UInt8) ENGINE = Memory" && echo "created d"

echo
echo "Replicated"
$CLIENT -q "CREATE DATABASE $DB_REPLICATED ENGINE = Replicated('$ZK_PATH', 'shard1', 'replica1')"
$CLIENT -q "
    CREATE TABLE $DB_REPLICATED.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB_REPLICATED.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
"
set_metadata_dir "$DB_REPLICATED" DIR_REPLICATED
run_cases "$DB_REPLICATED" "$DIR_REPLICATED"

# Keeper holds the exchange the retry made in the files.
$CLIENT -q "SYSTEM SYNC DATABASE REPLICA $DB_REPLICATED"
$CLICKHOUSE_CLIENT -q "
    SELECT 'keeper matches local: ' || toString(count())
    FROM
    (
        SELECT name, extract(value, 'UUID \'([0-9a-f-]+)\'') AS uuid
        FROM system.zookeeper WHERE path = '$ZK_PATH/metadata' AND name IN ('a', 'b')
    ) AS keeper
    INNER JOIN
    (
        SELECT name, toString(uuid) AS uuid FROM system.tables WHERE database = '$DB_REPLICATED' AND name IN ('a', 'b')
    ) AS local
    ON keeper.name = local.name AND keeper.uuid = local.uuid"
