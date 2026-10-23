#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# Tag no-parallel: the failpoint is server-wide and would change the exchanges of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the filesystem cannot exchange two metadata files atomically, an `Atomic` database exchanges them with three moves
# and a record. The failpoint `database_atomic_exchange_with_record` forces that path. Every statement that exchanges
# metadata files works with it in an `Atomic` and in a `Replicated` database, and leaves no record behind.

DB_ATOMIC="${CLICKHOUSE_DATABASE}_atomic"
DB_REPLICATED="${CLICKHOUSE_DATABASE}_replicated"
DB_OTHER="${CLICKHOUSE_DATABASE}_other"
DB_FRESH_ATOMIC="${CLICKHOUSE_DATABASE}_fresh_atomic"
DB_FRESH_REPLICATED="${CLICKHOUSE_DATABASE}_fresh_replicated"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"

CLIENT="$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_with_record"
    for db in "$DB_ATOMIC" "$DB_REPLICATED" "$DB_OTHER" "$DB_FRESH_ATOMIC" "$DB_FRESH_REPLICATED"
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
        SELECT '$2has y: ' || arrayStringConcat(arraySort(groupArray(table)), ', ')
        FROM system.columns WHERE database = '$1' AND name = 'y'"
}

# Prints the number of dot-files, such as exchange records, in the metadata directory $1.
function print_dot_files()
{
    # shellcheck disable=SC2010
    echo "dot-files: $(ls -A "$1" | grep -c '^\.')"
}

# Prints whether database $1 exchanged metadata files with a record since the time $2.
function print_record_used()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT if(count() > 0, 'exchanged with a record', 'exchanged without a record')
        FROM system.text_log
        WHERE event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) >= $2
            AND endsWith(logger_name, '($1)') AND message LIKE 'Exchanging % and % with an exchange record'"
}

# Runs the queries $3 and prints their effect on database $1 with the metadata directory $2.
function run()
{
    local since
    since=$(now_us)
    $CLIENT -q "$3"
    print_tables_with_y "$1"
    print_dot_files "$2"
    print_record_used "$1" "$since"
}

# Runs the statements in database $1 with the metadata directory $2.
# $3 is the engine of the refreshable materialized view: a `Replicated` database needs a replicated table there.
function run_statements()
{
    local db=$1
    $CLIENT -q "
        CREATE TABLE IF NOT EXISTS $db.a (x UInt8) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE $db.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
        CREATE DICTIONARY $db.d1 (x UInt64, v UInt8) PRIMARY KEY x SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
        CREATE DICTIONARY $db.d2 (x UInt64, y UInt8) PRIMARY KEY x SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
        CREATE TABLE $db.\`t a\` (x UInt8) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE $db.\`t.b\` (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    "

    echo "EXCHANGE TABLES"
    run "$db" "$2" "EXCHANGE TABLES $db.a AND $db.b"

    echo "EXCHANGE DICTIONARIES"
    run "$db" "$2" "EXCHANGE DICTIONARIES $db.d1 AND $db.d2"

    echo "CREATE OR REPLACE TABLE"
    run "$db" "$2" "CREATE OR REPLACE TABLE $db.a (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 4"
    $CLIENT -q "SELECT * FROM $db.a"

    echo "REPLACE TABLE"
    run "$db" "$2" "REPLACE TABLE $db.a (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 5, 6"
    $CLIENT -q "SELECT * FROM $db.a"

    echo "refreshable materialized view"
    run "$db" "$2" "
        CREATE MATERIALIZED VIEW $db.mv REFRESH EVERY 1 YEAR (x UInt8) ENGINE = $3 ORDER BY x AS SELECT 7 AS x;
        SYSTEM WAIT VIEW $db.mv;
        SYSTEM REFRESH VIEW $db.mv;
        SYSTEM WAIT VIEW $db.mv;
        SYSTEM REFRESH VIEW $db.mv;
        SYSTEM WAIT VIEW $db.mv;
        SYSTEM REFRESH VIEW $db.mv;
        SYSTEM WAIT VIEW $db.mv;
    "
    $CLIENT -q "
        SELECT * FROM $db.mv;
        SELECT 'temporary tables: ' || toString(count()) FROM system.tables WHERE database = '$db' AND startsWith(name, '_tmp_replace_');
    "

    echo "EXCHANGE TABLES twice"
    run "$db" "$2" "EXCHANGE TABLES $db.a AND $db.b; EXCHANGE TABLES $db.a AND $db.b"

    echo "EXCHANGE TABLES with names that need escaping"
    run "$db" "$2" "EXCHANGE TABLES $db.\`t a\` AND $db.\`t.b\`"
}

# Runs two exchanges at the same time in a new database $1, whose first exchange probes the filesystem.
function run_concurrent_exchanges()
{
    $CLIENT -q "
        CREATE TABLE $1.p1 (x UInt8) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE $1.q1 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE $1.p2 (x UInt8) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE $1.q2 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    "
    local dir
    set_metadata_dir "$1" dir
    local since
    since=$(now_us)
    $CLIENT -q "EXCHANGE TABLES $1.p1 AND $1.q1" &
    $CLIENT -q "EXCHANGE TABLES $1.p2 AND $1.q2" &
    wait
    print_tables_with_y "$1"
    print_dot_files "$dir"
    print_record_used "$1" "$since"
}

echo "Atomic"
run_statements "$DB_ATOMIC" "$DIR_ATOMIC" MergeTree

echo "EXCHANGE TABLES between databases"
$CLIENT -q "
    CREATE DATABASE $DB_OTHER ENGINE = Atomic;
    CREATE TABLE $DB_OTHER.b (x UInt8) ENGINE = MergeTree ORDER BY x;
"
set_metadata_dir "$DB_OTHER" DIR_OTHER
since=$(now_us)
$CLIENT -q "EXCHANGE TABLES $DB_ATOMIC.a AND $DB_OTHER.b"
print_tables_with_y "$DB_ATOMIC" "$DB_ATOMIC "
print_tables_with_y "$DB_OTHER" "$DB_OTHER "
print_dot_files "$DIR_ATOMIC"
print_dot_files "$DIR_OTHER"
print_record_used "$DB_ATOMIC" "$since"

echo "concurrent EXCHANGE TABLES"
$CLIENT -q "CREATE DATABASE $DB_FRESH_ATOMIC ENGINE = Atomic"
run_concurrent_exchanges "$DB_FRESH_ATOMIC"

echo
echo "Replicated"
$CLIENT -q "CREATE DATABASE $DB_REPLICATED ENGINE = Replicated('$ZK_PATH/replicated', 'shard1', 'replica1')"
$CLIENT -q "CREATE TABLE $DB_REPLICATED.a (x UInt8) ENGINE = MergeTree ORDER BY x"
set_metadata_dir "$DB_REPLICATED" DIR_REPLICATED
run_statements "$DB_REPLICATED" "$DIR_REPLICATED" ReplicatedMergeTree

echo "concurrent EXCHANGE TABLES"
$CLIENT -q "CREATE DATABASE $DB_FRESH_REPLICATED ENGINE = Replicated('$ZK_PATH/fresh_replicated', 'shard1', 'replica1')"
run_concurrent_exchanges "$DB_FRESH_REPLICATED"
