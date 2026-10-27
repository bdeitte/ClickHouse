#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the failpoints are server-wide and would change the swaps of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the filesystem of the metadata of an `Atomic` database cannot exchange two files atomically, `CREATE OR REPLACE`,
# `REPLACE TABLE` and refreshes of refreshable materialized views swap the two metadata files with three moves and a record.
# The failpoint `database_atomic_exchange_not_supported` makes the filesystem report that it cannot exchange files.
# `EXCHANGE TABLES` written by a user is not emulated and fails with `UNSUPPORTED_METHOD`.

DB="${CLICKHOUSE_DATABASE}_atomic"
DB_OTHER="${CLICKHOUSE_DATABASE}_other"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_not_supported"
    for db in "$DB" "$DB_OTHER"
    do
        $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
    done
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

# Swaps never use a record when the metadata of the database is not on the local filesystem (for example, a `database_disk`
# on object storage). The directory of a database exists once it has a table.
if [ ! -d "$DIR" ]
then
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

# `tests/config/install.sh` turns the emulation on for every server that runs this test.
EMULATION=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'database_atomic_allow_exchange_emulation'")
if [ "$EMULATION" != "1" ]
then
    echo "The server setting database_atomic_allow_exchange_emulation is '$EMULATION', not 1" >&2
    exit 1
fi

function now_us()
{
    $CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))"
}

# Prints the columns and the number of rows of each table $2... of database $1.
function print_tables()
{
    local db=$1
    shift
    local table
    for table in "$@"
    do
        $CLICKHOUSE_CLIENT -q "
            SELECT '$table columns: ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ', ')
            FROM system.columns WHERE database = '$db' AND table = '$table'"
        $CLICKHOUSE_CLIENT -q "SELECT '$table rows: ' || toString(count()) FROM $db.\`$table\`"
    done
}

# Prints the number of dot-files, such as records of swaps, and of files that are not metadata files,
# such as a temporary file of a swap, in the metadata directory $1.
function print_leftover_files()
{
    # shellcheck disable=SC2010
    echo "dot-files: $(ls -A "$1" | grep -c '^\.')"
    # shellcheck disable=SC2010
    echo "other files: $(ls "$1" | grep -vc '\.sql$')"
}

# Prints the number of swaps of metadata files with a record in database $1 since the time $2.
function print_record_used()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT 'swaps with a record: ' || toString(count())
        FROM system.text_log
        WHERE event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) >= $2
            AND endsWith(logger_name, '($1)') AND message LIKE 'Exchanging % and % with an exchange record'"
}

# Prints whether the metadata file `t.sql` on disk defines column `y`.
function print_file_has_y()
{
    if [ ! -f "$DIR/t.sql" ]
    then
        echo "on disk: t.sql is missing"
    elif grep -q '`y` UInt8' "$DIR/t.sql"
    then
        echo "on disk has y: yes"
    else
        echo "on disk has y: no"
    fi
}

echo "CREATE OR REPLACE TABLE"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE TABLE $DB.t (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 1, 2"
print_tables "$DB" t
print_file_has_y
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "REPLACE TABLE"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "REPLACE TABLE $DB.t (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 3"
print_tables "$DB" t
print_file_has_y
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "REPLACE TABLE whose SELECT throws"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "REPLACE TABLE $DB.t (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 4, throwIf(1)" 2>&1 \
    | grep -o 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' | head -n1
print_tables "$DB" t
$CLICKHOUSE_CLIENT -q "
    SELECT 'temporary tables: ' || toString(count())
    FROM system.tables WHERE database = '$DB' AND startsWith(name, '_tmp_replace_')"
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "CREATE OR REPLACE TABLE with a name that needs escaping"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $DB.\`t.x y\` (x UInt8) ENGINE = MergeTree ORDER BY x"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE TABLE $DB.\`t.x y\` (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 1, 2"
print_tables "$DB" 't.x y'
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "CREATE OR REPLACE TABLE of a table that does not exist"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE TABLE $DB.n (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 1, 2"
print_tables "$DB" n
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "concurrent CREATE OR REPLACE TABLE"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE $DB.p (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB.q (x UInt8) ENGINE = MergeTree ORDER BY x;
"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE TABLE $DB.p (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 1, 2" &
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE TABLE $DB.q (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 1, 2" &
wait
print_tables "$DB" p q
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "CREATE OR REPLACE VIEW"
$CLICKHOUSE_CLIENT -q "CREATE VIEW $DB.v AS SELECT 'old' AS s"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE VIEW $DB.v AS SELECT 'new' AS s"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB.v"
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "CREATE OR REPLACE DICTIONARY"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE $DB.dict_source_old (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO $DB.dict_source_old VALUES (1, 'old');
    CREATE TABLE $DB.dict_source_new (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO $DB.dict_source_new VALUES (1, 'new');
    CREATE DICTIONARY $DB.d (k UInt64, s String) PRIMARY KEY k
        SOURCE(CLICKHOUSE(DB '$DB' TABLE 'dict_source_old')) LAYOUT(FLAT()) LIFETIME(0);
"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB.d"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "
    CREATE OR REPLACE DICTIONARY $DB.d (k UInt64, s String) PRIMARY KEY k
        SOURCE(CLICKHOUSE(DB '$DB' TABLE 'dict_source_new')) LAYOUT(FLAT()) LIFETIME(0)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB.d"
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "refreshable materialized view with an inner table"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE $DB.src (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $DB.src VALUES (1);
    CREATE MATERIALIZED VIEW $DB.mv REFRESH EVERY 1 YEAR (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT x FROM $DB.src;
    SYSTEM WAIT VIEW $DB.mv;
    INSERT INTO $DB.src VALUES (2);
"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "
    SYSTEM REFRESH VIEW $DB.mv;
    SYSTEM WAIT VIEW $DB.mv;
"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB.mv ORDER BY x"
print_leftover_files "$DIR"
print_record_used "$DB" "$since"

echo "refreshable materialized view with a target table in another database"
$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $DB_OTHER ENGINE = Atomic;
    CREATE TABLE $DB_OTHER.target (x UInt8) ENGINE = MergeTree ORDER BY x;
"
set_metadata_dir "$DB_OTHER" DIR_OTHER
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW $DB.mv_to REFRESH EVERY 1 YEAR TO $DB_OTHER.target AS SELECT x FROM $DB.src;
    SYSTEM WAIT VIEW $DB.mv_to;
    INSERT INTO $DB.src VALUES (3);
"
since=$(now_us)
$CLICKHOUSE_CLIENT -q "
    SYSTEM REFRESH VIEW $DB.mv_to;
    SYSTEM WAIT VIEW $DB.mv_to;
"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $DB_OTHER.target ORDER BY x"
print_leftover_files "$DIR"
print_leftover_files "$DIR_OTHER"
print_record_used "$DB_OTHER" "$since"

echo "EXCHANGE TABLES"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE $DB.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
"
since=$(now_us)
error=$($CLICKHOUSE_CLIENT -q "EXCHANGE TABLES $DB.a AND $DB.b" 2>&1)
echo "$error" | grep -o 'UNSUPPORTED_METHOD' | head -n1
# Only the error for a filesystem without an atomic exchange with the server setting on says this.
echo "$error" | grep -o 'are not emulated' | head -n1
print_tables "$DB" a b
print_leftover_files "$DIR"
print_record_used "$DB" "$since"
