#!/usr/bin/env bash

# Finishing an exchange logs a warning.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An exchange of metadata files between two `Atomic` databases writes the record in the directory of the first file
# and a copy in the directory of the second. The load of either database finishes an exchange that a crash interrupted,
# and removes only the record in its own directory: the other record stays until the other database loads, so that
# its load lists its metadata files again after the finish. The test attaches the two databases in both orders and
# checks the tables and the records after each attach.

DB1="${CLICKHOUSE_DATABASE}_e1"
DB2="${CLICKHOUSE_DATABASE}_e2"

DISK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")

# Sets the variables named $2 and $3 to the relative and the absolute metadata directory of database $1.
# Exits when either is not a database directory, so that no path below can be empty.
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
    printf -v "$2" '%s' "$rel"
    printf -v "$3" '%s' "$abs"
}

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $DB1 ENGINE = Atomic;
    CREATE DATABASE $DB2 ENGINE = Atomic;
    CREATE TABLE $DB1.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB2.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $DB1.a VALUES (1);
    INSERT INTO $DB2.b VALUES (2, 3);
"

set_metadata_dir "$DB1" REL1 DIR1
set_metadata_dir "$DB2" REL2 DIR2

# The metadata of the databases is not on the local filesystem (for example, a `database_disk` on object storage),
# where loading does not finish exchanges. The directory of a database exists once it has a table.
if [ ! -d "$DIR1" ]
then
    $CLICKHOUSE_CLIENT -q "DROP DATABASE $DB1 SYNC"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE $DB2 SYNC"
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

# The original metadata files.
SAVED="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
A1="${SAVED}_a1.sql"
B2="${SAVED}_b2.sql"
cp "$DIR1/a.sql" "$A1"
cp "$DIR2/b.sql" "$B2"

function restore()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE IF EXISTS $DB1"
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE IF EXISTS $DB2"
    rm -f -- "$DIR1/.a.sql.exchange" "$DIR1/a.sql.tmp_exchange" "$DIR2/.b.sql.exchange"
    cp "$A1" "$DIR1/a.sql"
    cp "$B2" "$DIR2/b.sql"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB1"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB2"
}

function cleanup()
{
    restore
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB1 SYNC"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB2 SYNC"
    rm -f -- "$A1" "$B2"
}
trap cleanup EXIT

# Prints a record: `exchange v1`, the two paths, and the contents the two paths hold after the exchange.
# Arguments: old_path new_path old_path_content_file new_path_content_file.
# The escaping of `TSV` is the escaping of the record.
function record()
{
    $CLICKHOUSE_LOCAL -q "
        SELECT arrayJoin([
            'exchange v1', '$1', '$2',
            (SELECT raw_blob FROM file('$3', 'RawBLOB')),
            (SELECT raw_blob FROM file('$4', 'RawBLOB'))]) FORMAT TSV"
}

# Detaches both databases and leaves the files of an exchange of `$DB1.a` and `$DB2.b` that a crash interrupted
# after its first move: the record and its copy, the temporary file with the content of `a.sql`, and no `a.sql`.
function interrupt_exchange()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB1"
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB2"
    record "${REL1}a.sql" "${REL2}b.sql" "$B2" "$A1" > "$DIR1/.a.sql.exchange"
    cp "$DIR1/.a.sql.exchange" "$DIR2/.b.sql.exchange"
    cp "$A1" "$DIR1/a.sql.tmp_exchange"
    rm -f -- "$DIR1/a.sql"
}

# Prints the attached databases with the columns of their tables, and the files starting with a dot in both directories.
function print_state()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT database = '$DB1' ? 'db1' : 'db2', table, arrayStringConcat(arraySort(groupArray(name)), ',')
        FROM system.columns WHERE database IN ('$DB1', '$DB2')
        GROUP BY database, table ORDER BY ALL FORMAT TSV"
    # shellcheck disable=SC2010
    echo "db1 records: $(ls -A "$DIR1" | grep -c '^\.')"
    # shellcheck disable=SC2010
    echo "db2 records: $(ls -A "$DIR2" | grep -c '^\.')"
    if [ -f "$DIR1/a.sql.tmp_exchange" ]; then echo "the temporary file is left"; fi
}

echo "the database with the record attached first"
interrupt_exchange
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB1"
print_state
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB2"
print_state
$CLICKHOUSE_CLIENT -q "SELECT 'data', * FROM $DB1.a"
$CLICKHOUSE_CLIENT -q "SELECT 'data', * FROM $DB2.b"
echo
restore

echo "the database with the copy of the record attached first"
interrupt_exchange
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB2"
print_state
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB1"
print_state
$CLICKHOUSE_CLIENT -q "SELECT 'data', * FROM $DB1.a"
$CLICKHOUSE_CLIENT -q "SELECT 'data', * FROM $DB2.b"
