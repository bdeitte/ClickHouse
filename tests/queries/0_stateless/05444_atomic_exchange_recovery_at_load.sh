#!/usr/bin/env bash

# Finishing an exchange logs a warning.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Loading an `Atomic` database finishes the exchanges of metadata files that a crash interrupted.
# Each case detaches a database, writes the files an interrupted `EXCHANGE TABLES` leaves, and attaches the database again.

DB="${CLICKHOUSE_DATABASE}_r"
DB1="${CLICKHOUSE_DATABASE}_r1"
DB2="${CLICKHOUSE_DATABASE}_r2"

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
    CREATE DATABASE $DB ENGINE = Atomic;
    CREATE DATABASE $DB1 ENGINE = Atomic;
    CREATE DATABASE $DB2 ENGINE = Atomic;
    CREATE TABLE $DB.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB1.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB1.c (x UInt8, z UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB2.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $DB.a VALUES (1);
    INSERT INTO $DB.b VALUES (2, 3);
    INSERT INTO $DB1.a VALUES (1);
    INSERT INTO $DB1.c VALUES (4, 5);
    INSERT INTO $DB2.b VALUES (2, 3);
"

set_metadata_dir "$DB" REL DIR
set_metadata_dir "$DB1" REL1 DIR1
set_metadata_dir "$DB2" REL2 DIR2

# The metadata of the databases is not on the local filesystem (for example, a `database_disk` on object storage),
# where loading does not finish exchanges. The directory of a database exists once it has a table.
if [ ! -d "$DIR" ]
then
    $CLICKHOUSE_CLIENT -q "DROP DATABASE $DB SYNC"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE $DB1 SYNC"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE $DB2 SYNC"
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

# The original metadata files.
SAVED="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
A="${SAVED}_a.sql"
B="${SAVED}_b.sql"
A1="${SAVED}_a1.sql"
C1="${SAVED}_c1.sql"
B2="${SAVED}_b2.sql"
cp "$DIR/a.sql" "$A"
cp "$DIR/b.sql" "$B"
cp "$DIR1/a.sql" "$A1"
cp "$DIR1/c.sql" "$C1"
cp "$DIR2/b.sql" "$B2"

# Removes the files the cases write in directory $1, by exact names.
function remove_case_files()
{
    rm -f -- "$1"/.a.sql.exchange "$1"/.b.sql.exchange "$1"/.c.sql.exchange "$1"/.a.sql.exchange.tmp \
        "$1"/.exchange_probe_test_1 "$1"/a.sql.tmp_exchange
}

function restore_main()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE IF EXISTS $DB"
    remove_case_files "$DIR"
    cp "$A" "$DIR/a.sql"
    cp "$B" "$DIR/b.sql"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
}

function restore_pair()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE IF EXISTS $DB1"
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE IF EXISTS $DB2"
    remove_case_files "$DIR1"
    remove_case_files "$DIR2"
    cp "$A1" "$DIR1/a.sql"
    cp "$C1" "$DIR1/c.sql"
    cp "$B2" "$DIR2/b.sql"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB1"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB2"
}

function cleanup()
{
    restore_main
    restore_pair
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB1 SYNC"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB2 SYNC"
    rm -f -- "$A" "$B" "$A1" "$C1" "$B2"
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

# Prints whether table $2 of database $1 has column $3.
function has_column()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = '$1' AND table = '$2' AND name = '$3'"
}

function print_exchanged()
{
    if [ "$(has_column "$DB" a y)" = 1 ]; then echo "exchanged"; else echo "not exchanged"; fi
}

function print_dot_files()
{
    # shellcheck disable=SC2010
    ls -A "$1" | grep '^\.' | LC_ALL=C sort
}

# The record of an exchange of `a.sql` and `b.sql` in the main database.
function main_record()
{
    record "${REL}a.sql" "${REL}b.sql" "$B" "$A"
}

echo "case 1: the record only, before the exchange"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_exchanged
print_dot_files "$DIR"
echo
restore_main

echo "case 2: after the first move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
rm -f -- "$DIR/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_exchanged
print_dot_files "$DIR"
echo
restore_main

echo "case 3: after the second move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
cp "$B" "$DIR/a.sql"
rm -f -- "$DIR/b.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_exchanged
print_dot_files "$DIR"
echo
restore_main

echo "case 4: after the third move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$B" "$DIR/a.sql"
cp "$A" "$DIR/b.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_exchanged
print_dot_files "$DIR"
echo
restore_main

echo "case 5: a leftover temporary record and a leftover probe file"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
echo "partial record" > "$DIR/.a.sql.exchange.tmp"
echo "probe" > "$DIR/.exchange_probe_test_1"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_exchanged
print_dot_files "$DIR"
echo
restore_main

echo "case 6: the record only, and a metadata file that changed after it"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $DB.a ADD COLUMN z UInt8"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_exchanged
print_dot_files "$DIR"
echo
restore_main

echo "case 7: the temporary file, and a metadata file with neither of the recorded contents"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
sed "s/UUID '[0-9a-f-]*'/UUID '00000000-0000-0000-0000-000000000001'/" "$A" > "$DIR/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB" 2>&1 | grep -o -m1 'CORRUPTED_DATA'
print_dot_files "$DIR"
if [ -f "$DIR/a.sql.tmp_exchange" ]; then echo "the temporary file is left"; fi
echo
restore_main

echo "case 8: an exchange between two databases, the database with the copy of the record attached first"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB1"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB2"
record "${REL1}a.sql" "${REL2}b.sql" "$B2" "$A1" > "$DIR1/.a.sql.exchange"
cp "$DIR1/.a.sql.exchange" "$DIR2/.b.sql.exchange"
cp "$A1" "$DIR1/a.sql.tmp_exchange"
rm -f -- "$DIR1/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB2"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB1"
if [ "$(has_column "$DB1" a y)" = 1 ] && [ "$(has_column "$DB2" b y)" = 0 ]; then echo "exchanged"; else echo "not exchanged"; fi
print_dot_files "$DIR1"
print_dot_files "$DIR2"
echo
restore_pair

echo "case 9: a stale copy of a record, whose primary a newer exchange overwrote"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB1"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB2"
record "${REL1}a.sql" "${REL2}b.sql" "$B2" "$A1" > "$DIR2/.b.sql.exchange"
record "${REL1}a.sql" "${REL1}c.sql" "$C1" "$A1" > "$DIR1/.a.sql.exchange"
cp "$A1" "$DIR1/a.sql.tmp_exchange"
rm -f -- "$DIR1/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB2"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB1"
if [ "$(has_column "$DB1" a z)" = 1 ] && [ "$(has_column "$DB1" c z)" = 0 ]; then echo "a and c exchanged"; else echo "a and c not exchanged"; fi
if [ "$(has_column "$DB2" b y)" = 1 ]; then echo "b unchanged"; else echo "b changed"; fi
print_dot_files "$DIR1"
print_dot_files "$DIR2"
echo
restore_pair
