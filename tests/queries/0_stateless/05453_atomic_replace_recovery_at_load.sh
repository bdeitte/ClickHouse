#!/usr/bin/env bash

# Finishing a swap logs a warning.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Loading an `Atomic` database finishes the swaps of metadata files that a crash interrupted.
# Each case detaches a database, writes the files an interrupted swap leaves, and attaches the database again.

DB="${CLICKHOUSE_DATABASE}_r"

DISK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")

# Set before the first path is computed, so that the early exits drop the database too.
# The full cleanup below replaces it.
function cleanup_early()
{
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
}
trap cleanup_early EXIT

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
    CREATE TABLE $DB.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $DB.a VALUES (1);
    INSERT INTO $DB.b VALUES (2, 3);
"

set_metadata_dir "$DB" REL DIR

# The metadata of the database is not on the local filesystem (for example, a `database_disk` on object storage),
# where loading does not finish swaps. The directory of a database exists once it has a table.
if [ ! -d "$DIR" ]
then
    $CLICKHOUSE_CLIENT -q "DROP DATABASE $DB SYNC"
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

# The original metadata files, and a file that differs from both.
SAVED="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
A="${SAVED}_a.sql"
B="${SAVED}_b.sql"
CHANGED="${SAVED}_changed.sql"
cp "$DIR/a.sql" "$A"
cp "$DIR/b.sql" "$B"
sed "s/UUID '[0-9a-f-]*'/UUID '00000000-0000-0000-0000-000000000001'/" "$A" > "$CHANGED"

# Removes the files the cases write in directory $1, by exact names.
function remove_case_files()
{
    rm -f -- "$1"/.a.sql.exchange "$1"/.a.sql.exchange.tmp "$1"/a.sql.tmp_exchange "$1"/.a.sql.exchange_write
}

function restore_main()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE IF EXISTS $DB"
    remove_case_files "$DIR"
    cp "$A" "$DIR/a.sql"
    cp "$B" "$DIR/b.sql"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
}

function cleanup()
{
    restore_main
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
    rm -f -- "$A" "$B" "$CHANGED"
}
trap cleanup EXIT

# Prints a record: `exchange v1`, the two paths, and the contents the two paths hold after the swap.
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

# Prints the columns of the tables `a` and `b`.
function print_columns()
{
    for table in a b
    do
        echo "$table: $($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(groupArray(name), ',') FROM (SELECT name FROM system.columns WHERE database = '$DB' AND table = '$table' ORDER BY position)")"
    done
}

# Prints whether file $2 holds the same content as file $3, under the name $1.
function print_same()
{
    if cmp -s "$2" "$3"; then echo "$1 is untouched"; else echo "$1 is changed"; fi
}

function print_dot_files()
{
    # shellcheck disable=SC2010
    ls -A "$1" | grep '^\.' | LC_ALL=C sort
}

# The record of a swap of `a.sql` and `b.sql`.
function main_record()
{
    record "${REL}a.sql" "${REL}b.sql" "$B" "$A"
}

echo "case 1: the record only, before the swap"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
print_dot_files "$DIR"
echo
restore_main

echo "case 2: after the first move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
rm -f -- "$DIR/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
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
print_columns
print_dot_files "$DIR"
echo
restore_main

echo "case 4: after the third move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$B" "$DIR/a.sql"
cp "$A" "$DIR/b.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
print_dot_files "$DIR"
echo
restore_main

echo "case 5: a leftover temporary record and a leftover write of a finish"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
echo "partial record" > "$DIR/.a.sql.exchange.tmp"
echo "partial write" > "$DIR/.a.sql.exchange_write"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
print_dot_files "$DIR"
echo
restore_main

echo "case 6: the record only, and a metadata file that changed after it"
$CLICKHOUSE_CLIENT -q "ALTER TABLE $DB.a ADD COLUMN z UInt8"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
print_dot_files "$DIR"
echo
restore_main

echo "case 7: the temporary file, and a metadata file with neither of the recorded contents"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
cp "$CHANGED" "$DIR/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB" 2>&1 | grep -o -m1 'CORRUPTED_DATA'
print_dot_files "$DIR"
print_same "the temporary file" "$DIR/a.sql.tmp_exchange" "$A"
print_same "a.sql" "$DIR/a.sql" "$CHANGED"
print_same "b.sql" "$DIR/b.sql" "$B"
# Repair by hand, as the error says: restore the files, and remove the record and the temporary file.
remove_case_files "$DIR"
cp "$A" "$DIR/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
print_dot_files "$DIR"
echo
restore_main

echo "case 8: a temporary file with neither of the recorded contents, after the first move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
main_record > "$DIR/.a.sql.exchange"
cp "$CHANGED" "$DIR/a.sql.tmp_exchange"
rm -f -- "$DIR/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB" 2>&1 | grep -o -m1 'CORRUPTED_DATA'
print_dot_files "$DIR"
print_same "the temporary file" "$DIR/a.sql.tmp_exchange" "$CHANGED"
print_same "b.sql" "$DIR/b.sql" "$B"
if [ -f "$DIR/a.sql" ]; then echo "a.sql is back"; else echo "a.sql is missing"; fi
# Repair by hand, as the error says.
remove_case_files "$DIR"
cp "$A" "$DIR/a.sql"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
print_dot_files "$DIR"
echo
restore_main

echo "case 9: the temporary file without a record"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
cp "$B" "$DIR/a.sql.tmp_exchange"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB" 2>&1 | grep -o -m1 -e 'INCORRECT_FILE_NAME' -e 'no valid exchange record is next to it'
print_dot_files "$DIR"
if [ -f "$DIR/a.sql.tmp_exchange" ]; then echo "the temporary file is left"; fi
echo
restore_main

echo "case 10: a record whose path is not the record path of its old path"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
record "${REL}b.sql" "${REL}a.sql" "$A" "$B" > "$DIR/.a.sql.exchange"
cp "$B" "$DIR/a.sql.tmp_exchange"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB" 2>&1 | grep -o -m1 -e 'INCORRECT_FILE_NAME' -e 'no valid exchange record is next to it'
print_dot_files "$DIR"
print_same "a.sql" "$DIR/a.sql" "$A"
print_same "b.sql" "$DIR/b.sql" "$B"
remove_case_files "$DIR"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
print_columns
