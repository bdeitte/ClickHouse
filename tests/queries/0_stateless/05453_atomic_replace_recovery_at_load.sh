#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Loading an `Atomic` database finishes the swaps of metadata files that a crash interrupted.
# Each case writes the files that an interrupted swap leaves in the metadata directory of a database of `clickhouse-local`,
# then starts `clickhouse-local` on the same path again, which loads the database as a server start does.
# The metadata is in the path of `clickhouse-local`, on a local disk, whatever disks the server has.

LOCAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
# With a fixed UUID, the metadata directory of the database is `store/<first three characters of the UUID>/<UUID>/`.
UUID="5453a70c-0000-4000-8000-000000000001"
REL="store/${UUID:0:3}/${UUID}/"
DIR="${LOCAL_PATH}/${REL}"
# No path below can be empty or outside the temporary directory of the test.
if [ -z "$CLICKHOUSE_TMP" ] || [ -z "$CLICKHOUSE_TEST_UNIQUE_NAME" ] || [[ "$DIR" != "$CLICKHOUSE_TMP"/*/store/* ]]
then
    echo "Unexpected metadata directory '$DIR'" >&2
    exit 1
fi

function local_query()
{
    $CLICKHOUSE_LOCAL --path "$LOCAL_PATH" -q "$1"
}

local_query "
    CREATE DATABASE d UUID '$UUID' ENGINE = Atomic;
    CREATE TABLE d.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO d.a VALUES (1);
    INSERT INTO d.b VALUES (2, 3);
"
if [ ! -f "$DIR/a.sql" ] || [ ! -f "$DIR/b.sql" ]
then
    echo "The metadata files are not in '$DIR'" >&2
    exit 1
fi

# The original metadata files, and a file that differs from both.
SAVED="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
A="${SAVED}_a.sql"
B="${SAVED}_b.sql"
CHANGED="${SAVED}_changed.sql"
cp "$DIR/a.sql" "$A"
cp "$DIR/b.sql" "$B"
sed "s/UUID '[0-9a-f-]*'/UUID '00000000-0000-0000-0000-000000000001'/" "$A" > "$CHANGED"

function cleanup()
{
    rm -f -- "$A" "$B" "$CHANGED"
}
trap cleanup EXIT

# Removes the files the cases write in directory $1, by exact names.
function remove_case_files()
{
    rm -f -- "$1"/.a.sql.exchange "$1"/.a.sql.exchange.tmp "$1"/a.sql.tmp_exchange "$1"/.a.sql.exchange_write
}

function restore_main()
{
    remove_case_files "$DIR"
    cp "$A" "$DIR/a.sql"
    cp "$B" "$DIR/b.sql"
}

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

# Loads the database, and prints the columns and the rows of the tables `a` and `b`.
# The rows show which data each metadata file points to.
function load_and_print_tables()
{
    local_query "
        SELECT table || ': ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ',')
        FROM system.columns WHERE database = 'd' AND table IN ('a', 'b') GROUP BY table ORDER BY table;
        SELECT 'a rows: ' || formatRowNoNewline('CSV', *) FROM d.a;
        SELECT 'b rows: ' || formatRowNoNewline('CSV', *) FROM d.b;
    "
}

# Loads the database, and prints the first of the error codes $@ in the error of the load.
function load_and_print_error()
{
    local patterns=()
    for code in "$@"
    do
        patterns+=(-e "$code")
    done
    local_query "SELECT 1" 2>&1 | grep -o -m1 "${patterns[@]}"
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
main_record > "$DIR/.a.sql.exchange"
load_and_print_tables
print_dot_files "$DIR"
echo
restore_main

echo "case 2: after the first move"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
rm -f -- "$DIR/a.sql"
load_and_print_tables
print_dot_files "$DIR"
if [ -f "$DIR/a.sql.tmp_exchange" ]; then echo "the temporary file is left"; fi
echo
restore_main

echo "case 3: after the second move"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
cp "$B" "$DIR/a.sql"
rm -f -- "$DIR/b.sql"
load_and_print_tables
print_dot_files "$DIR"
if [ -f "$DIR/a.sql.tmp_exchange" ]; then echo "the temporary file is left"; fi
echo
restore_main

echo "case 4: after the third move"
main_record > "$DIR/.a.sql.exchange"
cp "$B" "$DIR/a.sql"
cp "$A" "$DIR/b.sql"
load_and_print_tables
print_dot_files "$DIR"
echo
restore_main

echo "case 5: a leftover temporary record and a leftover write of a finish"
echo "partial record" > "$DIR/.a.sql.exchange.tmp"
echo "partial write" > "$DIR/.a.sql.exchange_write"
load_and_print_tables
print_dot_files "$DIR"
echo
restore_main

echo "case 6: the record only, and a metadata file that changed after it"
local_query "ALTER TABLE d.a ADD COLUMN z UInt8"
main_record > "$DIR/.a.sql.exchange"
load_and_print_tables
print_dot_files "$DIR"
echo
restore_main

echo "case 7: the temporary file, and a metadata file with neither of the recorded contents"
main_record > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
cp "$CHANGED" "$DIR/a.sql"
load_and_print_error CORRUPTED_DATA
print_dot_files "$DIR"
print_same "the temporary file" "$DIR/a.sql.tmp_exchange" "$A"
print_same "a.sql" "$DIR/a.sql" "$CHANGED"
print_same "b.sql" "$DIR/b.sql" "$B"
# Repair by hand, as the error says: restore the files, and remove the record and the temporary file.
remove_case_files "$DIR"
cp "$A" "$DIR/a.sql"
load_and_print_tables
print_dot_files "$DIR"
echo
restore_main

echo "case 8: a temporary file with neither of the recorded contents, after the first move"
main_record > "$DIR/.a.sql.exchange"
cp "$CHANGED" "$DIR/a.sql.tmp_exchange"
rm -f -- "$DIR/a.sql"
load_and_print_error CORRUPTED_DATA
print_dot_files "$DIR"
print_same "the temporary file" "$DIR/a.sql.tmp_exchange" "$CHANGED"
print_same "b.sql" "$DIR/b.sql" "$B"
if [ -f "$DIR/a.sql" ]; then echo "a.sql is back"; else echo "a.sql is missing"; fi
# Repair by hand, as the error says.
remove_case_files "$DIR"
cp "$A" "$DIR/a.sql"
load_and_print_tables
print_dot_files "$DIR"
echo
restore_main

echo "case 9: the temporary file without a record"
cp "$B" "$DIR/a.sql.tmp_exchange"
load_and_print_error INCORRECT_FILE_NAME 'no valid exchange record is next to it'
print_dot_files "$DIR"
if [ -f "$DIR/a.sql.tmp_exchange" ]; then echo "the temporary file is left"; fi
echo
restore_main

echo "case 10: a record whose path is not the record path of its old path"
record "${REL}b.sql" "${REL}a.sql" "$A" "$B" > "$DIR/.a.sql.exchange"
cp "$B" "$DIR/a.sql.tmp_exchange"
load_and_print_error INCORRECT_FILE_NAME 'no valid exchange record is next to it'
print_dot_files "$DIR"
print_same "a.sql" "$DIR/a.sql" "$A"
print_same "b.sql" "$DIR/b.sql" "$B"
remove_case_files "$DIR"
load_and_print_tables
