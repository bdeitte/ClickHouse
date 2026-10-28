#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A load of an `Atomic` database from a read-only disk cannot undo a swap of metadata files whose record is left. It loads the
# database only when both files of each such swap still hold their contents from before the swap. It fails with `CORRUPTED_DATA`
# when a file holds neither recorded content, and otherwise with `INCORRECT_FILE_NAME` and the advice to load the database once
# from a writable disk. It changes no file.
# `clickhouse-local` skips the access check of disks, which is what marks a local disk read-only, so the failpoint
# `database_atomic_load_metadata_disk_read_only` makes the load treat the disk as read-only. A failpoint of `clickhouse-local`
# is in its own process, so it does not reach the server or other tests.
# The database is not in `metadata/`, so a start of `clickhouse-local` does not load it, and each case loads it with
# `ATTACH DATABASE` after it enables the failpoint. The failpoint also makes the load register the UUIDs of the tables, as a load
# from a read-only disk does. A load from a writable disk puts the metadata file of the database back and starts
# `clickhouse-local`, as a server start does.

LOCAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
# With a fixed UUID, the metadata directory of the database is `store/<first three characters of the UUID>/<UUID>/`.
UUID="5459a70c-0000-4000-8000-000000000001"
REL="store/${UUID:0:3}/${UUID}/"
DIR="${LOCAL_PATH}/${REL}"
# The metadata file of the database, and the symlink of its metadata directory.
DB_METADATA_FILE="${LOCAL_PATH}/metadata/d.sql"
DB_METADATA_LINK="${LOCAL_PATH}/metadata/d"
# No path below can be empty or outside the temporary directory of the test.
if [ -z "$CLICKHOUSE_TMP" ] || [ -z "$CLICKHOUSE_TEST_UNIQUE_NAME" ] || [[ "$DIR" != "$CLICKHOUSE_TMP"/*/store/* ]] \
    || [[ "$DB_METADATA_FILE" != "$CLICKHOUSE_TMP"/*/metadata/d.sql ]]
then
    echo "Unexpected metadata directory '$DIR'" >&2
    exit 1
fi

$CLICKHOUSE_LOCAL --path "$LOCAL_PATH" -q "
    CREATE DATABASE d UUID '$UUID' ENGINE = Atomic;
    CREATE TABLE d.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.x (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.y (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO d.a VALUES (1);
    INSERT INTO d.b VALUES (2, 3);
    INSERT INTO d.x VALUES (4);
    INSERT INTO d.y VALUES (5, 6);
"
if [ ! -f "$DIR/a.sql" ] || [ ! -f "$DIR/b.sql" ] || [ ! -f "$DIR/x.sql" ] || [ ! -f "$DIR/y.sql" ]
then
    echo "The metadata files are not in '$DIR'" >&2
    exit 1
fi

# The original metadata files, the metadata file of the database, and a file that differs from both.
SAVED="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SQL="${SAVED}_d.sql"
cp "$DB_METADATA_FILE" "$DB_SQL"

# Removes the metadata of the database, so that the next start of `clickhouse-local` does not load it.
function forget_database()
{
    rm -f -- "$DB_METADATA_FILE" "$DB_METADATA_LINK"
}
forget_database

A="${SAVED}_a.sql"
B="${SAVED}_b.sql"
X="${SAVED}_x.sql"
Y="${SAVED}_y.sql"
CHANGED="${SAVED}_changed.sql"
RECORD="${SAVED}_record"
cp "$DIR/a.sql" "$A"
cp "$DIR/b.sql" "$B"
cp "$DIR/x.sql" "$X"
cp "$DIR/y.sql" "$Y"
sed "s/UUID '[0-9a-f-]*'/UUID '00000000-0000-0000-0000-000000000001'/" "$A" > "$CHANGED"

function cleanup()
{
    rm -f -- "$A" "$B" "$X" "$Y" "$CHANGED" "$RECORD" "$DB_SQL"
}
trap cleanup EXIT

# Removes the files the cases write in the metadata directory, by exact names.
function remove_case_files()
{
    rm -f -- "$DIR"/.a.sql.exchange "$DIR"/a.sql.tmp_exchange "$DIR"/.x.sql.exchange "$DIR"/x.sql.tmp_exchange
}

function restore_main()
{
    remove_case_files
    cp "$A" "$DIR/a.sql"
    cp "$B" "$DIR/b.sql"
    cp "$X" "$DIR/x.sql"
    cp "$Y" "$DIR/y.sql"
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

# The records of a swap of `a.sql` and `b.sql`, and of a swap of `x.sql` and `y.sql`.
function record_a_b()
{
    record "${REL}a.sql" "${REL}b.sql" "$B" "$A"
}
function record_x_y()
{
    record "${REL}x.sql" "${REL}y.sql" "$Y" "$X"
}

# Prints the columns and the rows of the tables, which show which data each metadata file points to.
PRINT_TABLES="
    SELECT table || ': ' || arrayStringConcat(arrayMap(c -> c.2, arraySort(groupArray((position, name)))), ',')
    FROM system.columns WHERE database = 'd' GROUP BY table ORDER BY table;
    SELECT 'a rows: ' || formatRowNoNewline('CSV', *) FROM d.a;
    SELECT 'b rows: ' || formatRowNoNewline('CSV', *) FROM d.b;
    SELECT 'x rows: ' || formatRowNoNewline('CSV', *) FROM d.x;
    SELECT 'y rows: ' || formatRowNoNewline('CSV', *) FROM d.y;
"

# Prints the output $1 when there are no more arguments, or else the first of the error codes and phrases $2... in it.
function print_output_or_error()
{
    local output=$1
    shift
    if [ $# -eq 0 ]
    then
        echo "$output"
        return
    fi
    local patterns=()
    for pattern in "$@"
    do
        patterns+=(-e "$pattern")
    done
    echo "$output" | grep -o -m1 "${patterns[@]}"
}

# Attaches the database as from a read-only disk, and prints the tables, or the first of the error codes and phrases $@ in the
# error of the load.
function attach_read_only_and_print()
{
    local output
    output=$($CLICKHOUSE_LOCAL --path "$LOCAL_PATH" -q "
        SYSTEM ENABLE FAILPOINT database_atomic_load_metadata_disk_read_only;
        ATTACH DATABASE d UUID '$UUID' ENGINE = Atomic;
        SYSTEM DISABLE FAILPOINT database_atomic_load_metadata_disk_read_only;
        $PRINT_TABLES
        DETACH DATABASE d;
    " 2>&1)
    forget_database
    print_output_or_error "$output" "$@"
}

# Loads the database from a writable disk at the start of `clickhouse-local`, and prints the tables.
function load_writable_and_print()
{
    cp "$DB_SQL" "$DB_METADATA_FILE"
    local output
    output=$($CLICKHOUSE_LOCAL --path "$LOCAL_PATH" -q "$PRINT_TABLES" 2>&1)
    forget_database
    print_output_or_error "$output"
}

# Prints whether file $2 holds the same content as file $3, under the name $1.
function print_same()
{
    if cmp -s "$2" "$3"; then echo "$1 is untouched"; else echo "$1 is changed"; fi
}

function print_dot_files()
{
    # shellcheck disable=SC2010
    ls -A "$DIR" | grep '^\.' | LC_ALL=C sort
}

echo "case 1: the record only, before the swap"
record_a_b > "$DIR/.a.sql.exchange"
cp "$DIR/.a.sql.exchange" "$RECORD"
attach_read_only_and_print
print_dot_files
print_same "the record" "$DIR/.a.sql.exchange" "$RECORD"
echo
restore_main

echo "case 2: after the third move, before the record is removed"
record_a_b > "$DIR/.a.sql.exchange"
cp "$DIR/.a.sql.exchange" "$RECORD"
cp "$B" "$DIR/a.sql"
cp "$A" "$DIR/b.sql"
attach_read_only_and_print INCORRECT_FILE_NAME 'the disk is read-only'
print_dot_files
print_same "the record" "$DIR/.a.sql.exchange" "$RECORD"
print_same "a.sql" "$DIR/a.sql" "$B"
print_same "b.sql" "$DIR/b.sql" "$A"
# As the error says, a load from a writable disk undoes the swap.
load_writable_and_print
print_dot_files
echo
restore_main

echo "case 3: after the first move"
record_a_b > "$DIR/.a.sql.exchange"
cp "$A" "$DIR/a.sql.tmp_exchange"
rm -f -- "$DIR/a.sql"
attach_read_only_and_print INCORRECT_FILE_NAME 'the disk is read-only'
print_dot_files
print_same "the temporary file" "$DIR/a.sql.tmp_exchange" "$A"
print_same "b.sql" "$DIR/b.sql" "$B"
if [ -f "$DIR/a.sql" ]; then echo "a.sql is back"; else echo "a.sql is missing"; fi
echo
restore_main

echo "case 4: the record only, and a metadata file with neither of the recorded contents"
record_a_b > "$DIR/.a.sql.exchange"
cp "$CHANGED" "$DIR/a.sql"
attach_read_only_and_print CORRUPTED_DATA INCORRECT_FILE_NAME
print_dot_files
print_same "a.sql" "$DIR/a.sql" "$CHANGED"
print_same "b.sql" "$DIR/b.sql" "$B"
echo
restore_main

# The load inspects every record before it advises a writable disk, so the order in which it lists the two records does not
# matter. Cases 5 and 6 put the swap that needs an undo and the swap with a changed file in the two orders of the names.
echo "case 5: a swap that needs an undo, and a swap with a metadata file that holds neither of the recorded contents"
record_a_b > "$DIR/.a.sql.exchange"
cp "$B" "$DIR/a.sql"
cp "$A" "$DIR/b.sql"
record_x_y > "$DIR/.x.sql.exchange"
cp "$CHANGED" "$DIR/x.sql"
attach_read_only_and_print CORRUPTED_DATA INCORRECT_FILE_NAME
print_dot_files
print_same "a.sql" "$DIR/a.sql" "$B"
print_same "x.sql" "$DIR/x.sql" "$CHANGED"
echo
restore_main

echo "case 6: a swap with a metadata file that holds neither of the recorded contents, and a swap that needs an undo"
record_a_b > "$DIR/.a.sql.exchange"
cp "$CHANGED" "$DIR/a.sql"
record_x_y > "$DIR/.x.sql.exchange"
cp "$Y" "$DIR/x.sql"
cp "$X" "$DIR/y.sql"
attach_read_only_and_print CORRUPTED_DATA INCORRECT_FILE_NAME
print_dot_files
print_same "a.sql" "$DIR/a.sql" "$CHANGED"
print_same "x.sql" "$DIR/x.sql" "$Y"
echo
restore_main

echo "case 7: the temporary file without a record"
cp "$B" "$DIR/a.sql.tmp_exchange"
attach_read_only_and_print INCORRECT_FILE_NAME 'on a local disk that is writable'
print_dot_files
if [ -f "$DIR/a.sql.tmp_exchange" ]; then echo "the temporary file is left"; fi
echo
restore_main

echo "after the cases"
attach_read_only_and_print
print_dot_files
