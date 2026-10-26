#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the failpoints are server-wide and would change the swaps of other tests.
# Tag no-fasttest: a test that runs alone is kept out of the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Between the moves of a swap with a record, a metadata file is missing. A reader that does not take the database mutex,
# such as `SHOW CREATE TABLE` after it has found the table, waits for the moves on `metadata_files_mutex` instead of failing
# on the missing file. `REPLACE TABLE` moves the file of the temporary table first, so `t.sql` is missing after the second move.
# The failpoints hold the reader after it has found the table, and the swap after its second move, then release the reader
# while the file is missing.

DB="${CLICKHOUSE_DATABASE}_atomic"
READER_FAILPOINT="database_atomic_pause_before_reading_create_query"
SWAP_FAILPOINT="database_atomic_exchange_pause_after_second_move"

function cleanup()
{
    for failpoint in database_atomic_exchange_not_supported "$READER_FAILPOINT" "$SWAP_FAILPOINT"
    do
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"
    done
    wait
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

# Swaps never use a record when the metadata of the database is not on the local filesystem (for example, a `database_disk`
# on object storage). The swap would then never pause, and `SYSTEM WAIT FAILPOINT` below would wait forever.
# The directory of a database exists once it has a table.
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

# The reader finds the table and stops before it reads the metadata file. Only a query whose id starts with
# `atomic_pause_before_reading_create_query_` stops there, so that no other reader of metadata takes the pause.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $READER_FAILPOINT"
(
    output=$($CLICKHOUSE_CLIENT --query_id "atomic_pause_before_reading_create_query_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM" \
        -q "SHOW CREATE TABLE $DB.t FORMAT TSVRaw" 2>&1)
    if [[ "$output" == *'`y` UInt8'* ]]
    then
        echo "the reader sees the replaced table"
    else
        echo "the reader failed: $output"
    fi
) &
reader_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $READER_FAILPOINT PAUSE"

# The swap stops after its second move, while `t.sql` is missing. Only a query whose id starts with
# `atomic_exchange_pause_after_second_move_` stops there, so that no other swap takes the pause.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $SWAP_FAILPOINT"
$CLICKHOUSE_CLIENT --query_id "atomic_exchange_pause_after_second_move_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM" \
    -q "REPLACE TABLE $DB.t (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x" &
swap_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $SWAP_FAILPOINT PAUSE"
if [ -f "$DIR/t.sql" ]
then
    echo "t.sql exists during the pause"
else
    echo "t.sql is missing during the pause"
fi

# Released now, a reader without the lock reads the missing file and fails at once. With the lock, it waits.
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $READER_FAILPOINT"
for _ in {1..20}
do
    kill -0 "$reader_pid" 2>/dev/null || break
    sleep 0.1
done
if kill -0 "$reader_pid" 2>/dev/null
then
    echo "the reader waits for the moves"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $SWAP_FAILPOINT"
wait "$swap_pid"
wait "$reader_pid"
