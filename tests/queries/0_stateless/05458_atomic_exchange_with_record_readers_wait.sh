#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the failpoints of the exchange are server-wide and would change the exchanges of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Between the moves of an exchange with a record, the first metadata file is missing. A reader that does not take
# the database mutex, such as `SHOW CREATE TABLE` after it has found the table, waits for the moves on
# `metadata_files_mutex` instead of failing on the missing file. The failpoints hold the reader after it has found the
# table, and the exchange after its first move, then release the reader while the file is missing.

DB="${CLICKHOUSE_DATABASE}_readers_wait"
READER_FAILPOINT="database_atomic_pause_before_reading_create_query"
EXCHANGE_FAILPOINT="database_atomic_exchange_pause_after_first_move"

function cleanup()
{
    for failpoint in database_atomic_exchange_with_record "$READER_FAILPOINT" "$EXCHANGE_FAILPOINT"
    do
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"
    done
    wait
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $DB ENGINE = Atomic;
    CREATE TABLE $DB.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
"

# Exchanges never use a record when the server setting turns the emulation off, or when the metadata of the databases
# is not on the local filesystem (for example, a `database_disk` on object storage). The exchange would then never
# pause, and `SYSTEM WAIT FAILPOINT` below would wait forever. The directory of a database exists once it has a table.
EMULATION=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'database_atomic_allow_exchange_emulation'")
DISK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")
REL=$($CLICKHOUSE_CLIENT -q "SELECT metadata_path FROM system.databases WHERE name = '$DB'")
if [ -z "$DISK_PATH" ] || [ -z "$REL" ] || [[ "${DISK_PATH%/}/$REL" != */store/* ]]
then
    echo "Cannot find the metadata directory of $DB: disk path '$DISK_PATH', metadata path '$REL'" >&2
    exit 1
fi
if [ "$EMULATION" != "1" ] || [ ! -d "${DISK_PATH%/}/$REL" ]
then
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

# The reader finds the table and stops before it reads the metadata file. Only a query whose id starts with
# `atomic_pause_before_reading_create_query_` stops there, so that no other reader of metadata takes the pause.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $READER_FAILPOINT"
(
    output=$($CLICKHOUSE_CLIENT --query_id "atomic_pause_before_reading_create_query_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM" \
        -q "SHOW CREATE TABLE $DB.a FORMAT TSVRaw" 2>&1)
    if [[ "$output" == *'`y` UInt8'* ]]
    then
        echo "the reader sees the exchanged table"
    else
        echo "the reader failed: $output"
    fi
) &
reader_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $READER_FAILPOINT PAUSE"

# The exchange stops after its first move, while `a.sql` is missing.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_with_record"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $EXCHANGE_FAILPOINT"
$CLICKHOUSE_CLIENT -q "EXCHANGE TABLES $DB.a AND $DB.b" &
exchange_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $EXCHANGE_FAILPOINT PAUSE"

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

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $EXCHANGE_FAILPOINT"
wait "$exchange_pid"
wait "$reader_pid"
