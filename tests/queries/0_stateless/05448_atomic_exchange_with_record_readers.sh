#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the failpoint is server-wide and would change the exchanges of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An exchange with a record moves the two metadata files one after the other, so between the moves one of them is missing.
# The readers of metadata files that do not take the database mutex, `SHOW CREATE TABLE`, `system.tables` and `BACKUP`,
# wait for the moves, so they never see the missing file. The failpoint `database_atomic_exchange_with_record` forces
# the record, and the test reads the metadata of the two tables while they are exchanged again and again.
# The test is not expected to detect a missing lock in every run, because a read must fall between two moves for that.
# Every run checks that the readers and the exchanges do not deadlock.

DB="${CLICKHOUSE_DATABASE}_readers"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_with_record"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $DB ENGINE = Atomic;
    CREATE TABLE $DB.a (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE $DB.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_with_record"

# An even number of exchanges, so that `a` ends with its own definition.
EXCHANGES=$(for _ in $(seq 1 400); do echo "EXCHANGE TABLES $DB.a AND $DB.b;"; done)
$CLICKHOUSE_CLIENT -q "$EXCHANGES" &
EXCHANGES_PID=$!

# `SHOW CREATE TABLE` fails when the file is missing. `system.tables` shows an empty definition
# and a zero modification time instead.
READS=$(for _ in $(seq 1 20); do
    echo "SHOW CREATE TABLE $DB.a FORMAT Null;"
    echo "SHOW CREATE TABLE $DB.b FORMAT Null;"
    echo "SELECT 'cannot read the metadata of ' || name FROM system.tables
          WHERE database = '$DB' AND (create_table_query = '' OR metadata_modification_time = 0);"
done)
while kill -0 "$EXCHANGES_PID" 2>/dev/null
do
    $CLICKHOUSE_CLIENT -q "$READS"
done
wait "$EXCHANGES_PID"

$CLICKHOUSE_CLIENT -q "SELECT table, arraySort(groupArray(name)) FROM system.columns WHERE database = '$DB' GROUP BY table ORDER BY table"
