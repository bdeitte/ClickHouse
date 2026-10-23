#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# Tag no-parallel: the failpoint is server-wide and would change the exchanges of other tests.

# Finishing an exchange logs a warning.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Loading a `Replicated` database finishes the exchanges of metadata files that a crash interrupted, so that the files
# agree with Keeper without lost-replica recovery. A real `EXCHANGE TABLES` with the failpoint
# `database_atomic_exchange_with_record` puts the exchange in Keeper. Each case detaches the database, writes the files
# that the exchange leaves after one of its moves, and attaches the database again. The digest comparison that the
# attach causes runs later on the thread of the DDL worker, so each case waits for its message in `system.text_log`.

DB="${CLICKHOUSE_DATABASE}_replicated"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/exchange_recovery"

CLIENT="$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none"

# The metadata files after the exchange, saved once the metadata directory is checked.
SAVED_A=""
SAVED_B=""
DIR=""

# Removes the files the cases write in the metadata directory, by exact names.
function remove_case_files()
{
    rm -f -- "$DIR/.a.sql.exchange" "$DIR/.a.sql.exchange.tmp" "$DIR/a.sql.tmp_exchange"
}

# Puts back the metadata files the exchange left.
function restore()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE IF EXISTS $DB"
    remove_case_files
    cp "$SAVED_A" "$DIR/a.sql"
    cp "$SAVED_B" "$DIR/b.sql"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"
}

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_with_record"
    if [ -n "$DIR" ] && [ -n "$SAVED_A" ] && [ -f "$SAVED_A" ] && [ -f "$SAVED_B" ]
    then
        restore
    fi
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
    if [ -n "$SAVED_A" ]
    then
        rm -f -- "$SAVED_A" "$SAVED_B"
    fi
}
trap cleanup EXIT

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

$CLIENT -q "CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 'shard1', 'replica1')"
$CLIENT -q "CREATE TABLE $DB.a (x UInt8) ENGINE = MergeTree ORDER BY x"
set_metadata_dir "$DB" REL DIR_CHECKED

# The metadata of the database is not on the local filesystem (for example, a `database_disk` on object storage),
# where loading does not finish exchanges. The directory of a database exists once it has a table.
if [ ! -d "$DIR_CHECKED" ]
then
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

$CLIENT -q "CREATE TABLE $DB.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_with_record"
$CLIENT -q "EXCHANGE TABLES $DB.a AND $DB.b"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_with_record"

# After the exchange, `a.sql` holds the metadata of the original `b`, and `b.sql` holds the metadata of the original `a`.
DIR="$DIR_CHECKED"
SAVED="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
SAVED_A="${SAVED}_a.sql"
SAVED_B="${SAVED}_b.sql"
cp "$DIR/a.sql" "$SAVED_A"
cp "$DIR/b.sql" "$SAVED_B"

# Prints the record of the exchange: `exchange v1`, the two paths, and the contents the two paths hold after the exchange.
# The escaping of `TSV` is the escaping of the record.
function exchange_record()
{
    $CLICKHOUSE_LOCAL -q "
        SELECT arrayJoin([
            'exchange v1', '${REL}a.sql', '${REL}b.sql',
            (SELECT raw_blob FROM file('$SAVED_A', 'RawBLOB')),
            (SELECT raw_blob FROM file('$SAVED_B', 'RawBLOB'))]) FORMAT TSV"
}

# The condition on `system.text_log` for the messages of the DDL worker of the database since the time $1.
function worker_messages_since()
{
    echo "event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) > $1
        AND endsWith(logger_name, '($DB)')"
}

# Attaches the database, waits for the digest comparison that the attach causes, and prints its results.
function attach_and_check()
{
    local since
    since=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE $DB"

    local digests_equal=""
    for _ in $(seq 1 120)
    do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
        digests_equal=$($CLICKHOUSE_CLIENT -q "
            SELECT extract(message, 'local_digest=([0-9]+)') = extract(message, 'zk_digest=([0-9]+)')
            FROM system.text_log
            WHERE $(worker_messages_since "$since") AND message LIKE 'Trying to initialize replication%'
            ORDER BY event_time_microseconds
            LIMIT 1")
        if [ -n "$digests_equal" ]
        then
            break
        fi
        sleep 0.5
    done
    if [ -z "$digests_equal" ]
    then
        echo "timeout"
        return
    fi

    echo "local digest equals Keeper digest: $digests_equal"
    $CLICKHOUSE_CLIENT -q "
        SELECT 'lost replica messages: ' || toString(count())
        FROM system.text_log
        WHERE $(worker_messages_since "$since") AND message LIKE 'Replica seems to be lost%'"
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = '$DB' AND table = 'a' AND name = 'y'")" = 1 ]
    then
        echo "exchanged"
    else
        echo "not exchanged"
    fi
    # shellcheck disable=SC2010
    echo "dot-files: $(ls -A "$DIR" | grep -c '^\.')"
}

echo "after the first move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
exchange_record > "$DIR/.a.sql.exchange"
cp "$SAVED_B" "$DIR/a.sql.tmp_exchange"
cp "$SAVED_A" "$DIR/b.sql"
rm -f -- "$DIR/a.sql"
attach_and_check
echo
restore

echo "after the second move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
exchange_record > "$DIR/.a.sql.exchange"
cp "$SAVED_B" "$DIR/a.sql.tmp_exchange"
cp "$SAVED_A" "$DIR/a.sql"
rm -f -- "$DIR/b.sql"
attach_and_check
echo
restore

echo "after the third move"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $DB"
exchange_record > "$DIR/.a.sql.exchange"
cp "$SAVED_A" "$DIR/a.sql"
cp "$SAVED_B" "$DIR/b.sql"
attach_and_check
