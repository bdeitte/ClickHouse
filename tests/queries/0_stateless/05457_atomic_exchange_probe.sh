#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: the failpoints are server-wide and would change the exchanges of other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Before its first exchange, a database probes whether its filesystem exchanges two files atomically, and caches the answer.
# The failpoint `database_atomic_exchange_probe_not_supported` makes the probe find no atomic exchange. The failpoint
# `database_atomic_exchange_probe_as_rename` makes the probe do a plain rename that reports success, which the probe must
# detect from the contents of its files. Either way, the exchanges in the database use a record, also after the failpoint
# is disabled, and the probe leaves no files behind.

DB="${CLICKHOUSE_DATABASE}_probe"
FAILPOINTS="database_atomic_exchange_probe_not_supported database_atomic_exchange_probe_as_rename"

function cleanup()
{
    for failpoint in $FAILPOINTS
    do
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"
    done
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
}
trap cleanup EXIT

DISK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")

# Sets the variable named $1 to the absolute metadata directory of $DB.
# Exits when it is not a database directory, so that no path below can be empty.
function set_metadata_dir()
{
    local rel
    rel=$($CLICKHOUSE_CLIENT -q "SELECT metadata_path FROM system.databases WHERE name = '$DB'")
    local abs="${DISK_PATH%/}/${rel}"
    if [ -z "$DISK_PATH" ] || [ -z "$rel" ] || [[ "$abs" != */store/* ]]
    then
        echo "Cannot find the metadata directory of $DB: disk path '$DISK_PATH', metadata path '$rel'" >&2
        exit 1
    fi
    printf -v "$1" '%s' "$abs"
}

function now_us()
{
    $CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))"
}

function create_tables()
{
    $CLICKHOUSE_CLIENT -q "
        DROP DATABASE IF EXISTS $DB SYNC;
        CREATE DATABASE $DB ENGINE = Atomic;
        CREATE TABLE $DB.a (x UInt8) ENGINE = MergeTree ORDER BY x;
        CREATE TABLE $DB.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    "
}

# Exchanges never probe when the server setting turns the emulation off, or when the metadata of the databases is not
# on the local filesystem (for example, a `database_disk` on object storage). The directory of a database exists once
# it has a table. Both checks run before the loop prints anything, so that the skip prints the reference alone.
EMULATION=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'database_atomic_allow_exchange_emulation'")
create_tables
set_metadata_dir DIR
if [ "$EMULATION" != "1" ] || [ ! -d "$DIR" ]
then
    cat "${BASH_SOURCE[0]%.sh}.reference"
    exit 0
fi

for failpoint in $FAILPOINTS
do
    echo "$failpoint"
    create_tables
    set_metadata_dir DIR

    since=$(now_us)
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $failpoint"
    # The probe logs a warning when the exchange did not swap its files, which `text_log` checks below.
    $CLICKHOUSE_CLIENT --send_logs_level=error -q "EXCHANGE TABLES $DB.a AND $DB.b"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"
    $CLICKHOUSE_CLIENT -q "SELECT 'after the first exchange, has y: ' || arrayStringConcat(arraySort(groupArray(table)), ', ')
        FROM system.columns WHERE database = '$DB' AND name = 'y'"

    # The second exchange uses the cached answer of the probe.
    $CLICKHOUSE_CLIENT -q "EXCHANGE TABLES $DB.a AND $DB.b"
    $CLICKHOUSE_CLIENT -q "SELECT 'after the second exchange, has y: ' || arrayStringConcat(arraySort(groupArray(table)), ', ')
        FROM system.columns WHERE database = '$DB' AND name = 'y'"

    # shellcheck disable=SC2010
    echo "dot-files: $(ls -A "$DIR" | grep -c '^\.')"

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT
            'exchanges with a record: ' || toString(countIf(message LIKE 'Exchanging % and % with an exchange record')),
            'probe found no atomic exchange: ' || toString(countIf(message LIKE 'The filesystem of % cannot exchange two files atomically%')),
            'probe found no swap: ' || toString(countIf(message LIKE 'The filesystem of % reported an exchange of two files as done but did not swap them%'))
        FROM system.text_log
        WHERE event_date >= yesterday() AND toUnixTimestamp64Micro(event_time_microseconds) >= $since
            AND endsWith(logger_name, '($DB)')
        FORMAT TSV"
done
