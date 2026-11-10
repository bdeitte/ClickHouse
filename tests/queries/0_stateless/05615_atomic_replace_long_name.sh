#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the failpoint is server-wide and would change the swaps of other tests.
# Tag no-fasttest: a test that runs alone is kept out of the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A swap with a record writes files with names longer than the metadata file of a table, such as `.<table>.sql.exchange_write`.
# `CREATE` and `RENAME` limit table names so that those names fit, but `ATTACH` does not, and a refresh swaps its target table
# without the check. A refresh whose target has a longer name fails before the swap writes its record, so it leaves no record
# that a load could not undo, and the target is not fenced.

DB="${CLICKHOUSE_DATABASE}_long"
# `.<name>.sql.exchange_write` has 260 bytes, over the 255 that common filesystems allow, while `<name>.sql` has 244.
LONG=$(printf 'a%.0s' {1..240})

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT database_atomic_exchange_not_supported"
    # `DROP DATABASE` cannot drop a table whose name is too long for its file in `metadata_dropped`, so it gets a short name first.
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '$DB' AND table = '$LONG'")" = "1" ]
    then
        $CLICKHOUSE_CLIENT -q "RENAME TABLE $DB.$LONG TO $DB.target"
    fi
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT database_atomic_exchange_not_supported"

DISK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.disks WHERE name = 'default'")

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE $DB ENGINE = Atomic;
    CREATE TABLE $DB.src (x UInt8) ENGINE = MergeTree ORDER BY x;
"
REL=$($CLICKHOUSE_CLIENT -q "SELECT metadata_path FROM system.databases WHERE name = '$DB'")
DIR="${DISK_PATH%/}/${REL}"
if [ -z "$DISK_PATH" ] || [ -z "$REL" ] || [[ "$DIR" != */store/* ]]
then
    echo "Cannot find the metadata directory of $DB: disk path '$DISK_PATH', metadata path '$REL'" >&2
    exit 1
fi

# Swaps never use a record when the metadata of the database is not on the local filesystem (for example, a `database_disk`
# on object storage). The directory of a database exists once it has a table.
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

# An `Atomic` database attaches a table with a definition only with a `UUID`.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT -q "
    ATTACH TABLE $DB.$LONG UUID '$UUID' (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $DB.$LONG VALUES (1);
    SELECT 'the name is longer than CREATE allows: ' || toString(length('$LONG') > getMaxTableNameLengthForDatabase('$DB'));
    CREATE MATERIALIZED VIEW $DB.mv REFRESH EVERY 1 YEAR SETTINGS refresh_retries = 0 TO $DB.$LONG EMPTY AS SELECT 2 AS x;
    SYSTEM REFRESH VIEW $DB.mv;
    SYSTEM WAIT VIEW $DB.mv; -- { serverError REFRESH_FAILED }
    SELECT 'the refresh fails on the length of a name: '
        || toString(exception LIKE '%Cannot exchange % with a record: the name of the file %the directory allows at most%Rename the table to a shorter name%')
    FROM system.view_refreshes WHERE database = '$DB' AND view = 'mv';
"

echo "files of a swap left: $(find "$DIR" -maxdepth 1 -name '*exchange*' | wc -l | tr -d ' ')"

# The target is the table from before the refresh, and DDL on it works.
$CLICKHOUSE_CLIENT -q "
    SELECT 'rows: ' || arrayStringConcat(groupArray(toString(x)), ',') FROM (SELECT x FROM $DB.$LONG ORDER BY x);
    ALTER TABLE $DB.$LONG ADD COLUMN y UInt8;
    SELECT 'columns: ' || arrayStringConcat(groupArray(name), ',') FROM (SELECT name FROM system.columns
        WHERE database = '$DB' AND table = '$LONG' ORDER BY position);
"
