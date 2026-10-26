import os
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster

# When the swap of metadata files of `CREATE OR REPLACE` or of a refresh with a record fails after a move and cannot
# be finished from the record, DDL on the two tables and reads of their definitions are fenced until the server
# restarts, and the load finishes the swap. A fenced database refuses `DROP DATABASE`, so a stateless test cannot clean up
# after it. The failpoint `database_atomic_exchange_not_supported` makes the swap take the record path, which also
# keeps the real `RENAME_EXCHANGE` away from instance directories that do not support it.

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/exchange_emulation.xml", "configs/backups.xml"],
    user_configs=["configs/stop_refreshes_on_startup.xml"],
    stay_alive=True,
)

EXCHANGE_EMULATION_CONFIG = "/etc/clickhouse-server/config.d/exchange_emulation.xml"

# The number of entries in `system.warnings` about a fenced swap of the tables of `fence_db`.
FENCE_WARNINGS_QUERY = (
    "SELECT count() FROM system.warnings"
    " WHERE message LIKE '%`fence_db`.`t` failed and could not be finished%'"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@contextmanager
def failpoints_enabled(failpoints):
    # The failpoints are server-wide, so a failed assertion must not leave them enabled.
    try:
        for failpoint in failpoints:
            node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        yield
    finally:
        for failpoint in failpoints:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")


def metadata_files(database):
    rel = node.query(
        f"SELECT metadata_path FROM system.databases WHERE name = '{database}'"
    ).strip()
    assert rel.startswith("store/"), rel
    return node.exec_in_container(["ls", "-A", f"/var/lib/clickhouse/{rel}"]).split()


def test_fence_until_restart(started_cluster):
    with open(
        os.path.join(os.path.dirname(__file__), "configs", "exchange_emulation.xml")
    ) as f:
        original_config = f.read()
    try:
        node.query("CREATE DATABASE fence_db ENGINE = Atomic")
        node.query("CREATE TABLE fence_db.t (x UInt8) ENGINE = MergeTree ORDER BY x")
        node.query("INSERT INTO fence_db.t VALUES (1)")
        # Not part of the swap: the fence does not cover it.
        node.query("CREATE TABLE fence_db.u (x UInt8) ENGINE = MergeTree ORDER BY x")

        # The first move fails, and so does the finish from the record, so the two tables are fenced.
        with failpoints_enabled(
            [
                "database_atomic_exchange_not_supported",
                "database_atomic_exchange_fail_after_first_move",
                "database_atomic_exchange_fail_to_finish",
            ]
        ):
            error = node.query_and_get_error(
                "CREATE OR REPLACE TABLE fence_db.t (x UInt8, y UInt8)"
                " ENGINE = MergeTree ORDER BY x AS SELECT 2, 3"
            )
        assert "FAULT_INJECTED" in error
        assert "until the server restarts" in error
        # The fence is also shown to queries other than the one that hit it.
        assert node.query(FENCE_WARNINGS_QUERY) == "1\n"

        error = node.query_and_get_error("DROP TABLE fence_db.t")
        assert "UNFINISHED_METADATA_EXCHANGE" in error
        assert "until the server restarts" in error
        for query in [
            "ALTER TABLE fence_db.t ADD COLUMN z UInt8",
            "SHOW CREATE TABLE fence_db.t",
            "BACKUP TABLE fence_db.t TO Disk('backups', 'fence_t')",
            "DROP DATABASE fence_db",
            "DETACH DATABASE fence_db",
        ]:
            assert "UNFINISHED_METADATA_EXCHANGE" in node.query_and_get_error(query), query
        # The fence does not refuse inserts, and DDL on the other tables of the database goes on.
        node.query("INSERT INTO fence_db.t VALUES (5)")
        node.query("ALTER TABLE fence_db.u ADD COLUMN z UInt8")

        # The guards refused the `DROP TABLE` of `t` above and the cleanup `DROP` of the temporary table before they
        # shut the tables down. A `MergeTree` table that was prepared for shutdown refuses `OPTIMIZE` with `ABORTED`.
        fenced_tmp_tables = node.query(
            "SELECT name FROM system.tables"
            " WHERE database = 'fence_db' AND startsWith(name, '_tmp_replace_')"
        ).split()
        assert len(fenced_tmp_tables) == 1, fenced_tmp_tables
        fenced_tmp_table = fenced_tmp_tables[0]
        node.query("OPTIMIZE TABLE fence_db.t FINAL")
        node.query(f"OPTIMIZE TABLE fence_db.`{fenced_tmp_table}` FINAL")
        # The definitions of the two tables cannot be read.
        assert (
            node.query(
                "SELECT count() FROM system.tables"
                f" WHERE database = 'fence_db' AND name IN ('t', '{fenced_tmp_table}')"
                " AND create_table_query = ''"
            )
            == "2\n"
        )

        # A refresh of a view with an inner table that is not `APPEND` swaps the `.tmp` table it fills with the inner table.
        # Only the refresh below may swap the files, under the failpoints. `EMPTY` skips the first refresh. The view starts
        # stopped, because of `stop_refreshable_materialized_views_on_startup`, which also keeps it from refreshing when
        # the restart loads it. Started, it schedules its next refresh at the start of the next year.
        node.query("CREATE TABLE fence_db.src (x UInt8) ENGINE = MergeTree ORDER BY x")
        node.query("INSERT INTO fence_db.src VALUES (7)")
        node.query(
            "CREATE MATERIALIZED VIEW fence_db.mv REFRESH EVERY 1 YEAR SETTINGS refresh_retries = 0"
            " (x UInt8) ENGINE = MergeTree ORDER BY x EMPTY AS SELECT x FROM fence_db.src"
        )
        node.query("SYSTEM START VIEW fence_db.mv")
        with failpoints_enabled(
            [
                "database_atomic_exchange_not_supported",
                "database_atomic_exchange_fail_after_first_move",
                "database_atomic_exchange_fail_to_finish",
            ]
        ):
            node.query("SYSTEM REFRESH VIEW fence_db.mv")
            error = node.query_and_get_error("SYSTEM WAIT VIEW fence_db.mv")
        assert "REFRESH_FAILED" in error
        assert "until the server restarts" in error
        # The fence is on the two inner tables, not on the view, but `DROP VIEW` drops the inner tables, so it is refused
        # before it shuts the view down. A view that `DROP` shut down is gone from `system.view_refreshes`.
        view_refreshes_query = (
            "SELECT count() FROM system.view_refreshes"
            " WHERE database = 'fence_db' AND view = 'mv'"
        )
        error = node.query_and_get_error("DROP VIEW fence_db.mv")
        assert "UNFINISHED_METADATA_EXCHANGE" in error
        assert node.query(view_refreshes_query) == "1\n"

        # The load finishes the swap whatever the setting is.
        node.replace_config(
            EXCHANGE_EMULATION_CONFIG,
            "<clickhouse><database_atomic_allow_exchange_emulation>0"
            "</database_atomic_allow_exchange_emulation></clickhouse>",
        )
        node.restart_clickhouse()
        assert (
            node.query(
                "SELECT value FROM system.server_settings"
                " WHERE name = 'database_atomic_allow_exchange_emulation'"
            )
            == "0\n"
        )

        assert (
            node.query(
                "SELECT count() FROM system.columns"
                " WHERE database = 'fence_db' AND table = 't' AND name = 'y'"
            )
            == "1\n"
        )
        # The row inserted under the fence went to the table that the swap replaced, so it is not in `t`.
        assert node.query("SELECT * FROM fence_db.t") == "2\t3\n"
        # The fence refused the cleanup `DROP` of the temporary table, so the replaced table is still there under
        # the temporary name, with the row inserted under the fence.
        tmp_tables = node.query(
            "SELECT name FROM system.tables"
            " WHERE database = 'fence_db' AND startsWith(name, '_tmp_replace_')"
        ).split()
        assert len(tmp_tables) == 1, tmp_tables
        tmp_table = tmp_tables[0]
        assert (
            node.query(f"SELECT x FROM fence_db.`{tmp_table}` ORDER BY x") == "1\n5\n"
        )
        files = metadata_files("fence_db")
        assert not any(name.startswith(".") for name in files), files
        assert not any(name.endswith(".tmp_exchange") for name in files), files

        # The fence is gone, and so is its warning.
        assert node.query(FENCE_WARNINGS_QUERY) == "0\n"
        node.query(f"DROP TABLE fence_db.`{tmp_table}` SYNC")
        node.query("ALTER TABLE fence_db.t ADD COLUMN z UInt8")

        # The load also finished the swap of the refresh: the view reads the refreshed row, and the `.tmp` table that
        # the fence kept is the inner table from before the refresh, which was empty.
        assert node.query("SELECT x FROM fence_db.mv") == "7\n"
        leftover_tmp_tables = node.query(
            "SELECT name FROM system.tables"
            " WHERE database = 'fence_db' AND startsWith(name, '.tmp.inner')"
        ).split()
        assert len(leftover_tmp_tables) == 1, leftover_tmp_tables
        assert (
            node.query(f"SELECT count() FROM fence_db.`{leftover_tmp_tables[0]}`")
            == "0\n"
        )
        # `DROP VIEW` drops the inner table and the leftover `.tmp` table.
        node.query("DROP VIEW fence_db.mv SYNC")
        assert (
            node.query(
                "SELECT count() FROM system.tables"
                " WHERE database = 'fence_db' AND startsWith(name, '.')"
            )
            == "0\n"
        )

        # With the setting off, the default, a swap is not emulated: on a filesystem without an atomic exchange,
        # which the failpoint simulates, `CREATE OR REPLACE` fails, mentions the setting, and leaves nothing behind.
        node.query("CREATE TABLE fence_db.v (x UInt8) ENGINE = MergeTree ORDER BY x")
        with failpoints_enabled(["database_atomic_exchange_not_supported"]):
            error = node.query_and_get_error(
                "CREATE OR REPLACE TABLE fence_db.v (x UInt8, y UInt8)"
                " ENGINE = MergeTree ORDER BY x AS SELECT 2, 3"
            )
        assert "UNSUPPORTED_METHOD" in error
        assert "database_atomic_allow_exchange_emulation" in error
        # That message is only for the setting on.
        assert "are not emulated" not in error
        assert (
            node.query(
                "SELECT name FROM system.columns"
                " WHERE database = 'fence_db' AND table = 'v'"
            )
            == "x\n"
        )
        files = metadata_files("fence_db")
        assert not any(name.startswith(".") for name in files), files
        assert not any(name.endswith(".tmp_exchange") for name in files), files
        assert (
            node.query(
                "SELECT count() FROM system.tables"
                " WHERE database = 'fence_db' AND startsWith(name, '_tmp_replace_')"
            )
            == "0\n"
        )
    finally:
        node.replace_config(EXCHANGE_EMULATION_CONFIG, original_config)
        # Not `query`: when an assertion fails before the restart, the fence refuses the drop, and its error would
        # hide the failure.
        node.query_and_get_answer_with_error("DROP DATABASE IF EXISTS fence_db SYNC")
