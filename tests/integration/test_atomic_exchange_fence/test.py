from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# When an exchange of metadata files with a record fails after the Keeper commit and cannot be rolled
# forward from the record, DDL on the two tables is fenced until a restart, and in a `Replicated` database
# DDL on every table, because its digest reads every metadata file. This needs a restart to recover,
# so a stateless test cannot cover it. The failpoint `database_atomic_exchange_with_record` forces the
# record path, and other failpoints fail the moves of the exchange and the handling of the failure.

cluster = ClickHouseCluster(__file__)

# The restart attaches the `ReplicatedMergeTree` tables outside of a DDL query, so their default
# Keeper path takes `{shard}` and `{replica}` from these macros, not from the `Replicated` database.
node = cluster.add_instance(
    "node",
    main_configs=["configs/exchange_emulation.xml", "configs/cluster_and_backups.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "shard1", "replica": "replica1"},
)
# Holds a second replica of a `Replicated` database. A second replica on `node` cannot recreate the
# tables of the database, because their UUIDs are already taken there.
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@contextmanager
def failpoints_enabled(failpoints):
    # The failpoints are server-wide, so a failed assertion must not leave them enabled for the next tests.
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
    return node.exec_in_container(
        ["bash", "-c", f"ls -A /var/lib/clickhouse/{rel}"]
    ).split()


def has_column(database, table, column):
    return node.query(
        f"SELECT count() FROM system.columns"
        f" WHERE database = '{database}' AND table = '{table}' AND name = '{column}'"
    ).strip()


def test_fence_until_restart(started_cluster):
    node.query(
        "CREATE DATABASE fence_db"
        " ENGINE = Replicated('/clickhouse/databases/fence_db', 'shard1', 'replica1')"
    )
    # `ReplicatedMergeTree`, because the digest repair after the restart matches such tables by UUID
    # and renames them, keeping their data. It would recreate exchanged plain `MergeTree` tables
    # from Keeper metadata and move the originals with the data to the broken-tables database.
    node.query(
        "CREATE TABLE fence_db.a (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    node.query(
        "CREATE TABLE fence_db.b (x UInt8, y UInt8)"
        " ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    node.query("INSERT INTO fence_db.b VALUES (1, 2)")
    # Not part of the exchange: the fence refuses DDL on them all the same.
    node.query(
        "CREATE TABLE fence_db.untouched (x UInt8) ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    # The fence refuses `ALTER` of a table that does not support replication when it commits the change,
    # and of a `ReplicatedMergeTree` table before the table commits it to Keeper, so both are checked.
    node.query(
        "CREATE TABLE fence_db.plain (x UInt8) ENGINE = MergeTree ORDER BY x",
        settings={"distributed_ddl_output_mode": "none"},
    )

    with failpoints_enabled(
        [
            "database_atomic_exchange_with_record",
            "database_atomic_exchange_always_fail_before_first_move",
        ]
    ):
        error = node.query_and_get_error(
            "EXCHANGE TABLES fence_db.a AND fence_db.b",
            settings={"distributed_ddl_output_mode": "none"},
        )
    assert "FAULT_INJECTED" in error
    # Without the temporary file, the restart leaves the repair to the replica recovery from Keeper,
    # which moves exchanged tables that do not support replication to the broken-tables database.
    assert "`fence_db_broken_tables`" in error

    # Keeper holds the exchange, while the local files and tables do not.
    assert has_column("fence_db", "a", "y") == "0"
    # Reads still work, and the record of the fenced exchange is kept for the next load.
    assert node.query("SELECT count() FROM fence_db.b").strip() == "1"
    assert ".a.sql.exchange" in metadata_files("fence_db")
    # DDL on the database is fenced until a restart, and the fence refuses a detach or drop
    # before either changes anything.
    error = node.query_and_get_error("DETACH DATABASE fence_db")
    assert "UNFINISHED" in error
    assert "`fence_db_broken_tables`" in error
    assert "UNFINISHED" in node.query_and_get_error("DROP DATABASE fence_db")
    # The fence refuses to drop or detach a table before the table is shut down,
    # so an insert into the `ReplicatedMergeTree` table still succeeds. A `Replicated` database
    # rejects a plain `DETACH TABLE` before the fence, so the detach is `PERMANENTLY`.
    for query in [
        "DROP TABLE fence_db.untouched",
        "DETACH TABLE fence_db.untouched PERMANENTLY",
    ]:
        assert "UNFINISHED" in node.query_and_get_error(
            query, settings={"distributed_ddl_output_mode": "none"}
        )
    # The fence refuses other DDL before it changes anything, in the files or in Keeper.
    for query in [
        "CREATE TABLE fence_db.c (x UInt8) ENGINE = MergeTree ORDER BY x",
        "ALTER TABLE fence_db.plain ADD COLUMN z UInt8",
        "ALTER TABLE fence_db.untouched ADD COLUMN z UInt8",
        "RENAME TABLE fence_db.plain TO fence_db.renamed",
        "EXCHANGE TABLES fence_db.plain AND fence_db.untouched",
    ]:
        assert "UNFINISHED" in node.query_and_get_error(
            query, settings={"distributed_ddl_output_mode": "none"}
        )
    assert (
        node.query(
            "SELECT groupArray(name) FROM"
            " (SELECT name FROM system.tables WHERE database = 'fence_db' ORDER BY name)"
        ).strip()
        == "['a','b','plain','untouched']"
    )
    assert has_column("fence_db", "plain", "z") == "0"
    assert has_column("fence_db", "untouched", "z") == "0"
    # A query that the database replicates through its log ends with a check of the digest, which reads every
    # metadata file, so the fence refuses it even when it does not change a metadata file, as a mutation of `plain`.
    # A mutation of a `ReplicatedMergeTree` table in a database with one shard is not replicated through the log,
    # so the fence lets it through.
    assert "UNFINISHED" in node.query_and_get_error(
        "ALTER TABLE fence_db.plain DELETE WHERE x = 0",
        settings={"distributed_ddl_output_mode": "none"},
    )
    node.query(
        "ALTER TABLE fence_db.untouched DELETE WHERE x = 0",
        settings={"distributed_ddl_output_mode": "none"},
    )
    # The fence refuses to restart a replica before the table is shut down,
    # and `SYSTEM RESTART REPLICAS` skips the tables of the fenced database.
    assert "UNFINISHED" in node.query_and_get_error(
        "SYSTEM RESTART REPLICA fence_db.untouched"
    )
    node.query("SYSTEM RESTART REPLICAS")
    node.query("INSERT INTO fence_db.untouched VALUES (1)")
    assert node.query("SELECT count() FROM fence_db.untouched").strip() == "1"

    node.restart_clickhouse()

    # The load removes the record, and the digest check on the DDL worker repairs the files,
    # which are behind Keeper, so the exchange takes effect.
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.columns"
        " WHERE database = 'fence_db' AND table = 'a' AND name = 'y'",
        "1",
        retry_count=60,
    )
    assert has_column("fence_db", "b", "y") == "0"
    # Keeper did not get the refused `ALTER`, so the repair does not add its column.
    assert has_column("fence_db", "untouched", "z") == "0"
    assert node.query("SELECT count() FROM fence_db.a").strip() == "1"
    files = metadata_files("fence_db")
    assert not any(name.startswith(".") for name in files)
    assert not any(name.endswith(".tmp_exchange") for name in files)
    # The fence is gone.
    node.query(
        "CREATE TABLE fence_db.c (x UInt8) ENGINE = MergeTree ORDER BY x",
        settings={"distributed_ddl_output_mode": "none"},
    )
    node.query("DROP DATABASE fence_db SYNC")
    # A repair that went through the broken-tables path would leave these databases
    # on the module-scoped cluster.
    node.query("DROP DATABASE IF EXISTS fence_db_broken_tables SYNC")
    node.query("DROP DATABASE IF EXISTS fence_db_broken_replicated_tables SYNC")


def test_fence_in_atomic_covers_the_two_tables(started_cluster):
    # An `Atomic` database has no Keeper commit, so a failed retry fails cleanly. The fence needs a move
    # that fails after the first move, and a roll forward from the record that fails too.
    node.query("CREATE DATABASE fence_atomic ENGINE = Atomic")
    node.query(
        "CREATE DATABASE fence_ordinary ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    # `ReplicatedMergeTree`, so that the fence is checked before the tables commit a change to Keeper.
    node.query(
        "CREATE TABLE fence_atomic.a (x UInt8)"
        " ENGINE = ReplicatedMergeTree('/clickhouse/tables/fence_atomic_a', 'r1') ORDER BY x"
    )
    node.query(
        "CREATE TABLE fence_atomic.b (x UInt8, y UInt8)"
        " ENGINE = ReplicatedMergeTree('/clickhouse/tables/fence_atomic_b', 'r1') ORDER BY x"
    )
    node.query("INSERT INTO fence_atomic.a VALUES (1)")
    # Not part of the exchange: the fence does not cover it.
    node.query(
        "CREATE TABLE fence_atomic.untouched (x UInt8) ENGINE = MergeTree ORDER BY x"
    )
    node.query("CREATE TABLE fence_ordinary.t (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO fence_ordinary.t VALUES (1)")
    # An `ALTER` through an alias in another database reaches `a` through the DDL guard of `a`.
    node.query(
        "CREATE TABLE fence_ordinary.a_alias ENGINE = Alias('fence_atomic', 'a')"
    )
    # A second replica outside the fenced database sends `ALTER_METADATA` entries to `a`.
    node.query(
        "CREATE TABLE fence_ordinary.a2 (x UInt8)"
        " ENGINE = ReplicatedMergeTree('/clickhouse/tables/fence_atomic_a', 'r2') ORDER BY x"
    )
    # A refreshable materialized view with an inner table, which a refresh exchanges with a fresh table.
    # `EMPTY` and the long period keep it from refreshing until `SYSTEM REFRESH VIEW`.
    node.query(
        "CREATE MATERIALIZED VIEW fence_atomic.mv REFRESH EVERY 1 YEAR"
        " (x UInt8) ENGINE = MergeTree ORDER BY x EMPTY AS SELECT 1 AS x"
    )
    # A table dropped before the fence, for `UNDROP` into the database during the fence.
    node.query("CREATE TABLE fence_atomic.u (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO fence_atomic.u VALUES (1)")
    node.query("DROP TABLE fence_atomic.u")

    with failpoints_enabled(
        [
            "database_atomic_exchange_with_record",
            "database_atomic_exchange_fail_after_first_move",
            "database_atomic_exchange_fail_to_finish",
        ]
    ):
        error = node.query_and_get_error(
            "EXCHANGE TABLES fence_atomic.a AND fence_atomic.b"
        )
    assert "FAULT_INJECTED" in error
    assert "is left, so the next load finishes the exchange" in error
    assert "stays in the distributed DDL queue of this server" in error

    # The DDL guard of each of the two tables refuses DDL on it before the query changes anything.
    for query in [
        "DROP TABLE fence_atomic.a",
        "DETACH TABLE fence_atomic.b",
        "ALTER TABLE fence_atomic.b ADD COLUMN z UInt8",
        "RENAME TABLE fence_atomic.untouched TO fence_atomic.a",
        "EXCHANGE TABLES fence_atomic.b AND fence_atomic.untouched",
        "SYSTEM RESTART REPLICA fence_atomic.a",
        "DROP DATABASE fence_atomic",
    ]:
        assert "UNFINISHED" in node.query_and_get_error(query), query
    # The tables were not shut down, so inserts still succeed.
    node.query("INSERT INTO fence_atomic.a VALUES (2)")
    assert node.query("SELECT count() FROM fence_atomic.a").strip() == "2"
    # Moving a table from `Ordinary` to one of the two names is refused before the table is detached
    # and its data moves, so the table stays in `Ordinary` with its data.
    assert "UNFINISHED" in node.query_and_get_error(
        "RENAME TABLE fence_ordinary.t TO fence_atomic.a"
    )
    assert node.query("SELECT count() FROM fence_ordinary.t").strip() == "1"
    # The metadata file of `a` is missing. Readers of the definition refuse the two tables, and `BACKUP`
    # refuses them instead of leaving them out of the backup.
    assert "UNFINISHED" in node.query_and_get_error("SHOW CREATE TABLE fence_atomic.a")
    assert "UNFINISHED" in node.query_and_get_error(
        "BACKUP TABLE fence_atomic.a TO Disk('backups', 'fence_a')"
    )
    # The `ReplicatedMergeTree` table refuses an `ALTER` through an alias before it commits the change to Keeper.
    assert "UNFINISHED" in node.query_and_get_error(
        "ALTER TABLE fence_ordinary.a_alias ADD COLUMN z UInt8"
    )
    keeper_columns = node.query(
        "SELECT value FROM system.zookeeper"
        " WHERE path = '/clickhouse/tables/fence_atomic_a' AND name = 'columns'"
    )
    assert "`x` UInt8" in keeper_columns
    assert "`z`" not in keeper_columns
    # The replica `a` refuses the `ALTER_METADATA` entry of `a2` before it changes its own metadata
    # nodes in Keeper, and retries it until the restart.
    node.query("ALTER TABLE fence_ordinary.a2 ADD COLUMN w UInt8")
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.replication_queue"
        " WHERE database = 'fence_atomic' AND table = 'a' AND type = 'ALTER_METADATA'"
        " AND last_exception LIKE '%UNFINISHED%'",
        "1",
        retry_count=60,
    )
    replica_columns = node.query(
        "SELECT value FROM system.zookeeper"
        " WHERE path = '/clickhouse/tables/fence_atomic_a/replicas/r1' AND name = 'columns'"
    )
    assert "`w`" not in replica_columns

    # DDL on the other tables of the database goes on.
    node.query("ALTER TABLE fence_atomic.untouched ADD COLUMN z UInt8")
    assert has_column("fence_atomic", "untouched", "z") == "1"
    node.query("CREATE TABLE fence_atomic.c (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query("RENAME TABLE fence_ordinary.t TO fence_atomic.t")
    assert node.query("SELECT count() FROM fence_atomic.t").strip() == "1"
    node.query("UNDROP TABLE fence_atomic.u")
    assert node.query("SELECT count() FROM fence_atomic.u").strip() == "1"
    node.query("DROP TABLE fence_atomic.c SYNC")
    node.query("BACKUP TABLE fence_atomic.untouched TO Disk('backups', 'fence_untouched')")

    # An `ON CLUSTER` query refused on one of the two tables stays in the distributed DDL queue and is retried
    # until the restart, so the `ON CLUSTER` query after it waits. The timeout 0 returns without waiting.
    node.query(
        "CREATE TABLE IF NOT EXISTS fence_atomic.a ON CLUSTER fence_cluster (x UInt8)"
        " ENGINE = MergeTree ORDER BY x",
        settings={"distributed_ddl_task_timeout": 0},
    )
    assert "TIMEOUT_EXCEEDED" in node.query_and_get_error(
        "CREATE TABLE fence_atomic.behind ON CLUSTER fence_cluster (x UInt8)"
        " ENGINE = MergeTree ORDER BY x",
        settings={"distributed_ddl_task_timeout": 3},
    )
    assert node.query("EXISTS TABLE fence_atomic.behind").strip() == "0"

    # A refresh whose exchange of the inner table is fenced. `DROP TABLE` of the view checks the names of
    # its inner tables before it shuts the view down, so the view keeps working.
    with failpoints_enabled(
        [
            "database_atomic_exchange_with_record",
            "database_atomic_exchange_fail_after_first_move",
            "database_atomic_exchange_fail_to_finish",
        ]
    ):
        node.query("SYSTEM REFRESH VIEW fence_atomic.mv")
        assert_eq_with_retry(
            node,
            "SELECT exception != '' FROM system.view_refreshes"
            " WHERE database = 'fence_atomic' AND view = 'mv'",
            "1",
            retry_count=60,
        )
    assert "UNFINISHED" in node.query_and_get_error("DROP TABLE fence_atomic.mv")
    assert node.query("SELECT count() FROM fence_atomic.mv").strip() == "0"

    node.restart_clickhouse()

    # The load finishes the exchange from its record, and the fence is gone. The table with the Keeper path
    # of `a` is now `b`.
    assert has_column("fence_atomic", "a", "y") == "1"
    assert has_column("fence_atomic", "b", "y") == "0"
    assert has_column("fence_atomic", "b", "z") == "0"
    assert node.query("SELECT count() FROM fence_atomic.b").strip() == "2"
    # The replica applies the entry after the restart and is not read-only.
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.columns"
        " WHERE database = 'fence_atomic' AND table = 'b' AND name = 'w'",
        "1",
        retry_count=60,
    )
    assert (
        node.query(
            "SELECT is_readonly FROM system.replicas"
            " WHERE database = 'fence_atomic' AND table = 'b'"
        ).strip()
        == "0"
    )
    files = metadata_files("fence_atomic")
    assert not any(name.startswith(".") for name in files)
    assert not any(name.endswith(".tmp_exchange") for name in files)
    # The distributed DDL queue applies the retried query, which now finds `a`, and the query after it.
    assert_eq_with_retry(
        node, "EXISTS TABLE fence_atomic.behind", "1", retry_count=60
    )
    node.query("ALTER TABLE fence_atomic.a ADD COLUMN z UInt8")
    # The load finished the exchange of the refresh, so the view shows the fresh table, and the drop of the view
    # also drops the temporary table that the fenced refresh could not drop.
    assert node.query("SELECT count() FROM fence_atomic.mv").strip() == "1"
    node.query("DROP TABLE fence_atomic.mv SYNC")
    assert (
        node.query(
            "SELECT count() FROM system.tables"
            " WHERE database = 'fence_atomic' AND name LIKE '%inner%'"
        ).strip()
        == "0"
    )
    node.query("DROP TABLE fence_ordinary.a_alias SYNC")
    node.query("DROP DATABASE fence_atomic SYNC")
    node.query("DROP DATABASE fence_ordinary SYNC")


def test_fence_refuses_replica_recovery(started_cluster):
    # The fenced exchange is in Keeper but not in the files, so the digests differ, and every
    # reinitialization of the DDL worker tries to recover the replica. Recovery would shut down the
    # exchanged dictionaries, which keep no data on disk, before the fence refuses to drop them.
    for name, value in [("1", "one"), ("2", "two")]:
        node.query(
            f"CREATE TABLE default.fence_dict_src{name} (k UInt64, v String)"
            " ENGINE = MergeTree ORDER BY k"
        )
        node.query(f"INSERT INTO default.fence_dict_src{name} VALUES (1, '{value}')")
    create_database = (
        "CREATE DATABASE fence_dict"
        " ENGINE = Replicated('/clickhouse/databases/fence_dict', 'shard1', '{}')"
    )
    node.query(create_database.format("replica1"))
    for name in ["1", "2"]:
        node.query(
            f"CREATE DICTIONARY fence_dict.d{name} (k UInt64, v String) PRIMARY KEY k"
            f" SOURCE(CLICKHOUSE(DB 'default' TABLE 'fence_dict_src{name}'))"
            " LAYOUT(FLAT()) LIFETIME(0)",
            settings={"distributed_ddl_output_mode": "none"},
        )
    dict_get = "SELECT dictGet('fence_dict.d1', 'v', toUInt64(1))"
    assert node.query(dict_get).strip() == "one"

    with failpoints_enabled(
        [
            "database_atomic_exchange_with_record",
            "database_atomic_exchange_always_fail_before_first_move",
        ]
    ):
        error = node.query_and_get_error(
            "EXCHANGE DICTIONARIES fence_dict.d1 AND fence_dict.d2",
            settings={"distributed_ddl_output_mode": "none"},
        )
    assert "FAULT_INJECTED" in error

    # A second replica, created after the exchange so that it copies the tables from Keeper, writes an
    # entry that the fenced replica refuses, which reinitializes its worker. The query does not wait for
    # the fenced replica, which applies the entry only after the restart.
    node2.query(create_database.format("replica2"))
    node2.query(
        "CREATE TABLE fence_dict.t (x UInt8) ENGINE = MergeTree ORDER BY x",
        settings={"distributed_ddl_task_timeout": 0},
    )
    node.wait_for_log_line(
        r"Error on initialization of fence_dict: .*An exchange of .* failed and could not be finished",
        timeout=60,
    )
    # The fence refuses the recovery before it changes anything.
    assert node.query(dict_get).strip() == "one"
    assert (
        node.query(
            "SELECT count() FROM system.databases"
            " WHERE startsWith(name, 'fence_dict_broken')"
        ).strip()
        == "0"
    )

    node.restart_clickhouse()

    # The recovery after the restart recreates the dictionaries from Keeper, which holds the exchange,
    # and applies the entry of the second replica.
    assert_eq_with_retry(node, dict_get, "two", retry_count=60)
    assert_eq_with_retry(node, "EXISTS TABLE fence_dict.t", "1", retry_count=60)
    node2.query("DROP DATABASE fence_dict SYNC")
    node.query("DROP DATABASE fence_dict SYNC")
    node.query("DROP DATABASE IF EXISTS fence_dict_broken_tables SYNC")
    node.query("DROP DATABASE IF EXISTS fence_dict_broken_replicated_tables SYNC")
    for name in ["1", "2"]:
        node.query(f"DROP TABLE default.fence_dict_src{name} SYNC")
