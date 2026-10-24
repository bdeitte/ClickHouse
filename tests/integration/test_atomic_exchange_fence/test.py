import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# When an exchange of metadata files with a record fails after the Keeper commit and cannot be rolled
# forward from the record, the database fences its DDL until a restart. This needs a restart to recover,
# so a stateless test cannot cover it. The failpoint `database_atomic_exchange_with_record` forces the
# record path, and other failpoints fail the moves of the exchange and the handling of the failure.

cluster = ClickHouseCluster(__file__)

# The restart attaches the `ReplicatedMergeTree` tables outside of a DDL query, so their default
# Keeper path takes `{shard}` and `{replica}` from these macros, not from the `Replicated` database.
node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "shard1", "replica": "replica1"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


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

    node.query("SYSTEM ENABLE FAILPOINT database_atomic_exchange_with_record")
    node.query(
        "SYSTEM ENABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move"
    )
    error = node.query_and_get_error(
        "EXCHANGE TABLES fence_db.a AND fence_db.b",
        settings={"distributed_ddl_output_mode": "none"},
    )
    assert "FAULT_INJECTED" in error
    # Without the temporary file, the restart leaves the repair to the replica recovery from Keeper,
    # which moves exchanged tables that do not support replication to the broken-tables database.
    assert "`fence_db_broken_tables`" in error
    node.query(
        "SYSTEM DISABLE FAILPOINT database_atomic_exchange_always_fail_before_first_move"
    )
    node.query("SYSTEM DISABLE FAILPOINT database_atomic_exchange_with_record")

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
    # A mutation does not change the metadata file, so the fence lets it through.
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


def test_fence_refuses_move_from_ordinary(started_cluster):
    # An `Atomic` database has no Keeper commit, so a failed retry fails cleanly. The fence needs a move
    # that fails after the first move, and a roll forward from the record that fails too.
    node.query("CREATE DATABASE fence_atomic ENGINE = Atomic")
    node.query(
        "CREATE DATABASE fence_ordinary ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query("CREATE TABLE fence_atomic.a (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query(
        "CREATE TABLE fence_atomic.b (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x"
    )
    node.query("CREATE TABLE fence_ordinary.t (x UInt8) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO fence_ordinary.t VALUES (1)")
    # An `ALTER` through an alias in another database reaches the `ReplicatedMergeTree` table without
    # the check of `InterpreterAlterQuery`, which checks only the database of the alias.
    node.query(
        "CREATE TABLE fence_atomic.r (x UInt8)"
        " ENGINE = ReplicatedMergeTree('/clickhouse/tables/fence_atomic_r', 'r1') ORDER BY x"
    )
    node.query(
        "CREATE TABLE fence_ordinary.r_alias ENGINE = Alias('fence_atomic', 'r')"
    )

    failpoints = [
        "database_atomic_exchange_with_record",
        "database_atomic_exchange_fail_after_first_move",
        "database_atomic_exchange_fail_to_finish",
    ]
    for failpoint in failpoints:
        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    error = node.query_and_get_error(
        "EXCHANGE TABLES fence_atomic.a AND fence_atomic.b"
    )
    for failpoint in failpoints:
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
    assert "FAULT_INJECTED" in error
    assert "is left, so the next load finishes the exchange" in error

    # Moving a table from `Ordinary` into the fenced database is refused before the table is detached
    # and its data moves, so the table stays in `Ordinary` with its data.
    assert "UNFINISHED" in node.query_and_get_error(
        "RENAME TABLE fence_ordinary.t TO fence_atomic.t"
    )
    assert node.query("SELECT count() FROM fence_ordinary.t").strip() == "1"
    # The `ReplicatedMergeTree` table refuses the `ALTER` before it commits the change to Keeper.
    assert "UNFINISHED" in node.query_and_get_error(
        "ALTER TABLE fence_ordinary.r_alias ADD COLUMN z UInt8"
    )
    keeper_columns = node.query(
        "SELECT value FROM system.zookeeper"
        " WHERE path = '/clickhouse/tables/fence_atomic_r' AND name = 'columns'"
    )
    assert "`x` UInt8" in keeper_columns
    assert "`z`" not in keeper_columns
    # A `CREATE` is refused before the storage is built, which creates the replica of a
    # `ReplicatedMergeTree` table in Keeper.
    create_c = (
        "CREATE TABLE fence_atomic.c (x UInt8)"
        " ENGINE = ReplicatedMergeTree('/clickhouse/tables/fence_atomic_c', 'r1') ORDER BY x"
    )
    assert "UNFINISHED" in node.query_and_get_error(create_c)
    assert (
        node.query(
            "SELECT count() FROM system.zookeeper"
            " WHERE path = '/clickhouse/tables' AND name = 'fence_atomic_c'"
        ).strip()
        == "0"
    )

    node.restart_clickhouse()

    # The load finishes the exchange from its record, and the fence is gone.
    assert has_column("fence_atomic", "a", "y") == "1"
    assert has_column("fence_atomic", "b", "y") == "0"
    assert has_column("fence_atomic", "r", "z") == "0"
    files = metadata_files("fence_atomic")
    assert not any(name.startswith(".") for name in files)
    assert not any(name.endswith(".tmp_exchange") for name in files)
    node.query("RENAME TABLE fence_ordinary.t TO fence_atomic.t")
    assert node.query("SELECT count() FROM fence_atomic.t").strip() == "1"
    node.query(create_c)
    node.query("DROP TABLE fence_ordinary.r_alias SYNC")
    node.query("DROP DATABASE fence_atomic SYNC")
    node.query("DROP DATABASE fence_ordinary SYNC")
