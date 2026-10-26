import os
import time
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path

# The swap of metadata files of `CREATE OR REPLACE`, `REPLACE TABLE` and refreshes with a record, on a real NFS mount,
# where `RENAME_EXCHANGE` fails with `EINVAL` without a failpoint, including a crash between the moves of a swap.
# The NFS server `nfs1` is NFS-Ganesha, from `docker_compose_nfs.yml`. The stateless tests `05452` to `05457` cover the rest
# of the swap with failpoints, and use `clickhouse-local` for the fence until a restart and for the load after a crash.

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/exchange_emulation.xml",
        "configs/nfs_disk.xml",
    ],
    user_configs=["configs/stop_refreshes_on_startup.xml"],
    stay_alive=True,
    # The swap is emulated only for metadata on a `local` disk. The `db disk` CI jobs would put the metadata of every
    # database on object storage by default.
    with_remote_database_disk=False,
)

cluster.base_cmd.extend(
    ["--file", os.path.join(get_docker_compose_path(), "docker_compose_nfs.yml")]
)

EXCHANGE_EMULATION_CONFIG = "/etc/clickhouse-server/config.d/exchange_emulation.xml"

with open(
    os.path.join(os.path.dirname(__file__), "configs", "exchange_emulation.xml")
) as f:
    # Turns the setting on.
    ORIGINAL_EXCHANGE_EMULATION_CONFIG = f.read()

EXCHANGE_EMULATION_QUERY = (
    "SELECT value FROM system.server_settings"
    " WHERE name = 'database_atomic_allow_exchange_emulation'"
)

# The directory of the disk `nfs` in `configs/nfs_disk.xml`, which `nfs_mounted` mounts the export of `nfs1` over.
NFS_DISK_PATH = "/var/lib/clickhouse/disks/nfs"

# Only a query whose id starts with this prefix pauses at `database_atomic_exchange_pause_after_second_move`.
PAUSE_AFTER_SECOND_MOVE_QUERY_ID = "atomic_exchange_pause_after_second_move_nfs"


def wait_for_healthy(service_name, timeout=60):
    container = cluster.get_docker_handle(cluster.get_instance_docker_id(service_name))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        container.reload()
        if container.attrs["State"]["Health"]["Status"] == "healthy":
            return
        time.sleep(0.5)
    raise Exception(f"{service_name} did not become healthy in {timeout} s")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        wait_for_healthy("nfs1")
        yield cluster
    finally:
        cluster.shutdown()


def start_with_exchange_emulation():
    # The config of a test that turned the setting off is restored without a restart, so the setting can still be off.
    node.replace_config(EXCHANGE_EMULATION_CONFIG, ORIGINAL_EXCHANGE_EMULATION_CONFIG)
    if node.query(EXCHANGE_EMULATION_QUERY) != "1\n":
        node.restart_clickhouse()
    assert node.query(EXCHANGE_EMULATION_QUERY) == "1\n"


def restart_without_exchange_emulation():
    node.replace_config(
        EXCHANGE_EMULATION_CONFIG,
        "<clickhouse><database_atomic_allow_exchange_emulation>0"
        "</database_atomic_allow_exchange_emulation></clickhouse>",
    )
    node.restart_clickhouse()
    assert node.query(EXCHANGE_EMULATION_QUERY) == "0\n"


def metadata_dir(database, disk_path="/var/lib/clickhouse"):
    rel = node.query(
        f"SELECT metadata_path FROM system.databases WHERE name = '{database}'"
    ).strip()
    assert rel.startswith("store/"), rel
    return f"{disk_path}/{rel}"


def list_files(directory):
    return node.exec_in_container(["ls", "-A", directory]).split()


def exchange_files(files):
    # The record of a swap, its temporary record, a write of its finish, and its temporary file.
    suffixes = (".exchange", ".exchange.tmp", ".exchange_write", ".tmp_exchange")
    return [name for name in files if name.endswith(suffixes)]


@contextmanager
def nfs_mounted():
    # Mounted in the container of the instance, which has `SYS_ADMIN`. Without `mount.nfs` in the image, the kernel
    # needs the server as an IP in `addr`. `soft` keeps a server that went away from hanging a query forever.
    ip = cluster.get_instance_ip("nfs1")
    node.exec_in_container(["mkdir", "-p", NFS_DISK_PATH], user="root")
    node.exec_in_container(
        [
            "mount",
            "-t",
            "nfs4",
            "-o",
            f"vers=4.2,soft,addr={ip}",
            f"{ip}:/",
            NFS_DISK_PATH,
        ],
        user="root",
    )
    try:
        # The server may run as the user of the test runner, which owns nothing on the export.
        node.exec_in_container(["chmod", "0777", NFS_DISK_PATH], user="root")
        assert (
            node.exec_in_container(["stat", "-f", "-c", "%T", NFS_DISK_PATH]).strip()
            == "nfs"
        )
        yield
    finally:
        # Lazy, so that a server that went away cannot hang the teardown.
        node.exec_in_container(
            ["umount", "-l", NFS_DISK_PATH], user="root", nothrow=True
        )


def test_swap_on_nfs(started_cluster):
    start_with_exchange_emulation()
    with nfs_mounted():
        try:
            node.query("CREATE DATABASE nfs_db ENGINE = Atomic SETTINGS disk = 'nfs'")
            node.query("CREATE TABLE nfs_db.t (x UInt8) ENGINE = MergeTree ORDER BY x")
            node.query("INSERT INTO nfs_db.t VALUES (1)")
            # Computed now: a swap paused below holds the database mutex, which `system.databases` can wait for.
            nfs_db_dir = metadata_dir("nfs_db", NFS_DISK_PATH)
            assert "t.sql" in list_files(nfs_db_dir)
            nfs_db_uuid = node.query(
                "SELECT uuid FROM system.databases WHERE name = 'nfs_db'"
            ).strip()

            # No failpoint: on NFS, `renameat2(RENAME_EXCHANGE)` itself fails with `EINVAL`, so each swap uses a record.
            node.query(
                "CREATE OR REPLACE TABLE nfs_db.t (x UInt8, y UInt8)"
                " ENGINE = MergeTree ORDER BY x AS SELECT 2, 3"
            )
            assert node.query("SELECT * FROM nfs_db.t") == "2\t3\n"
            node.query(
                "REPLACE TABLE nfs_db.t (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 4"
            )
            assert node.query("SELECT * FROM nfs_db.t") == "4\n"

            # A refresh of a view with an inner table that is not `APPEND` swaps the `.tmp` table it fills with the inner
            # table. The view starts stopped, because of `stop_refreshable_materialized_views_on_startup`.
            node.query(
                "CREATE TABLE nfs_db.src (x UInt8) ENGINE = MergeTree ORDER BY x"
            )
            node.query("INSERT INTO nfs_db.src VALUES (7)")
            node.query(
                "CREATE MATERIALIZED VIEW nfs_db.mv REFRESH EVERY 1 YEAR"
                " (x UInt8) ENGINE = MergeTree ORDER BY x EMPTY AS SELECT x FROM nfs_db.src"
            )
            node.query("SYSTEM START VIEW nfs_db.mv")
            node.query("SYSTEM REFRESH VIEW nfs_db.mv")
            node.query("SYSTEM WAIT VIEW nfs_db.mv")
            assert node.query("SELECT x FROM nfs_db.mv") == "7\n"

            # The three swaps above used a record, and left none of their files.
            assert (
                node.count_in_log(
                    f"Exchanging store/.*{nfs_db_uuid}.* with an exchange record"
                ).strip()
                == "3"
            )
            assert exchange_files(list_files(nfs_db_dir)) == []

            # A crash between the moves of a swap: the server is killed while `c.sql` is missing, and the load at the restart
            # undoes the swap from the record.
            node.query("CREATE TABLE nfs_db.c (x UInt8) ENGINE = MergeTree ORDER BY x")
            node.query("INSERT INTO nfs_db.c VALUES (1)")
            node.query(
                "SYSTEM ENABLE FAILPOINT database_atomic_exchange_pause_after_second_move"
            )
            request = node.get_query_request(
                "CREATE OR REPLACE TABLE nfs_db.c (x UInt8, y UInt8)"
                " ENGINE = MergeTree ORDER BY x AS SELECT 2, 3",
                query_id=PAUSE_AFTER_SECOND_MOVE_QUERY_ID,
            )
            node.query(
                "SYSTEM WAIT FAILPOINT database_atomic_exchange_pause_after_second_move PAUSE"
            )
            files = list_files(nfs_db_dir)
            assert "c.sql" not in files, files
            assert (
                len([name for name in files if name.endswith(".tmp_exchange")]) == 1
            ), files
            node.stop_clickhouse(kill=True)
            # The client lost its connection to the killed server.
            request.get_answer_and_error()
            node.start_clickhouse()

            # Only for `nfs_db`, which is the database of this swap.
            assert (
                node.count_in_log(
                    f"Undid the interrupted exchange of store/.*{nfs_db_uuid}"
                ).strip()
                == "1"
            )
            assert node.query("SELECT * FROM nfs_db.c") == "1\n"
            # The query did not finish, so the replacement table is left under the temporary name.
            tmp_tables = node.query(
                "SELECT name FROM system.tables"
                " WHERE database = 'nfs_db' AND startsWith(name, '_tmp_replace_')"
            ).split()
            assert len(tmp_tables) == 1, tmp_tables
            assert node.query(f"SELECT * FROM nfs_db.`{tmp_tables[0]}`") == "2\t3\n"
            assert exchange_files(list_files(nfs_db_dir)) == []
            node.query(f"DROP TABLE nfs_db.`{tmp_tables[0]}` SYNC")

            # With the setting off, `CREATE OR REPLACE` fails on NFS, mentions the setting, and leaves nothing behind.
            restart_without_exchange_emulation()
            node.query("CREATE TABLE nfs_db.v (x UInt8) ENGINE = MergeTree ORDER BY x")
            error = node.query_and_get_error(
                "CREATE OR REPLACE TABLE nfs_db.v (x UInt8, y UInt8)"
                " ENGINE = MergeTree ORDER BY x AS SELECT 2, 3"
            )
            assert "UNSUPPORTED_METHOD" in error
            assert "database_atomic_allow_exchange_emulation" in error
            assert (
                node.query(
                    "SELECT name FROM system.columns"
                    " WHERE database = 'nfs_db' AND table = 'v'"
                )
                == "x\n"
            )
            assert exchange_files(list_files(nfs_db_dir)) == []
            assert (
                node.query(
                    "SELECT count() FROM system.tables"
                    " WHERE database = 'nfs_db' AND startsWith(name, '_tmp_replace_')"
                )
                == "0\n"
            )
        finally:
            node.replace_config(
                EXCHANGE_EMULATION_CONFIG, ORIGINAL_EXCHANGE_EMULATION_CONFIG
            )
            # When an assertion fails while the swap is paused, the failpoint is still enabled.
            node.query_and_get_answer_with_error(
                "SYSTEM DISABLE FAILPOINT database_atomic_exchange_pause_after_second_move"
            )
            node.query_and_get_answer_with_error("DROP DATABASE IF EXISTS nfs_db SYNC")
