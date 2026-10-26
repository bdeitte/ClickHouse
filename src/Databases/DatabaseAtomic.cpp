#include <filesystem>
#include <thread>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/MetadataExchange.h>
#include <Disks/DiskLocal.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/Context.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageTimeSeries.h>
#include <base/isSharedPtrUnique.h>
#include <Common/PoolId.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/atomicRename.h>
#include <Common/logger_useful.h>
#include <Common/AsyncLoader.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Interpreters/ProcessList.h>


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool check_referential_table_dependencies;
    extern const SettingsBool check_table_dependencies;
    extern const SettingsBool fsync_metadata;
}

namespace ServerSetting
{
    extern const ServerSettingsBool database_atomic_allow_exchange_emulation;
}

namespace FailPoints
{
    extern const char rename_database_after_dependency_check[];
    extern const char database_atomic_exchange_not_supported[];
    extern const char database_atomic_exchange_fail_before_first_move[];
    extern const char database_atomic_exchange_fail_after_first_move[];
    extern const char database_atomic_exchange_fail_after_second_move[];
    extern const char database_atomic_exchange_fail_after_third_move[];
    extern const char database_atomic_exchange_always_fail_before_first_move[];
    extern const char database_atomic_exchange_fail_to_finish[];
    extern const char database_atomic_exchange_pause_after_second_move[];
    extern const char database_atomic_pause_before_reading_create_query[];
}

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int DATABASE_NOT_EMPTY;
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_ALREADY_EXISTS;
    extern const int INCORRECT_QUERY;
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
    extern const int UNFINISHED;
    extern const int UNFINISHED_METADATA_EXCHANGE;
    extern const int UNSUPPORTED_METHOD;
}


namespace DatabaseMetadataDiskSetting
{
extern const DatabaseMetadataDiskSettingsString disk;
}

namespace
{

/// Only a query whose id starts with the prefix pauses at `database_atomic_pause_before_reading_create_query`,
/// so that the readers of other queries and of background threads do not take the pause meant for a test.
constexpr std::string_view pause_before_reading_create_query_query_id_prefix = "atomic_pause_before_reading_create_query_";

/// Only a query whose id starts with the prefix pauses at `database_atomic_exchange_pause_after_second_move`,
/// so that the swaps of other queries and of refreshes do not take the pause meant for a test.
constexpr std::string_view pause_after_second_move_query_id_prefix = "atomic_exchange_pause_after_second_move_";

}

class AtomicDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit AtomicDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base) noexcept
        : DatabaseTablesSnapshotIterator(std::move(base))
    {
    }
    UUID uuid() const override { return table()->getStorageID().uuid; }
};

DatabaseAtomic::DatabaseAtomic(
    String name_,
    String metadata_path_,
    UUID uuid,
    const String & logger_name,
    ContextPtr context_,
    DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseOrdinary(
        name_,
        metadata_path_,
        DatabaseCatalog::getStoreDirPath() / "",
        logger_name,
        context_,
        database_metadata_disk_settings_)
    , path_to_table_symlinks(DatabaseCatalog::getDataDirPath(name_) / "")
    , path_to_metadata_symlink(DatabaseCatalog::getMetadataDirPath(name_))
    , db_uuid(uuid)
{
    chassert(db_uuid != UUIDHelpers::Nil);
}

DatabaseAtomic::DatabaseAtomic(
    String name_, String metadata_path_, UUID uuid, ContextPtr context_, DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseAtomic(name_, std::move(metadata_path_), uuid, "DatabaseAtomic (" + name_ + ")", context_, database_metadata_disk_settings_)
{
}

void DatabaseAtomic::createDirectories()
{
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
}

void DatabaseAtomic::createDirectoriesUnlocked()
{
    auto db_disk = getDisk();

    DatabaseOnDisk::createDirectoriesUnlocked();
    db_disk->createDirectories(DatabaseCatalog::getMetadataDirPath());
    if (db_disk->isSymlinkSupported())
        db_disk->createDirectories(path_to_table_symlinks);
    tryCreateMetadataSymlink();
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} not found in database {}", table_name, database_name);
    chassert(it->second != data_path && !it->second.empty());
    return it->second;
}

String DatabaseAtomic::getTableDataPath(const ASTCreateQuery & query) const
{
    auto tmp = data_path + DatabaseCatalog::getPathForUUID(query.uuid);
    chassert(tmp != data_path && !tmp.empty());
    return tmp;
}

void DatabaseAtomic::drop(ContextPtr)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::drop");
    waitDatabaseStarted();
    {
        std::lock_guard lock(mutex);
        chassert(tables.empty());
    }

    auto db_disk = getDisk();
    try
    {
        if (db_disk->isSymlinkSupported() && !db_disk->isReadOnly())
        {
            db_disk->removeFileIfExists(path_to_metadata_symlink);
            db_disk->removeRecursive(path_to_table_symlinks);
        }
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
    if (!db_disk->isReadOnly())
        db_disk->removeRecursive(getMetadataPath());
}

void DatabaseAtomic::attachTable(ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::attachTable");
    chassert(relative_table_path != data_path && !relative_table_path.empty());
    DetachedTables not_in_use;
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
    not_in_use = cleanupDetachedTables();
    auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    DatabaseOrdinary::attachTableUnlocked(name, table);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(ContextPtr /* context */, const String & name)
{
    ensurePopulated();

    // it is important to call the destructors of not_in_use without
    // locked mutex to avoid potential deadlock.
    DetachedTables not_in_use;
    StoragePtr detached_table;
    {
        std::lock_guard lock(mutex);
        assertTableHasNoUnfinishedExchangeUnlocked(name);
        detached_table = DatabaseOrdinary::detachTableUnlocked(name);
        table_name_to_path.erase(name);
        detached_tables.emplace(detached_table->getStorageID().uuid, detached_table);
        not_in_use = cleanupDetachedTables();
    }

    if (!not_in_use.empty())
    {
        not_in_use.clear();
        LOG_DEBUG(log, "Finished removing not used detached tables");
    }

    return detached_table;
}

void DatabaseAtomic::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::dropTable");
    waitDatabaseStarted();
    auto table = tryGetTable(table_name, local_context);
    /// Remove the inner table (if any) to avoid deadlock
    /// (due to attempt to execute DROP from the worker thread)
    if (table)
        table->dropInnerTableIfAny(sync, local_context);
    else
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(getDatabaseName()), backQuote(table_name));

    dropTableImpl(local_context, table_name, sync);
}

void DatabaseAtomic::dropTableImpl(ContextPtr local_context, const String & table_name, bool sync)
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop;
    StoragePtr table;
    auto db_disk = getDisk();
    {
        std::lock_guard lock(mutex);
        assertTableHasNoUnfinishedExchangeUnlocked(table_name);
        table = getTableUnlocked(table_name);
        table_metadata_path_drop = DatabaseCatalog::instance().getPathForDroppedMetadata(table->getStorageID());

        db_disk->createDirectories(fs::path(table_metadata_path_drop).parent_path());

        auto txn = local_context->getZooKeeperMetadataTransaction();
        if (txn && !local_context->isInternalSubquery())
            txn->commit();      /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following rename
        /// We apply changes in ZooKeeper before applying changes in local metadata file
        /// to reduce probability of failures between these operations
        /// (it's more likely to lost connection, than to fail before applying local changes).
        /// TODO better detection and recovery

        db_disk->replaceFile(table_metadata_path, table_metadata_path_drop); /// Mark table as dropped
        DatabaseOrdinary::detachTableUnlocked(table_name);  /// Should never throw
        table_name_to_path.erase(table_name);
        snapshot_detached_tables.erase(table_name);
    }

    if (table->storesDataOnDisk())
        tryRemoveSymlink(table_name);

    /// Notify DatabaseCatalog that table was dropped. It will remove table data in background.
    /// Cleanup is performed outside of database to allow easily DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, db_disk, table_metadata_path_drop, sync);
}

void DatabaseAtomic::renameTable(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange, bool dictionary, bool allow_exchange_emulation)
    TSA_NO_THREAD_SAFETY_ANALYSIS   /// TSA does not support conditional locking
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::renameTable");
    if (typeid(*this) != typeid(to_database))
    {
        if (typeid_cast<DatabaseOrdinary *>(&to_database))
        {
            /// Allow moving tables between Atomic and Ordinary (with table lock)
            DatabaseOnDisk::renameTable(
                local_context, table_name, to_database, to_table_name, exchange, dictionary, allow_exchange_emulation);
            return;
        }

        if (!allowMoveTableToOtherDatabaseEngine(to_database))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases of different engines is not supported");
    }

    std::string message;
    if (exchange && !supportsAtomicRename(&message))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RENAME EXCHANGE is not supported because exchanging files is not supported by the OS ({})", message);

    createDirectories();
    waitDatabaseStarted();

    auto & other_db = dynamic_cast<DatabaseAtomic &>(to_database);
    bool inside_database = this == &other_db;

    /// Where the filesystem cannot exchange two files atomically, such as NFS, an exchange of two metadata files can be emulated
    /// with three moves and a record, see `exchangeMetadataFilesWithRecord`. Only inside one database that is exactly `Atomic`,
    /// so not `Replicated`, on a local disk, and only with the server setting on. Everything else exchanges as without the emulation.
    const bool emulation_applies = exchange && inside_database && typeid(*this) == typeid(DatabaseAtomic) && canEmulateExchange(getDisk());
    const bool emulation_enabled = local_context->getServerSettings()[ServerSetting::database_atomic_allow_exchange_emulation];

    ensurePopulated();
    if (!inside_database)
        other_db.ensurePopulated();

    if (!inside_database)
    {
        other_db.createDirectories();
        other_db.waitDatabaseStarted();
    }

    String old_metadata_path = getObjectMetadataPath(table_name);
    String new_metadata_path = to_database.getObjectMetadataPath(to_table_name);

    auto detach = [](DatabaseAtomic & db, const String & table_name_, bool has_symlink) TSA_REQUIRES(db.mutex)
    {
        auto it = db.table_name_to_path.find(table_name_);
        String table_data_path_saved;
        /// Path can be not set for DDL dictionaries, but it does not matter for StorageDictionary.
        if (it != db.table_name_to_path.end())
            table_data_path_saved = it->second;
        chassert(!table_data_path_saved.empty());
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        /// This path bypasses detachTableUnlocked, so clear stale async-load names
        /// here too, otherwise getAllTableNames keeps suggesting the old name (#91777).
        db.eraseAsyncLoadState(table_name_);
        if (has_symlink)
            db.tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [](DatabaseAtomic & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_) TSA_REQUIRES(db.mutex)
    {
        db.tables.emplace(table_name_, table_);
        if (table_data_path_.empty())
            return;
        db.table_name_to_path.emplace(table_name_, table_data_path_);
        if (table_->storesDataOnDisk())
            db.tryCreateSymlink(table_);
    };

    auto assert_can_move_mat_view = [inside_database](const StoragePtr & table_)
    {
        if (inside_database)
            return;
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table_.get()))
            if (mv->hasInnerTable())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move MaterializedView with inner table to other database");
        if (const auto * ts = dynamic_cast<const StorageTimeSeries *>(table_.get()))
            if (ts->hasInnerTables())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move TimeSeries table with inner tables to other database");
    };

    String table_data_path;
    String other_table_data_path;

    if (inside_database && table_name == to_table_name)
        return;

    std::unique_lock<std::mutex> db_lock;
    std::unique_lock<std::mutex> other_db_lock;
    if (inside_database)
        db_lock = std::unique_lock{mutex};
    else if (this < &other_db)
    {
        db_lock = std::unique_lock{mutex};
        other_db_lock = std::unique_lock{other_db.mutex};
    }
    else
    {
        other_db_lock = std::unique_lock{other_db.mutex};
        db_lock = std::unique_lock{mutex};
    }

    assertTableHasNoUnfinishedExchangeUnlocked(table_name);
    other_db.assertTableHasNoUnfinishedExchangeUnlocked(to_table_name);

    if (!exchange)
        other_db.checkMetadataFilenameAvailabilityUnlocked(to_table_name);

    StoragePtr table = getTableUnlocked(table_name);

    if (dictionary && !table->isDictionary())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");

    StorageID old_table_id = table->getStorageID();
    StorageID new_table_id = {other_db.database_name, to_table_name, old_table_id.uuid};
    table->checkTableCanBeRenamed({new_table_id});
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    StorageID other_table_new_id = StorageID::createEmpty();
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name);
        if (dictionary && !other_table->isDictionary())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");
        other_table_new_id = {database_name, table_name, other_table->getStorageID().uuid};
        other_table->checkTableCanBeRenamed(other_table_new_id);
        assert_can_move_mat_view(other_table);
    }

    /// Check the destination `max_tables` quota only after the source table has been resolved
    /// and validated, so that a full destination does not mask `UNKNOWN_TABLE` and other
    /// source-side errors. An exchange does not change the number of tables.
    if (!inside_database && !exchange)
        other_db.checkTablesLimitUnlocked();

    /// Table renaming actually begins here
    auto txn = local_context->getZooKeeperMetadataTransaction();
    if (txn && !local_context->isInternalSubquery())
        txn->commit();     /// Commit point (a sort of) for Replicated database

    auto db_disk = getDisk();

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery
    if (exchange)
    {
        /// Makes the filesystem report that it cannot exchange two files atomically, as NFS does. It applies in every branch below,
        /// so with the server setting `database_atomic_allow_exchange_emulation` on or off.
        bool exchange_not_supported = false;
        fiu_do_on(FailPoints::database_atomic_exchange_not_supported, { exchange_not_supported = true; });

        if (emulation_applies && emulation_enabled)
        {
            /// It returns `false` when the filesystem does not support the exchange, and throws on every other error.
            if (exchange_not_supported || !db_disk->renameExchangeIfSupported(old_metadata_path, new_metadata_path))
            {
                if (!allow_exchange_emulation)
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_METHOD,
                        "Cannot exchange {} and {}: the filesystem, such as NFS, cannot exchange two files atomically. "
                        "`EXCHANGE TABLES` and `EXCHANGE DICTIONARIES` are not emulated on such a filesystem. `CREATE OR REPLACE`, "
                        "`REPLACE TABLE` and refreshes of refreshable materialized views work on it with the server setting "
                        "`database_atomic_allow_exchange_emulation`",
                        old_metadata_path,
                        new_metadata_path);

                /// What each of the two files holds after the exchange.
                MetadataExchangeRecord record;
                record.old_path = old_metadata_path;
                record.new_path = new_metadata_path;
                record.old_path_content = readMetadataFile(db_disk, new_metadata_path);
                record.new_path_content = readMetadataFile(db_disk, old_metadata_path);
                const bool fsync = local_context->getSettingsRef()[Setting::fsync_metadata];
                const String record_path = writeMetadataExchangeRecord(db_disk, record, fsync);
                try
                {
                    exchangeMetadataFilesWithRecord(table_name, to_table_name, db_disk, record, record_path, fsync);
                }
                catch (...)
                {
                    /// Neither name had a fence when the exchange began, see the checks above, so a fence now is from this exchange.
                    if (unfinished_exchanges.contains(table_name))
                        warnAboutUnfinishedExchange(db_lock, table_name, to_table_name);
                    throw;
                }
            }
        }
        else if (emulation_applies && allow_exchange_emulation)
        {
            /// The server setting would let this exchange work on such a filesystem, so the error says so.
            if (exchange_not_supported || !db_disk->renameExchangeIfSupported(old_metadata_path, new_metadata_path))
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Cannot exchange {} and {}: the filesystem, such as NFS, cannot exchange two files atomically. `CREATE OR REPLACE`, "
                    "`REPLACE TABLE` and refreshes of refreshable materialized views work on it when the server setting "
                    "`database_atomic_allow_exchange_emulation` is enabled",
                    old_metadata_path,
                    new_metadata_path);
        }
        else if (exchange_not_supported)
        {
            /// As `renameExchange` fails on such a filesystem.
            throw Exception(
                ErrorCodes::UNSUPPORTED_METHOD,
                "Cannot exchange {} and {}: the filesystem does not support an atomic exchange of files",
                old_metadata_path,
                new_metadata_path);
        }
        else
        {
            db_disk->renameExchange(old_metadata_path, new_metadata_path);
        }
    }
    else
    {
        db_disk->moveFile(old_metadata_path, new_metadata_path);
    }

    /// After metadata was successfully moved, the following methods should not throw (if they do, it's a logical error)
    table_data_path = detach(*this, table_name, table->storesDataOnDisk());
    if (exchange)
        other_table_data_path = detach(other_db, to_table_name, other_table->storesDataOnDisk());

    table->renameInMemory(new_table_id);
    if (exchange)
        other_table->renameInMemory(other_table_new_id);

    if (!inside_database)
    {
        DatabaseCatalog::instance().updateUUIDMapping(old_table_id.uuid, other_db.shared_from_this(), table);
        if (exchange)
            DatabaseCatalog::instance().updateUUIDMapping(other_table->getStorageID().uuid, shared_from_this(), other_table);
    }

    attach(other_db, to_table_name, table_data_path, table);
    if (exchange)
        attach(*this, table_name, other_table_data_path, other_table);
}

void DatabaseAtomic::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                                       ContextPtr query_context)
{
    auto db_disk = getDisk();

    createDirectories();
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(query);
    try
    {
        std::lock_guard lock{mutex};
        if (query.getDatabase() != database_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                            database_name, query.getDatabase());
        assertTableHasNoUnfinishedExchangeUnlocked(query.getTable());
        /// Do some checks before renaming file from .tmp to .sql
        not_in_use = cleanupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        chassert(DatabaseCatalog::instance().hasUUIDMapping(query.uuid));

        auto txn = query_context->getZooKeeperMetadataTransaction();
        if (txn && !query_context->isInternalSubquery())
            txn->commit();     /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following renameNoReplace(...)
        /// TODO better detection and recovery

        /// It throws if `table_metadata_path` already exists (it's possible if table was detached)
        db_disk->moveFile(table_metadata_tmp_path, table_metadata_path); /// Commit point (a sort of)
        attachTableUnlocked(query.getTable(), table);   /// Should never throw
        table_name_to_path.emplace(query.getTable(), table_data_path);
    }
    catch (...)
    {
        db_disk->removeFileIfExists(table_metadata_tmp_path);
        throw;
    }
    if (table->storesDataOnDisk())
        tryCreateSymlink(table);
}

void DatabaseAtomic::commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path,
                                      const String & /*statement*/, ContextPtr query_context)
{
    auto db_disk = getDisk();

    bool check_file_exists = true;
    SCOPE_EXIT({
        if (check_file_exists)
            db_disk->removeFileIfExists(table_metadata_tmp_path);
    });

    std::lock_guard lock{mutex};
    assertTableHasNoUnfinishedExchangeUnlocked(table_id.table_name);
    auto actual_table_id = getTableUnlocked(table_id.table_name)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER, "Cannot alter table because it was renamed");

    auto txn = query_context->getZooKeeperMetadataTransaction();
    if (txn && !query_context->isInternalSubquery())
        txn->commit();      /// Commit point (a sort of) for Replicated database

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery

    check_file_exists = db_disk->renameExchangeIfSupported(table_metadata_tmp_path, table_metadata_path);
    if (!check_file_exists)
        db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);
}

void DatabaseAtomic::assertDetachedTableNotInUse(const UUID & uuid)
{
    /// Without this check the following race is possible since table RWLocks are not used:
    /// 1. INSERT INTO table ...;
    /// 2. DETACH TABLE table; (INSERT still in progress, it holds StoragePtr)
    /// 3. ATTACH TABLE table; (new instance of Storage with the same UUID is created, instances share data on disk)
    /// 4. INSERT INTO table ...; (both Storage instances writes data without any synchronization)
    /// To avoid it, we remember UUIDs of detached tables and does not allow ATTACH table with such UUID until detached instance still in use.
    if (detached_tables.contains(uuid))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Cannot attach table with UUID {}, "
                        "because it was detached but still used by some query. Retry later.", uuid);
}

void DatabaseAtomic::setDetachedTableNotInUseForce(const UUID & uuid)
{
    std::lock_guard lock{mutex};
    detached_tables.erase(uuid);
}

DatabaseAtomic::DetachedTables DatabaseAtomic::cleanupDetachedTables()
{
    DetachedTables not_in_use;
    if (detached_tables.empty())
        return not_in_use;
    auto it = detached_tables.begin();
    LOG_DEBUG(log, "There are {} detached tables. Start searching non used tables.", detached_tables.size());
    while (it != detached_tables.end())
    {
        if (isSharedPtrUnique(it->second))
        {
            not_in_use.emplace(it->first, it->second);
            it = detached_tables.erase(it);
        }
        else
            ++it;
    }
    LOG_DEBUG(log, "Found {} non used tables in detached tables.", not_in_use.size());
    /// It should be destroyed in caller with released database mutex
    return not_in_use;
}

void DatabaseAtomic::assertCanBeDetached(bool cleanup)
{
    if (cleanup)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock(mutex);
            not_in_use = cleanupDetachedTables();
        }
    }
    std::lock_guard lock(mutex);
    if (!detached_tables.empty())
        throw Exception(ErrorCodes::DATABASE_NOT_EMPTY, "Database {} cannot be detached, because some tables are still in use. "
                        "Retry later.", backQuoteIfNeed(database_name));
}

DatabaseTablesIteratorPtr
DatabaseAtomic::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    auto base_iter = DatabaseOrdinary::getTablesIterator(local_context, filter_by_table_name, skip_not_loaded);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

UUID DatabaseAtomic::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseAtomic::beforeLoadingMetadata(ContextMutablePtr /*context*/, LoadingStrictnessLevel mode)
{
    auto db_disk = getDisk();

    if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
        return;

    if (!db_disk->isSymlinkSupported())
        return;

    // When `db_disk` is a `DiskLocal` object, `existsDirectory` will return false if the input path is a symlink.
    // So we use `existsFileOrDirectory` here to check if the symlink exists.
    if (!db_disk->existsFileOrDirectory(path_to_table_symlinks))
        return;

    /// Recreate symlinks to table data dirs in case of force restore, because some of them may be broken
    for (const auto it = db_disk->iterateDirectory(path_to_table_symlinks); it->isValid(); it->next())
    {
        auto table_path = fs::path(it->path());
        if (table_path.filename().empty())
            table_path = table_path.parent_path();
        if (!db_disk->isSymlink(table_path))
        {
            throw Exception(
                ErrorCodes::ABORTED, "'{}' is not a symlink. Atomic database should contains only symlinks.", std::string(table_path));
        }

        db_disk->removeFileIfExists(table_path);
    }
}

ASTPtr DatabaseAtomic::readCreateQueryFromMetadataFile(const String & table_name, bool throw_on_error) const
{
    /// Lets a test hold a reader that has found the table until a swap is between its moves. This runs for every table in
    /// `system.tables`, so the check of the query id comes second: it is not evaluated in builds without failpoints.
    if (FailPointInjection::hasAnyFailPointBeenRegistered()
        && CurrentThread::getQueryId().starts_with(pause_before_reading_create_query_query_id_prefix))
        FailPointInjection::pauseFailPoint(FailPoints::database_atomic_pause_before_reading_create_query);
    /// The file of a table of an unfinished exchange can be missing or hold the definition of the other table. Checked before
    /// the read, so that a missing file is reported as the unfinished exchange, and after it, because an exchange that fails
    /// during the read sets the fence before it releases `metadata_files_mutex`. Never while holding `metadata_files_mutex`,
    /// because the check takes `mutex`, which an exchange holds while it waits for `metadata_files_mutex`.
    /// Without a fence in the database, neither check locks `mutex`, see `has_unfinished_exchange`.
    auto table_has_unfinished_exchange = [&]
    {
        if (!has_unfinished_exchange.load(std::memory_order_relaxed))
            return false;
        std::lock_guard lock{mutex};
        if (!unfinished_exchanges.contains(table_name))
            return false;
        if (throw_on_error)
            assertTableHasNoUnfinishedExchangeUnlocked(table_name);
        return true;
    };
    if (table_has_unfinished_exchange())
        return nullptr;
    ASTPtr ast;
    {
        std::shared_lock files_lock(metadata_files_mutex);
        ast = DatabaseOrdinary::readCreateQueryFromMetadataFile(table_name, throw_on_error);
    }
    if (table_has_unfinished_exchange())
        return nullptr;
    return ast;
}

std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseAtomic::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    /// The base skips a table whose definition cannot be read, which is the case for the tables of an unfinished exchange,
    /// so they are refused here instead of left out of the backup. Checked after the reads, see `readCreateQueryFromMetadataFile`.
    auto res = DatabaseOrdinary::getTablesForBackup(filter, local_context);
    /// See `has_unfinished_exchange`.
    if (!has_unfinished_exchange.load(std::memory_order_relaxed))
        return res;
    std::lock_guard lock{mutex};
    for (const auto & [table_name, _] : unfinished_exchanges)
        if (!filter || filter(table_name))
            assertTableHasNoUnfinishedExchangeUnlocked(table_name);
    return res;
}

void DatabaseAtomic::checkNoUnfinishedExchange(const String & table_name) const
{
    /// The caller holds the DDL guard of `table_name`, see `has_unfinished_exchange`, or accepts missing a fence set at the
    /// same moment, see `StorageMaterializedView::checkTableCanBeDropped`. `DatabaseOverlay::getCreateTableQueryImpl` calls
    /// it after a read of the definition that the fence refused in the same thread. With an empty name, the caller holds
    /// no lock that an exchange takes, so the check locks `mutex` and waits for an exchange that holds it.
    if (!table_name.empty() && !has_unfinished_exchange.load(std::memory_order_relaxed))
        return;
    std::lock_guard lock{mutex};
    if (!table_name.empty())
    {
        assertTableHasNoUnfinishedExchangeUnlocked(table_name);
        return;
    }
    if (!unfinished_exchanges.empty())
    {
        const auto & [_, exchange] = *unfinished_exchanges.begin();
        throw Exception(ErrorCodes::UNFINISHED_METADATA_EXCHANGE,
                        "An exchange of {} in database {} failed and could not be finished, so the database cannot be dropped "
                        "or detached until the server restarts. {}",
                        exchange.files, backQuote(database_name), exchange.hint);
    }
}

void DatabaseAtomic::warnAboutUnfinishedExchange(
    std::unique_lock<std::mutex> & lock, const String & table_name, const String & other_table_name) const noexcept
    TSA_NO_THREAD_SAFETY_ANALYSIS   /// Reads `database_name` under `lock`, then releases it.
{
    try
    {
        const auto warning = PreformattedMessage::create(
            "An exchange of the metadata files of {}.{} and {}.{} failed and could not be finished, so DDL on these two tables "
            "and reads of their definitions fail until the server restarts. The server log has an error for each such exchange "
            "that says what the restart does. If several exchanges failed, this warning is about the last one",
            backQuote(database_name), backQuote(table_name), backQuote(database_name), backQuote(other_table_name));
        /// `system.warnings` is updated under the lock of the global context, so `mutex` is released first.
        lock.unlock();
        getContext()->addOrUpdateWarningMessage(Context::WarningType::UNFINISHED_METADATA_EXCHANGE, warning);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot add the warning about an exchange of metadata files that could not be finished");
    }
}

void DatabaseAtomic::assertTableHasNoUnfinishedExchangeUnlocked(const String & table_name) const
{
    if (auto it = unfinished_exchanges.find(table_name); it != unfinished_exchanges.end())
        throw Exception(ErrorCodes::UNFINISHED_METADATA_EXCHANGE,
                        "An exchange of {} failed and could not be finished, so DDL on table {}.{} and reads of its definition "
                        "fail until the server restarts. {}",
                        it->second.files, backQuote(database_name), backQuote(table_name), it->second.hint);
}

String DatabaseAtomic::readObjectMetadataFile(const String & object_name) const
{
    std::shared_lock files_lock(metadata_files_mutex);
    return DatabaseOrdinary::readObjectMetadataFile(object_name);
}

time_t DatabaseAtomic::getObjectMetadataModificationTime(const String & object_name) const
{
    std::shared_lock files_lock(metadata_files_mutex);
    return DatabaseOrdinary::getObjectMetadataModificationTime(object_name);
}

bool DatabaseAtomic::finishInterruptedMetadataExchangesAtLoad() const
{
    /// This does not read the server setting `database_atomic_allow_exchange_emulation`: a crash can leave files of a swap
    /// after the setting was turned off, and the load must still finish it.
    /// `DatabaseReplicated` derives from `DatabaseAtomic` and is excluded, so it keeps refusing a temporary file of a swap.
    if (typeid(*this) != typeid(DatabaseAtomic))
        return false;
    auto db_disk = getDisk();
    if (!canEmulateExchange(db_disk) || db_disk->isReadOnly())
        return false;
    const bool fsync = getContext()->getGlobalContext()->getSettingsRef()[Setting::fsync_metadata];
    finishInterruptedMetadataExchanges(db_disk, getMetadataPath(), fsync, log);
    return true;
}

bool DatabaseAtomic::canEmulateExchange(const DiskPtr & disk)
{
    return dynamic_cast<const DiskLocal *>(disk.get()) != nullptr;
}

void DatabaseAtomic::exchangeMetadataFilesWithRecord(
    const String & table_name,
    const String & other_table_name,
    const DiskPtr & disk,
    const MetadataExchangeRecord & record,
    const String & record_path,
    bool fsync)
{
    /// Readers of the metadata files wait until the moves and the handling of their failure are over.
    std::unique_lock files_lock(metadata_files_mutex);

    LOG_DEBUG(log, "Exchanging {} and {} with an exchange record", record.old_path, record.new_path);

    /// The temporary file exists only between the first and the last move, so it shows that an exchange is in progress.
    const String tmp_path = metadataExchangeTmpPath(record.old_path);
    /// On a filesystem without `RENAME_EXCHANGE`, such as NFS, `RENAME_NOREPLACE` fails with `EINVAL` too, so each `moveFile` checks
    /// that its target is absent, then does a plain rename (`renameNoReplaceFallback`). That is safe here: no other query can create
    /// a target while `mutex` and the DDL guards of both names are held, and every target is absent when its move runs. The two
    /// metadata files are moved away by the move before. The temporary file of `table_name` is left only by a crash, or by an earlier
    /// exchange of `table_name` that failed and was fenced, which refuses this exchange until the restart. The load at the restart
    /// finishes that exchange or refuses to load the database.
    auto try_move_files = [&]() -> std::exception_ptr
    {
        try
        {
            fiu_do_on(FailPoints::database_atomic_exchange_fail_before_first_move,
                { throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault before the first move of an exchange"); });
            fiu_do_on(FailPoints::database_atomic_exchange_always_fail_before_first_move,
                { throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault before the first move of an exchange"); });
            disk->moveFile(record.old_path, tmp_path);
            fiu_do_on(FailPoints::database_atomic_exchange_fail_after_first_move,
                { throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault after the first move of an exchange"); });
            disk->moveFile(record.new_path, record.old_path);
            fiu_do_on(FailPoints::database_atomic_exchange_fail_after_second_move,
                { throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault after the second move of an exchange"); });
            /// Lets a test hold the swap while a file is missing, to check that readers wait for the moves. The query id is
            /// checked second, so that it is not read in builds without failpoints.
            if (FailPointInjection::hasAnyFailPointBeenRegistered()
                && CurrentThread::getQueryId().starts_with(pause_after_second_move_query_id_prefix))
                FailPointInjection::pauseFailPoint(FailPoints::database_atomic_exchange_pause_after_second_move);
            disk->moveFile(tmp_path, record.new_path);
            fiu_do_on(FailPoints::database_atomic_exchange_fail_after_third_move,
                { throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault after the third move of an exchange"); });
            return nullptr;
        }
        catch (...)
        {
            /// Handled below by the state of the files.
            return std::current_exception();
        }
    };

    enum class Outcome : uint8_t
    {
        Done,
        Retry,
        FailCleanly,
        Fence,
    };

    /// A move can also report an error after it succeeded, so the state of the files decides, not the error.
    std::exception_ptr error;
    /// Built here, so that the `catch` of the `noexcept` lambda below does not allocate.
    const String finish_error_prefix = fmt::format("Cannot finish the exchange of {} and {}", record.old_path, record.new_path);
    auto handle_error = [&](bool retried) noexcept -> Outcome
    {
        try
        {
            switch (inspectMetadataExchange(disk, record))
            {
                case MetadataExchangeState::Interrupted:
                    fiu_do_on(FailPoints::database_atomic_exchange_fail_to_finish,
                        { throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault before finishing an exchange"); });
                    finishMetadataExchange(disk, record, fsync);
                    LOG_WARNING(log, "Finished the exchange of {} and {} after an error: {}",
                        record.old_path, record.new_path, getExceptionMessage(error, false));
                    return Outcome::Done;
                case MetadataExchangeState::AfterExchange:
                    LOG_WARNING(log, "The exchange of {} and {} completed despite an error: {}",
                        record.old_path, record.new_path, getExceptionMessage(error, false));
                    return Outcome::Done;
                case MetadataExchangeState::BeforeExchange:
                    if (!retried)
                        return Outcome::Retry;
                    /// Nothing has changed, so the query can fail.
                    return Outcome::FailCleanly;
                case MetadataExchangeState::Inconsistent:
                    return Outcome::Fence;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, finish_error_prefix);
        }
        return Outcome::Fence;
    };

    error = try_move_files();
    Outcome outcome = error ? handle_error(/* retried */ false) : Outcome::Done;
    if (outcome == Outcome::Retry)
    {
        /// Runs the moves again instead of `finishMetadataExchange`. In `BeforeExchange` there is no temporary file, so a crash between
        /// its two writes would leave two metadata files with the same UUID, and the next load would not see an interrupted exchange.
        LOG_WARNING(log, "Retrying the exchange of {} and {} after an error: {}",
            record.old_path, record.new_path, getExceptionMessage(error, false));
        if (auto retry_error = try_move_files())
        {
            error = retry_error;
            outcome = handle_error(/* retried */ true);
        }
        else
        {
            outcome = Outcome::Done;
        }
    }

    if (outcome == Outcome::Fence)
    {
        /// The fence must be set even when the query is over its memory limit. Otherwise an allocation for the hint or for
        /// `unfinished_exchanges` could throw before both names are fenced, and DDL could change the half-exchanged files.
        LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

        /// The record stays for the next load, and until then DDL on the two tables fails. The next load finishes the exchange
        /// when the temporary file is left. Without it, the load only removes the record and leaves the files as they are.
        /// The hint tells the user which of these the restart does.
        std::optional<bool> tmp_exists;
        try
        {
            tmp_exists = disk->existsFile(tmp_path);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Cannot check whether {} exists", tmp_path));
        }

        /// `CORRUPTED_DATA` from the load of a database at server start fails the start of the server.
        const String corrupted_data_outcome = "or, if the files hold none of the recorded contents, reports `CORRUPTED_DATA`, and the "
            "server does not start until the files are repaired by hand as that error describes";
        String hint;
        if (tmp_exists.has_value() && *tmp_exists)
        {
            hint = fmt::format(
                "The temporary file {} is left, so the next load finishes the exchange from its record {}, {}",
                tmp_path, record_path, corrupted_data_outcome);
        }
        else
        {
            if (tmp_exists.has_value())
                hint = fmt::format("The temporary file {} is not left, so ", tmp_path);
            else
                hint = fmt::format(
                    "Cannot check whether the temporary file {} is left. If it is, the next load finishes the exchange from its record {}, "
                    "{}. If it is not, ",
                    tmp_path, record_path, corrupted_data_outcome);
            hint += fmt::format(
                "the next load removes the record {} and leaves the two files as they are, "
                "so compare them with the contents in the record before the restart",
                record_path);
        }
        fenceUnfinishedExchange(table_name, other_table_name,
            fmt::format("{} and {}", record.old_path, record.new_path), std::move(hint), error);
    }

    /// Without the temporary file, the record is harmless, and the next load removes it.
    tryRemoveMetadataExchangeFiles(disk, {record_path}, log, /* found_at_load */ false);
    if (outcome == Outcome::FailCleanly)
        std::rethrow_exception(error);
}

void DatabaseAtomic::fenceUnfinishedExchange(
    const String & table_name,
    const String & other_table_name,
    const String & files,
    String hint,
    std::exception_ptr error)
{
    /// The fence also refuses the cleanup `DROP` of the temporary table of `CREATE OR REPLACE` or of a refresh,
    /// so the hint says what becomes of that table and of the rows inserted until the restart.
    hint += ". Inserts still reach the tables as they were before the exchange. If the exchange was done by `CREATE OR REPLACE` "
        "or `REPLACE TABLE`, its `_tmp_replace_*` table may still exist after the restart";
    /// Until the restart, each name keeps the table it had before the exchange. The files on disk can be exchanged at the restart
    /// even without the temporary file: the moves or the finish may have completed before an error was reported.
    hint += ". If the two files hold the contents after the exchange when the server starts again, because the load finishes the "
        "exchange or because the files were already exchanged, rows inserted into the replaced table before the restart end up in "
        "the `_tmp_replace_*` table, and it should be checked for such rows before it is dropped";
    /// Until the restart, the fence refuses the `CREATE OR REPLACE` of the `.tmp` table by the next refresh.
    hint += ". If it was done by a refresh of a materialized view, the first refresh after the restart replaces the leftover `.tmp` "
        "table. A `ReplicatedMergeTree` table among the two cannot apply `ALTER` entries until the restart";

    const UnfinishedExchange unfinished{files, hint};
    unfinished_exchanges[table_name] = unfinished;
    unfinished_exchanges[other_table_name] = unfinished;
    has_unfinished_exchange.store(true, std::memory_order_relaxed);

    LOG_ERROR(log, "Cannot finish the exchange of {} after an error: {}. DDL on the two tables and reads of their definitions "
        "fail until the server restarts. {}", files, getExceptionMessage(error, false), hint);
    const String message = fmt::format(
        "The exchange of {} could not be finished, so DDL on the two tables and reads of their definitions fail "
        "until the server restarts. {}", files, hint);

    try
    {
        std::rethrow_exception(error);
    }
    catch (Exception & e)
    {
        e.addMessage(message);
        throw;
    }
    catch (...)
    {
        /// A failed move on a local disk usually arrives as `std::filesystem::filesystem_error`, which cannot carry the message.
        /// The new exception keeps its code and message. The version is left out, because the final report adds it.
        throw [&]
        {
            auto e = Exception::createRuntime(
                getCurrentExceptionCode(),
                getCurrentExceptionMessage(
                    /* with_stacktrace */ false, /* check_embedded_stacktrace */ false, /* with_extra_info */ true, /* with_version */ false));
            e.addMessage(message);
            return e;
        }();
    }
}

LoadTaskPtr DatabaseAtomic::startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode)
{
    auto db_disk = getDisk();

    auto base = DatabaseOrdinary::startupDatabaseAsync(async_loader, std::move(startup_after), mode);
    auto job = makeLoadJob(
        base->goals(),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup Atomic database {}", getDatabaseName()),
        [this, mode, db_disk](AsyncLoader &, const LoadJobPtr &)
        {
            if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
                return;
            NameToPathMap table_names;
            {
                std::lock_guard lock{mutex};
                table_names = table_name_to_path;
            }
            if (db_disk->isSymlinkSupported())
                db_disk->createDirectories(path_to_table_symlinks);
            for (const auto & table : table_names)
            {
                /// All tables in database should be loaded at this point
                StoragePtr table_ptr = tryGetTable(table.first, getContext());
                if (table_ptr)
                {
                    if (table_ptr->storesDataOnDisk())
                        tryCreateSymlink(table_ptr, true);
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is not loaded before database startup", table.first);
            }
        });
    std::scoped_lock lock(mutex);
    return startup_atomic_database_task = makeLoadTask(async_loader, {job});
}

void DatabaseAtomic::waitDatabaseStarted() const
{
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        task = startup_atomic_database_task;
    }
    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task, false);
}

void DatabaseAtomic::stopLoading()
{
    LoadTaskPtr stop_atomic_database;
    {
        std::scoped_lock lock(mutex);
        stop_atomic_database.swap(startup_atomic_database_task);
    }
    stop_atomic_database.reset();
    DatabaseOrdinary::stopLoading();
}

void DatabaseAtomic::tryCreateSymlink(const StoragePtr & table, bool if_data_path_exist)
{
    auto db_disk = getDisk();

    if (!db_disk->isSymlinkSupported())
        return;

    if (table->getDataPaths().empty())
        return;

    const auto table_data_path = fs::path(table->getDataPaths().front()).lexically_normal();

    try
    {
        String table_name = table->getStorageID().getTableName();

        if (!table->storesDataOnDisk())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} doesn't have data path to create symlink", table_name);

        String link = path_to_table_symlinks / escapeForFileName(table_name);

        LOG_DEBUG(
            log,
            "Trying to create a symlink for table {}, data_path {}, link {}",
            table->getStorageID().getNameForLogs(),
            table_data_path,
            link);

        /// If it already points where needed.
        if (db_disk->equivalentNoThrow(table_data_path, link))
            return;

        if (if_data_path_exist && !db_disk->existsFileOrDirectory(data_path))
            return;

        db_disk->createDirectorySymlink(table_data_path, link);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

void DatabaseAtomic::tryRemoveSymlink(const String & table_name)
{
    auto db_disk = getDisk();

    if (!db_disk->isSymlinkSupported())
        return;

    try
    {
        String path = path_to_table_symlinks / escapeForFileName(table_name);
        db_disk->removeFileIfExists(path);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

void DatabaseAtomic::tryCreateMetadataSymlink()
{
    auto db_disk = getDisk();
    if (!db_disk->isSymlinkSupported())
        return;

    /// Symlinks in data/db_name/ directory and metadata/db_name/ are not used by ClickHouse,
    /// it's needed only for convenient introspection.
    chassert(path_to_metadata_symlink != metadata_path);
    if (db_disk->existsFileOrDirectory(path_to_metadata_symlink))
    {
        if (!db_disk->isSymlink(path_to_metadata_symlink))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Directory {} already exists", path_to_metadata_symlink);
    }
    else
    {
        try
        {
            /// fs::exists could return false for broken symlink
            if (db_disk->isSymlinkNoThrow(path_to_metadata_symlink))
                db_disk->removeFileIfExists(path_to_metadata_symlink);

            LOG_DEBUG(
                log,
                "Creating directory symlink, path_to_metadata_symlink: {}, metadata_path: {}",
                path_to_metadata_symlink,
                metadata_path);

            db_disk->createDirectorySymlink(metadata_path, path_to_metadata_symlink);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

void DatabaseAtomic::renameDatabase(ContextPtr query_context, const String & new_name)
{
    auto component_guard = Coordination::setCurrentComponent("DatabaseAtomic::renameDatabase");
    /// CREATE, ATTACH, DROP, DETACH and RENAME DATABASE must hold DDLGuard
    createDirectories();
    waitDatabaseStarted();
    std::lock_guard lock(mutex);

    /// A longer database name leaves less room for the table name in the dropped-metadata file
    /// name metadata_dropped/{db}.{table}.{uuid}.sql, so a rename can leave a table that cannot
    /// be dropped. Detached tables are checked too, because ATTACH does not re-check the length.
    for (const auto & table : tables)
        checkTableNameLengthUnlocked(new_name, table.first, getContext());
    for (const auto & detached_table : snapshot_detached_tables)
        checkTableNameLengthUnlocked(new_name, detached_table.first, getContext());

    /// The `renameInMemory` loop below only notifies the storages, it does not ask them whether the new
    /// database name is acceptable. Detached tables are not asked: no storage object exists for them here.
    for (const auto & table : tables)
        table.second->checkTableCanBeRenamedByDatabaseRename(new_name);

    bool check_ref_deps = query_context->getSettingsRef()[Setting::check_referential_table_dependencies];
    bool check_loading_deps = !check_ref_deps && query_context->getSettingsRef()[Setting::check_table_dependencies];
    if (check_ref_deps || check_loading_deps)
    {
        for (auto & table : tables)
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed({database_name, table.first}, check_ref_deps, check_loading_deps);
    }

    /// The rename re-keys every table of the database under the new name in the server-wide dependency
    /// graph, which can close a cycle - the simplest case being a table `db.t = Alias(<new name>, t)`,
    /// accepted at `CREATE` because the target database did not exist yet and thus becoming a
    /// self-reference. Refuse such a rename before anything is modified, the same way `RENAME TABLE`
    /// refuses it for a single table. The check is done here, under `mutex` and the exclusive database
    /// DDL lock held by the caller, so that it sees exactly the set of tables that is about to be
    /// re-keyed below: no table can be created, attached or dropped in between.
    Strings table_names;
    table_names.reserve(tables.size());
    for (const auto & table : tables)
        table_names.push_back(table.first);
    DatabaseCatalog::instance().checkDatabaseCanBeRenamedWithNoCyclicDependencies(database_name, new_name, table_names);

    /// Lets a test hold the rename between the dependency check and the catalog rewrite, to prove
    /// that a concurrent `CREATE TABLE` cannot slip a new table into the database at this point, and
    /// that a concurrent `CREATE TABLE` in another database cannot close a cycle that this check missed.
    FailPointInjection::pauseFailPoint(FailPoints::rename_database_after_dependency_check);


    try
    {
        auto db_disk = getDisk();
        if (db_disk->isSymlinkSupported())
            db_disk->removeFileIfExists(path_to_metadata_symlink);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }

    auto old_metadata_file_path = DatabaseCatalog::getMetadataFilePath(database_name);
    auto new_metadata_file_path = DatabaseCatalog::getMetadataFilePath(new_name);
    auto default_db_disk = getContext()->getDatabaseDisk();
    default_db_disk->moveFile(old_metadata_file_path, new_metadata_file_path);

    String old_path_to_table_symlinks;

    {
        /// `updateDatabaseName` repeats the cycle check under the same lock as the rewrite, because DDL in
        /// other databases is not excluded by our locks and may have added a dependency edge since the
        /// check above. If it refuses the rename, the catalog is untouched; put the metadata file back.
        try
        {
            DatabaseCatalog::instance().updateDatabaseName(database_name, new_name, table_names);
        }
        catch (...)
        {
            default_db_disk->moveFile(new_metadata_file_path, old_metadata_file_path);
            tryCreateMetadataSymlink();
            throw;
        }
        database_name = new_name;

        onDatabaseRenamed();

        for (auto & table : tables)
        {
            auto table_id = table.second->getStorageID();
            table_id.database_name = database_name;
            table.second->renameInMemory(table_id);
        }

        for (auto & [detached_table_name, snapshot] : snapshot_detached_tables)
        {
            snapshot.database = database_name;
        }

        path_to_metadata_symlink = DatabaseCatalog::getMetadataDirPath(new_name);
        old_path_to_table_symlinks = path_to_table_symlinks;
        path_to_table_symlinks = DatabaseCatalog::getDataDirPath(new_name) / "";
    }

    auto db_disk = getDisk();
    if (db_disk->isSymlinkSupported())
    {
        db_disk->moveDirectory(old_path_to_table_symlinks, path_to_table_symlinks);
        tryCreateMetadataSymlink();
    }
}

void DatabaseAtomic::waitDetachedTableNotInUse(const UUID & uuid, std::function<void()> throw_if_cancelled)
{
    /// Table is in use while its shared_ptr counter is greater than 1.
    /// We cannot trigger condvar on shared_ptr destruction, so it's busy wait.
    LOG_DEBUG(log, "Waiting for detached table {} to be no longer in use", toString(uuid));

    unsigned iterations = 0;
    while (!DatabaseCatalog::instance().isShuttingDown())
    {
        bool found = true;
        int64_t use_count = 0;
        bool log_slow_wait = false;
        DetachedTables not_in_use;
        {
            std::lock_guard lock{mutex};
            not_in_use = cleanupDetachedTables();
            if (!detached_tables.contains(uuid))
            {
                found = false;
            }
            else if (iterations > 0 && iterations % 100 == 0)
            {
                auto it = detached_tables.find(uuid);
                if (it != detached_tables.end() && it->second)
                    use_count = it->second.use_count();
                log_slow_wait = true;
            }
        }
        /// not_in_use destroyed here (after lock released) — StoragePtrs freed without holding mutex

        if (!found)
        {
            LOG_DEBUG(log, "Detached table {} is no longer in use", toString(uuid));
            return;
        }

        /// Check cancellation after verifying the table is still tracked.
        /// This ordering avoids throwing a cancellation exception when
        /// the wait has already completed.
        if (throw_if_cancelled)
            throw_if_cancelled();

        if (log_slow_wait)
            LOG_INFO(log, "Still waiting for detached table {} to be no longer in use (use_count={}, elapsed ~{}s)",
                toString(uuid), use_count, iterations / 10);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ++iterations;
    }

    /// Server is shutting down. Do one final cleanup pass — the table may have
    /// become free just before or during shutdown.
    bool still_tracked = false;
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock{mutex};
            not_in_use = cleanupDetachedTables();
            still_tracked = detached_tables.contains(uuid);
        }
    }

    if (!still_tracked)
    {
        LOG_DEBUG(log, "Detached table {} is no longer in use (resolved during shutdown)", toString(uuid));
        return;
    }

    throw Exception(ErrorCodes::UNFINISHED,
        "Did not finish waiting for detached table {} to be no longer in use "
        "because the server is shutting down", uuid);
}

void DatabaseAtomic::checkDetachedTableNotInUse(const UUID & uuid)
{
    DetachedTables not_in_use;
    std::lock_guard lock{mutex};
    not_in_use = cleanupDetachedTables();
    assertDetachedTableNotInUse(uuid);
}

void registerDatabaseAtomic(DatabaseFactory & factory);

void registerDatabaseAtomic(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        if (args.database_name.ends_with(DatabaseReplicated::BROKEN_REPLICATED_TABLES_SUFFIX))
            args.context->addOrUpdateWarningMessage(
                Context::WarningType::MAYBE_BROKEN_TABLES,
                PreformattedMessage::create(
                    "The database {} is probably created during recovering a lost replica. If it has no tables, it can be deleted. If it "
                    "has tables, it worth to check why they were considered broken.",
                    backQuoteIfNeed(args.database_name)));

        DatabaseMetadataDiskSettings database_metadata_disk_settings;
        auto * engine_define = args.create_query.storage;
        chassert(engine_define);
        database_metadata_disk_settings.loadFromQuery(*engine_define, args.context, args.isLoadingFromExistingMetadata());

        return make_shared<DatabaseAtomic>(
            args.database_name, args.metadata_path, args.uuid, args.context, database_metadata_disk_settings);
    };
    factory.registerDatabase("Atomic", create_fn, SecretArgumentsSpec{}, /*features=*/{.supports_settings = true, .has_builtin_setting_fn = DatabaseMetadataDiskSettings::hasBuiltin}, Documentation{
        .description = R"DOCS_MD(
The `Atomic` engine supports non-blocking [`DROP TABLE`](#drop-detach-table) and [`RENAME TABLE`](#rename-table) queries, and atomic [`EXCHANGE TABLES`](#exchange-tables) queries. The `Atomic` database engine is used by default in open-source ClickHouse.

<Note>
On ClickHouse Cloud, the [`Shared` database engine](/products/cloud/features/infrastructure/shared-catalog#shared-database-engine) is used by default and also supports
the above mentioned operations.
</Note>

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS name = value, ...];
```

`ENGINE = Atomic` may be omitted, because it is the default. A `SETTINGS` clause may hold both settings
of the database engine (such as [`disk`](#metadata-disk) or [`max_tables`](#limiting-the-number-of-tables))
and ordinary query settings; each name is dispatched to whichever of the two it belongs to.

## Specifics and recommendations {#specifics-and-recommendations}

### Table UUID {#table-uuid}

Each table in the `Atomic` database has a persistent [UUID](/reference/data-types/uuid) and stores its data in the following directory:

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

Where `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` is the UUID of the table.

By default, the UUID is generated automatically. However, users can explicitly specify the UUID when creating a table, though this is not recommended.

For example:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

<Note>
You can use the [show_table_uuid_in_table_create_query_if_not_nil](/reference/settings/session-settings/show#show_table_uuid_in_table_create_query_if_not_nil) setting to display the UUID with the `SHOW CREATE` query.
</Note>

### RENAME TABLE {#rename-table}

[`RENAME`](/reference/statements/rename) queries do not modify the UUID or move table data. These queries execute immediately and do not wait for other queries that are using the table to complete.

### DROP/DETACH TABLE {#drop-detach-table}

When using `DROP TABLE`, no data is removed. The `Atomic` engine just marks the table as dropped by moving it's metadata to `/clickhouse_path/metadata_dropped/` and notifies the background thread. The delay before the final table data deletion is specified by the [`database_atomic_delay_before_drop_table_sec`](/reference/settings/server-settings/settings/other#database_atomic_delay_before_drop_table_sec) setting.
You can specify synchronous mode using `SYNC` modifier. Use the [`database_atomic_wait_for_drop_and_detach_synchronously`](/reference/settings/session-settings/database#database_atomic_wait_for_drop_and_detach_synchronously) setting to do this. In this case `DROP` waits for running `SELECT`, `INSERT` and other queries which are using the table to finish. The table will be removed when it's not in use.

### EXCHANGE TABLES/DICTIONARIES {#exchange-tables}

The [`EXCHANGE`](/reference/statements/exchange) query swaps tables or dictionaries atomically. For instance, instead of this non-atomic operation:

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```
you can use an atomic one:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

### ReplicatedMergeTree in atomic database {#replicatedmergetree-in-atomic-database}

For [`ReplicatedMergeTree`](/reference/engines/table-engines/mergetree-family/replication) tables, it is recommended not to specify the engine parameters for the path in ZooKeeper and the replica name. In this case, the configuration parameters [`default_replica_path`](/reference/settings/server-settings/settings/default-replica#default_replica_path) and [`default_replica_name`](/reference/settings/server-settings/settings/default-replica#default_replica_name) will be used. If you want to specify engine parameters explicitly, it is recommended to use the `{uuid}` macros. This ensures that unique paths are automatically generated for each table in ZooKeeper.

### Metadata disk {#metadata-disk}
When `disk` is specified in `SETTINGS`, the disk is used to store table metadata files.
It can name a disk from the server configuration, or define one inline with the `disk` function,
the same way a single table does:

```sql
CREATE DATABASE db SETTINGS disk = 'db_disk';
CREATE DATABASE db SETTINGS disk = disk(type = 'local', path = '/var/lib/clickhouse-disks/db_disk');
```

If unspecified, the disk defined in `database_disk.disk` is used by default.

The same `SETTINGS` clause works for `ATTACH DATABASE`, which is how a database whose metadata files
live on another disk is attached to a server. `Atomic` requires the `UUID` of the database to be given
explicitly in that case:

```sql
ATTACH DATABASE db UUID '28f1c61c-2970-457a-bffe-454156ddcfef'
SETTINGS disk = disk(type = 'local', path = '/var/lib/clickhouse-disks/db_disk');
```

### Metadata on NFS {#metadata-on-nfs}

`EXCHANGE TABLES`, `EXCHANGE DICTIONARIES`, `CREATE OR REPLACE`, `REPLACE TABLE` and the refreshes of refreshable materialized views
that are not `APPEND` swap the metadata files of two tables with the system call `renameat2(RENAME_EXCHANGE)`. NFS, including
Amazon EFS, does not support that call, so these statements fail with `UNSUPPORTED_METHOD` when the metadata of the database is on NFS.

With the experimental server setting [`database_atomic_allow_exchange_emulation`](/reference/settings/server-settings/settings/other#database_atomic_allow_exchange_emulation),
`CREATE OR REPLACE`, `REPLACE TABLE` and refreshes work on NFS when the metadata is on a `local` disk. The swap first writes a record
of both files, then moves the files one at a time, so the next load of the database finishes a swap that a crash interrupted.
`EXCHANGE TABLES`, `EXCHANGE DICTIONARIES`, exchanges between two databases and `Replicated` databases still fail on NFS.
The description of the setting explains what happens when a swap fails and cannot be finished.

### Limiting the number of tables {#limiting-the-number-of-tables}

The `max_tables` setting limits how many tables the database may contain. `0` (the default) means unlimited. Every table-like object counts toward the limit: an ordinary table, a view, a materialized view, and a dictionary created with `CREATE DICTIONARY`. When the limit is reached, `CREATE TABLE`, `CREATE DICTIONARY` and `ATTACH TABLE` throw a `TOO_MANY_TABLES` exception.

```sql
CREATE DATABASE db ENGINE = Atomic SETTINGS max_tables = 100;
```

The limit can be changed for an existing database with `ALTER DATABASE`:

```sql
ALTER DATABASE db MODIFY SETTING max_tables = 200;
```

Lowering the limit below the current number of tables does not drop any tables. It only prevents new ones from being created until the count drops below the limit again.

`CREATE OR REPLACE TABLE` briefly creates the replacement under a temporary name before swapping it in, so replacing a table while the database is exactly at `max_tables` fails with `TOO_MANY_TABLES` even though the final table count would not grow. Moving an object into the database with `RENAME TABLE` or `RENAME DICTIONARY` is also subject to the limit.

A materialized view created without a `TO` clause has a hidden inner table that counts toward the limit as a table of its own.

The limit is checked before an operation starts, so it is best-effort: concurrent queries can push the database slightly over it.

The setting is available for the on-disk database engines that keep their tables in memory and their metadata in local `.sql` files: `Atomic` and `Ordinary`. It is not supported by the `Replicated` engine.

## See also {#see-also}

- [system.databases](/reference/system-tables/databases) system table
)DOCS_MD",
        .syntax = "ENGINE = Atomic",
        .related = {"Replicated", "Ordinary"}});
}

}
