#pragma once

#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/MetadataExchange.h>
#include <Storages/IStorage_fwd.h>
#include <Common/SharedMutex.h>

#include <mutex>
#include <optional>


namespace DB
{

/// All tables in DatabaseAtomic have persistent UUID and store data in
/// /clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
/// where xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy is UUID of the table.
/// RENAMEs are performed without changing UUID and moving table data.
/// Tables in Atomic databases can be accessed by UUID through DatabaseCatalog.
/// On DROP TABLE no data is removed, DatabaseAtomic just marks table as dropped
/// by moving metadata to /clickhouse_path/metadata_dropped/ and notifies DatabaseCatalog.
/// Running queries still may use dropped table. Table will be actually removed when it's not in use.
/// Allows to execute RENAME and DROP without IStorage-level RWLocks
class DatabaseAtomic : public DatabaseOrdinary
{
public:
    DatabaseAtomic(
        String name_,
        String metadata_path_,
        UUID uuid,
        const String & logger_name,
        ContextPtr context_,
        DatabaseMetadataDiskSettings database_metadata_disk_settings_ = {});
    DatabaseAtomic(
        String name_,
        String metadata_path_,
        UUID uuid,
        ContextPtr context_,
        DatabaseMetadataDiskSettings database_metadata_disk_settings_ = {});

    String getEngineName() const override { return "Atomic"; }
    UUID getUUID() const override { return db_uuid; }

    void renameDatabase(ContextPtr query_context, const String & new_name) override;

    void renameTable(
            ContextPtr context,
            const String & table_name,
            IDatabase & to_database,
            const String & to_table_name,
            bool exchange,
            bool dictionary) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync) override;
    void dropTableImpl(ContextPtr context, const String & table_name, bool sync);

    void attachTable(ContextPtr context, const String & name, const StoragePtr & table, const String & relative_table_path) override;
    StoragePtr detachTable(ContextPtr context, const String & name) override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    void drop(ContextPtr /*context*/) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    void beforeLoadingMetadata(ContextMutablePtr context, LoadingStrictnessLevel mode) override;

    /// Finishes the exchanges of metadata files that a crash interrupted, then loads the metadata.
    void loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup) override;

    /// Whether an exchange of metadata files on `disk` can be done with three moves and a record when the disk has no atomic exchange.
    static bool canEmulateExchange(const DiskPtr & disk);

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;
    void waitDatabaseStarted() const override;
    void stopLoading() override;

    /// Atomic database cannot be detached if there is detached table which still in use
    void assertCanBeDetached(bool cleanup) override;

    /// Throws `UNFINISHED` while an exchange of metadata files in the database could not be finished.
    void checkCanBeDroppedOrDetached() const override;

    /// Throws `UNFINISHED` while an exchange of metadata files in the database could not be finished.
    void checkTablesCanBeDroppedOrDetached() const override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void tryCreateSymlink(const StoragePtr & table, bool if_data_path_exist = false);
    void tryRemoveSymlink(const String & table_name);

    void waitDetachedTableNotInUse(const UUID & uuid, std::function<void()> throw_if_cancelled) override;
    void checkDetachedTableNotInUse(const UUID & uuid) override;
    void setDetachedTableNotInUseForce(const UUID & uuid) override;

    /// Waits for the moves of an exchange with a record, see `metadata_files_mutex`.
    time_t getObjectMetadataModificationTime(const String & object_name) const override;

protected:
    /// Waits for the moves of an exchange with a record, see `metadata_files_mutex`.
    ASTPtr readCreateQueryFromMetadataFile(const String & table_name, bool throw_on_error) const override;

    void commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path, const String & statement, ContextPtr query_context) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path, ContextPtr query_context) override;

    void assertDetachedTableNotInUse(const UUID & uuid) TSA_REQUIRES(mutex);
    using DetachedTables = std::unordered_map<UUID, StoragePtr>;
    [[nodiscard]] DetachedTables cleanupDetachedTables() TSA_REQUIRES(mutex);

    void createDirectories();
    void createDirectoriesUnlocked() TSA_REQUIRES(mutex);

    void tryCreateMetadataSymlink();

    virtual bool allowMoveTableToOtherDatabaseEngine(IDatabase & /*to_database*/) const { return false; }

    virtual void onDatabaseRenamed() TSA_REQUIRES(mutex) {}

    //TODO store path in DatabaseWithOwnTables::tables
    using NameToPathMap = std::unordered_map<String, String>;
    NameToPathMap table_name_to_path TSA_GUARDED_BY(mutex);

    DetachedTables detached_tables TSA_GUARDED_BY(mutex);
    std::filesystem::path path_to_table_symlinks;
    std::filesystem::path path_to_metadata_symlink;
    const UUID db_uuid;

    LoadTaskPtr startup_atomic_database_task TSA_GUARDED_BY(mutex);

private:
    /// Whether the filesystem of the metadata directory exchanges two files atomically.
    /// The first call checks it with two scratch files, and later calls return the cached answer.
    bool canExchangeAtomically();

    /// Exchanges the two metadata files of `record` with three moves, instead of an atomic exchange,
    /// and removes the records at `record_paths` that `writeMetadataExchangeRecords` wrote before the Keeper commit.
    /// When a move throws, it reads the files and rolls the exchange forward from the record. When it cannot,
    /// it keeps the records, sets `unfinished_exchange` and `unfinished_exchange_hint` on both databases and rethrows.
    /// `has_keeper_transaction` tells that the Keeper commit ran. The caller holds `mutex` of both databases.
    void exchangeMetadataFilesWithRecord(
        DatabaseAtomic & other_db,
        const DiskPtr & disk,
        const MetadataExchangeRecord & record,
        const Strings & record_paths,
        bool fsync,
        bool has_keeper_transaction) TSA_NO_THREAD_SAFETY_ANALYSIS;

    void assertNoUnfinishedExchangeUnlocked() const TSA_REQUIRES(mutex);

    /// Set when an exchange failed and could not be finished, so the metadata files may be half exchanged.
    /// DDL on the database fails until a restart. The next load finishes an interrupted exchange from its record,
    /// or reports `CORRUPTED_DATA` when it cannot. In `Replicated`, the replica recovery from Keeper repairs files that are behind Keeper.
    String unfinished_exchange TSA_GUARDED_BY(mutex);
    /// What the restart does about the unfinished exchange, for the error of DDL until then.
    String unfinished_exchange_hint TSA_GUARDED_BY(mutex);

    /// Held exclusively by `exchangeMetadataFilesWithRecord` while it moves the metadata files, and shared by the readers
    /// of metadata files that do not take `mutex`, such as `SHOW CREATE TABLE`, `system.tables` and `BACKUP`,
    /// so that they never see a file missing between the moves, as with an atomic exchange.
    /// It is taken after `mutex` and `metadataExchangeMutex`, so a reader must not take `mutex` while holding it:
    /// the shared lock covers only the read of the file, not `getDatabaseName`, which takes `mutex`.
    /// `finishInterruptedMetadataExchanges` rewrites the files at load without it: it puts each file in place with `replaceFile`,
    /// so a reader that reaches the database during `ATTACH DATABASE` never sees a file disappear. It can only see a file
    /// that the crash left missing appear.
    mutable SharedMutex metadata_files_mutex;

    std::mutex exchange_probe_mutex;
    std::optional<bool> exchange_is_atomic TSA_GUARDED_BY(exchange_probe_mutex);
};

}
