#pragma once

#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Storages/IStorage_fwd.h>
#include <Common/SharedMutex.h>


namespace DB
{

struct MetadataExchangeRecord;

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
            bool dictionary,
            bool allow_exchange_emulation) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync) override;
    void dropTableImpl(ContextPtr context, const String & table_name, bool sync);

    void attachTable(ContextPtr context, const String & name, const StoragePtr & table, const String & relative_table_path) override;
    StoragePtr detachTable(ContextPtr context, const String & name) override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    void drop(ContextPtr /*context*/) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    void beforeLoadingMetadata(ContextMutablePtr context, LoadingStrictnessLevel mode) override;

    /// Whether an exchange of metadata files on `disk` can be done with three moves and a record when the disk has no atomic exchange.
    static bool canEmulateExchange(const DiskPtr & disk);

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;
    void waitDatabaseStarted() const override;
    void stopLoading() override;

    /// Atomic database cannot be detached if there is detached table which still in use
    void assertCanBeDetached(bool cleanup) override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void tryCreateSymlink(const StoragePtr & table, bool if_data_path_exist = false);
    void tryRemoveSymlink(const String & table_name);

    void waitDetachedTableNotInUse(const UUID & uuid, std::function<void()> throw_if_cancelled) override;
    void checkDetachedTableNotInUse(const UUID & uuid) override;
    void setDetachedTableNotInUseForce(const UUID & uuid) override;

    /// Waits for the moves of a swap with a record, see `metadata_files_mutex`.
    time_t getObjectMetadataModificationTime(const String & object_name) const override;

protected:
    /// Finishes a swap that a crash interrupted, before `iterateMetadataFiles` processes any metadata file.
    bool finishInterruptedMetadataExchangesAtLoad() const override;

    /// Waits for the moves of a swap with a record, see `metadata_files_mutex`.
    ASTPtr readCreateQueryFromMetadataFile(const String & table_name, bool throw_on_error) const override;
    /// Waits for the moves of a swap with a record, see `metadata_files_mutex`.
    String readObjectMetadataFile(const String & object_name) const override;

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
    /// Held exclusively by a swap of metadata files with a record while it moves the files and handles the failure of a move,
    /// because one `.sql` file is missing between the moves. The readers of a metadata file that do not take `mutex` hold it shared.
    /// A swap takes it while it holds `mutex`, so a reader must not take `mutex` while it holds this mutex.
    mutable SharedMutex metadata_files_mutex;

    /// Exchanges the two metadata files of `record` in this database with three moves, instead of an atomic exchange,
    /// and then removes the record at `record_path` that `writeMetadataExchangeRecord` wrote.
    /// The caller holds `mutex` and the DDL guards of `table_name` and `other_table_name`.
    void exchangeMetadataFilesWithRecord(
        const String & table_name,
        const String & other_table_name,
        const DiskPtr & disk,
        const MetadataExchangeRecord & record,
        const String & record_path,
        bool fsync) TSA_REQUIRES(mutex);
};

}
