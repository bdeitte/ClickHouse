#pragma once

#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Storages/IStorage_fwd.h>
#include <Common/SharedMutex.h>

#include <atomic>
#include <map>


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

    /// Throws `UNFINISHED_METADATA_EXCHANGE` while an exchange of the metadata file of `table_name` could not be finished,
    /// or, with an empty `table_name`, while this is so for any table of the database. See `unfinished_exchanges`.
    void checkNoUnfinishedExchange(const String & table_name) const override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void tryCreateSymlink(const StoragePtr & table, bool if_data_path_exist = false);
    void tryRemoveSymlink(const String & table_name);

    void waitDetachedTableNotInUse(const UUID & uuid, std::function<void()> throw_if_cancelled) override;
    void checkDetachedTableNotInUse(const UUID & uuid) override;
    void setDetachedTableNotInUseForce(const UUID & uuid) override;

    /// Waits for the moves of a swap with a record, see `metadata_files_mutex`.
    time_t getObjectMetadataModificationTime(const String & object_name) const override;

    /// Refuses the tables of an unfinished exchange, see `unfinished_exchanges`, instead of leaving them out of the backup.
    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const override;

protected:
    /// Undoes a swap that a crash or a fence interrupted, before `iterateMetadataFiles` processes any metadata file.
    bool undoInterruptedMetadataExchangesAtLoad() const override;

    /// Waits for the moves of a swap with a record, see `metadata_files_mutex`. Refuses the tables of an unfinished exchange,
    /// or returns `nullptr` for them when `throw_on_error` is `false`.
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

    /// Writes `record` with `writeMetadataExchangeRecord`, exchanges the two metadata files of `record` in this database with three
    /// moves, instead of an atomic exchange, and then removes the record. The caller renames the tables in memory only after this
    /// returns, so a record on disk means that its exchange did not reach the tables in memory, and the next load undoes it, see
    /// `undoInterruptedMetadataExchanges`. When a move fails and the exchange can be neither retried nor finished from the record,
    /// or when the record cannot be removed, it leaves the record and fences the two tables, see `fenceUnfinishedExchange`.
    /// The caller holds `mutex` and the DDL guards of `table_name` and `other_table_name`.
    void exchangeMetadataFilesWithRecord(
        const String & table_name,
        const String & other_table_name,
        const DiskPtr & disk,
        const MetadataExchangeRecord & record,
        bool fsync) TSA_REQUIRES(mutex);

    /// Fences an exchange that failed and could not be finished: adds `table_name` and `other_table_name` to `unfinished_exchanges`,
    /// then rethrows `error`. `hint` tells what the restart does about the metadata files `files` and where the rows inserted
    /// until then end up. This function adds what the fence refuses and accepts until then, and adds the result to `error` and to
    /// the error it logs. The queries that the fence refuses later get a short error that refers to these two.
    [[noreturn]] void fenceUnfinishedExchange(
        const String & table_name,
        const String & other_table_name,
        const String & files,
        String hint,
        std::exception_ptr error) TSA_REQUIRES(mutex);

    /// Adds an entry to `system.warnings` about the fence that the exchange of `table_name` and `other_table_name` has just set,
    /// so that it is seen by more than the query that hit it. Releases `lock` of `mutex` first. Does not throw, so that the caller
    /// can rethrow the error of the exchange.
    void warnAboutUnfinishedExchange(
        std::unique_lock<std::mutex> & lock, const String & table_name, const String & other_table_name) const noexcept;

    /// Throws `UNFINISHED_METADATA_EXCHANGE` while an exchange of the metadata file of `table_name` could not be finished.
    void assertTableHasNoUnfinishedExchangeUnlocked(const String & table_name) const TSA_REQUIRES(mutex);

    /// Set when an exchange of two metadata files with a record failed and could not be finished, so the files of its two tables
    /// may be half exchanged. By table name, the two metadata files of the exchange, for the errors. Until the server restarts, DDL on the two tables and reads of their definitions fail,
    /// and so do `DROP DATABASE`, `DETACH DATABASE` and `TRUNCATE DATABASE`. The record of the exchange is left, and the next load
    /// undoes the exchange from it, or reports `CORRUPTED_DATA` when the files hold none of the recorded contents. Inserts are not
    /// refused: until the restart, they reach the tables as they were before the exchange.
    std::map<String, String> unfinished_exchanges TSA_GUARDED_BY(mutex);

    /// Whether `unfinished_exchanges` is not empty. Set by `fenceUnfinishedExchange` and never cleared, like the fence. While it is
    /// `false`, `readCreateQueryFromMetadataFile`, `getTablesForBackup` and `checkNoUnfinishedExchange` for a table do not lock
    /// `mutex`, so that the readers of definitions and the DDL guards of a database without a fence do not contend on it.
    /// The commit points still check under `mutex`.
    ///
    /// It is set while the exchange holds `mutex`, `metadata_files_mutex` exclusively and the DDL guards of both names. So a reader
    /// that then takes `metadata_files_mutex` shared, or a DDL that then takes the guard of either name, sees it: the unlock by the
    /// exchange is ordered before that lock, and the store before the unlock. A reader of a definition also takes `mutex` in
    /// `tryGetTable` before its first check (`DatabaseOnDisk::getCreateTableQueryImpl`), so it sees a fence set before. A reader
    /// whose first check runs while a failing exchange holds `mutex` can miss the fence there, and then fails on the missing file
    /// or sees the fence in its second check. `getTablesForBackup` checks after its reads, and a read leaves a table out because
    /// of the fence only after the same thread saw `true`. `checkNoUnfinishedExchange` for the whole database holds no lock that
    /// the exchange takes, so it always locks `mutex`. The flag publishes nothing itself, because a check that sees `true` reads
    /// `unfinished_exchanges` under `mutex`, and a load ordered after a store sees it whatever its memory order, so
    /// `std::memory_order_relaxed` is enough.
    std::atomic<bool> has_unfinished_exchange{false};
};

}
