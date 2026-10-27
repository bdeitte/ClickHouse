#include <Databases/MetadataExchange.h>

#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CORRUPTED_DATA;
    extern const int INCORRECT_FILE_NAME;
}

namespace
{

constexpr auto exchange_record_suffix = ".exchange";
constexpr auto exchange_record_header = "exchange v1";
constexpr auto exchange_write_suffix = ".exchange_write";

/// The ends of the names of the files of an exchange of the metadata file `t.sql` of a table. Constants, so that matching
/// the name of every file of every database at load builds no strings.
constexpr std::string_view sql_exchange_record_suffix = ".sql.exchange";
constexpr std::string_view sql_exchange_record_tmp_suffix = ".sql.exchange.tmp";
constexpr std::string_view sql_exchange_write_suffix = ".sql.exchange_write";
constexpr std::string_view sql_exchange_tmp_suffix = ".sql.tmp_exchange";
static_assert(sql_exchange_record_suffix.ends_with(exchange_record_suffix));
static_assert(sql_exchange_record_tmp_suffix.starts_with(sql_exchange_record_suffix));
static_assert(sql_exchange_write_suffix.ends_with(exchange_write_suffix));
static_assert(sql_exchange_tmp_suffix.ends_with(metadata_exchange_tmp_suffix));

std::optional<String> readMetadataFileIfExists(const DiskPtr & disk, const String & path)
{
    if (!disk->existsFile(path))
        return {};
    return readMetadataFile(disk, path);
}

/// `dir/a.sql` -> `dir/.a.sql.exchange_write`. Not `a.sql.tmp`, which `ALTER` writes before it takes the database mutex,
/// so a finish after an error during a concurrent `ALTER` of the same table could put the `ALTER`'s content in place of the recorded one.
String metadataExchangeWritePath(const String & metadata_file_path)
{
    fs::path path(metadata_file_path);
    return (path.parent_path() / ("." + path.filename().string() + exchange_write_suffix)).string();
}

void replaceMetadataFile(const DiskPtr & disk, const String & path, const String & content, bool fsync)
{
    const String write_path = metadataExchangeWritePath(path);
    writeMetadataFile(disk, write_path, content, fsync);
    disk->replaceFile(write_path, path);
}

/// Writes `old_path_content` to the file `record.old_path` and `new_path_content` to `record.new_path`,
/// then removes the temporary file of the exchange.
void writeMetadataExchangeFiles(
    const DiskPtr & disk,
    const MetadataExchangeRecord & record,
    const String & old_path_content,
    const String & new_path_content,
    bool fsync,
    const LoggerPtr & log)
{
    replaceMetadataFile(disk, record.old_path, old_path_content, fsync);
    replaceMetadataFile(disk, record.new_path, new_path_content, fsync);
    /// Last, so that a crash before it leaves the exchange `Interrupted`, and the next load undoes it.
    const String tmp_path = metadataExchangeTmpPath(record.old_path);
    LOG_INFO(log, "Removing file {}", tmp_path);
    disk->removeFileIfExists(tmp_path);
}

}

bool isMetadataExchangeFileName(const String & file_name)
{
    if (!file_name.starts_with('.'))
        return file_name.ends_with(sql_exchange_tmp_suffix);
    return file_name.ends_with(sql_exchange_record_suffix) || file_name.ends_with(sql_exchange_record_tmp_suffix)
        || file_name.ends_with(sql_exchange_write_suffix);
}

String metadataExchangeRecordPath(const String & metadata_file_path)
{
    fs::path path(metadata_file_path);
    return (path.parent_path() / ("." + path.filename().string() + exchange_record_suffix)).string();
}

String metadataExchangeTmpPath(const String & old_path)
{
    return old_path + metadata_exchange_tmp_suffix;
}

String serializeMetadataExchangeRecord(const MetadataExchangeRecord & record)
{
    WriteBufferFromOwnString out;
    writeString(exchange_record_header, out);
    writeChar('\n', out);
    for (const auto * field : {&record.old_path, &record.new_path, &record.old_path_content, &record.new_path_content})
    {
        writeEscapedString(*field, out);
        writeChar('\n', out);
    }
    return out.str();
}

std::optional<MetadataExchangeRecord> parseMetadataExchangeRecord(const String & content)
{
    ReadBufferFromString in(content);
    MetadataExchangeRecord record;
    try
    {
        if (!checkString(exchange_record_header, in) || !checkChar('\n', in))
            return {};
        for (auto * field : {&record.old_path, &record.new_path, &record.old_path_content, &record.new_path_content})
        {
            readEscapedString(*field, in);
            if (!checkChar('\n', in))
                return {};
        }
    }
    catch (const Exception & e)
    {
        /// Malformed escaping is the only error `readEscapedString` throws on malformed input: the content is not a record.
        if (e.code() != ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE)
            throw;
        return {};
    }
    if (!in.eof() || record.old_path.empty() || record.new_path.empty())
        return {};
    return record;
}

void syncMetadataExchangeDirectory(const DiskPtr & disk, const String & file_path)
{
    if (auto guard = disk->getDirectorySyncGuard(fs::path(file_path).parent_path()))
        guard->sync();
}

String writeMetadataExchangeRecord(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync)
{
    const String record_path = metadataExchangeRecordPath(record.old_path);
    writeMetadataFile(disk, record_path + ".tmp", serializeMetadataExchangeRecord(record), fsync);
    disk->replaceFile(record_path + ".tmp", record_path);
    /// Durable before the first move, which a crash could otherwise leave on disk without the record.
    if (fsync)
        syncMetadataExchangeDirectory(disk, record_path);
    return record_path;
}

void tryRemoveLeftoverMetadataExchangeFiles(const DiskPtr & disk, const Strings & paths, const LoggerPtr & log) noexcept
{
    for (const auto & path : paths)
    {
        try
        {
            if (!disk->existsFile(path))
                continue;
            LOG_INFO(log, "Removing file {}", path);
            disk->removeFileIfExists(path);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Cannot remove {}", path));
        }
    }
}

MetadataExchangeState inspectMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record)
{
    const auto old_path_current = readMetadataFileIfExists(disk, record.old_path);
    const auto new_path_current = readMetadataFileIfExists(disk, record.new_path);

    /// The temporary file exists only between the first and the last move, and holds what `new_path` holds after the exchange.
    if (const auto tmp_current = readMetadataFileIfExists(disk, metadataExchangeTmpPath(record.old_path)))
    {
        auto is_absent_or_recorded = [&](const std::optional<String> & current)
        {
            return !current || *current == record.old_path_content || *current == record.new_path_content;
        };
        if (*tmp_current == record.new_path_content && is_absent_or_recorded(old_path_current) && is_absent_or_recorded(new_path_current))
            return MetadataExchangeState::Interrupted;
        return MetadataExchangeState::Inconsistent;
    }

    if (old_path_current == record.old_path_content && new_path_current == record.new_path_content)
        return MetadataExchangeState::AfterExchange;
    if (old_path_current == record.new_path_content && new_path_current == record.old_path_content)
        return MetadataExchangeState::BeforeExchange;
    return MetadataExchangeState::Inconsistent;
}

void finishMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync, const LoggerPtr & log)
{
    writeMetadataExchangeFiles(disk, record, record.old_path_content, record.new_path_content, fsync, log);
}

void undoMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync, const LoggerPtr & log)
{
    /// Without the temporary file, the writes below pass through a state where both files hold the same content, which a crash
    /// would leave `Inconsistent`. The temporary file, with the content it has during the exchange, keeps every state until the
    /// undo ends `Interrupted`, so the next load undoes the exchange again.
    const String tmp_path = metadataExchangeTmpPath(record.old_path);
    if (!disk->existsFile(tmp_path))
    {
        const String write_path = metadataExchangeWritePath(record.old_path);
        writeMetadataFile(disk, write_path, record.new_path_content, fsync);
        disk->replaceFile(write_path, tmp_path);
    }
    writeMetadataExchangeFiles(disk, record, record.new_path_content, record.old_path_content, fsync, log);
}

namespace
{

struct MetadataExchangeFiles
{
    /// The records of exchanges.
    Strings record_paths;
    /// Leftover temporary records and writes of a finish or an undo.
    Strings leftover_paths;
};

/// The directory is scanned without a mutex: a database's own load cannot overlap its swaps.
/// At server start, the load runs before the server accepts queries. `ATTACH DATABASE` holds the exclusive DDL guard
/// of the database while it loads, and every table DDL guard takes that guard's mutex in shared mode first.
MetadataExchangeFiles listMetadataExchangeFiles(const DiskPtr & disk, const String & metadata_dir)
{
    MetadataExchangeFiles files;
    for (const auto it = disk->iterateDirectory(metadata_dir); it->isValid(); it->next())
    {
        const String file_name = it->name();
        if (!file_name.starts_with('.'))
            continue;
        const String path = (fs::path(metadata_dir) / file_name).string();
        if (file_name.ends_with(sql_exchange_record_suffix))
            files.record_paths.push_back(path);
        else if (file_name.ends_with(sql_exchange_record_tmp_suffix) || file_name.ends_with(sql_exchange_write_suffix))
            files.leftover_paths.push_back(path);
    }
    return files;
}

/// Reads the record `record_path` in `metadata_dir`. Returns nothing when the file is gone, or, with a warning, when it is
/// not a record of an exchange of two files in `metadata_dir`.
std::optional<MetadataExchangeRecord> readMetadataExchangeRecord(
    const DiskPtr & disk, const String & record_path, const String & metadata_dir, const LoggerPtr & log)
{
    const auto content = readMetadataFileIfExists(disk, record_path);
    if (!content)
        return {};

    /// `metadata_dir` ends with a slash, and `parent_path` of a file in it does not.
    const fs::path dir = (fs::path(metadata_dir) / "x").parent_path();
    auto record = parseMetadataExchangeRecord(*content);
    if (!record
        || record_path != metadataExchangeRecordPath(record->old_path)
        || fs::path(record->old_path).parent_path() != dir
        || fs::path(record->new_path).parent_path() != dir)
    {
        LOG_WARNING(log, "{} is not an exchange record, leaving it as is", record_path);
        return {};
    }
    return record;
}

/// For an exchange in the state `Inconsistent`, which no load can undo, so the files need a repair by hand.
/// A record is left only by a crash during its exchange, by a fence, or by an exchange that could not remove it, which is
/// fenced too. Until the restart, the crash or the fence kept DDL from changing the two files, so a content that is not
/// recorded comes from outside the server.
[[noreturn]] void throwCannotUndoMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, const String & record_path)
{
    const String tmp_path = metadataExchangeTmpPath(record.old_path);
    const bool tmp_exists = disk->existsFile(tmp_path);
    throw Exception(
        ErrorCodes::CORRUPTED_DATA,
        "Cannot undo the exchange of {} and {} recorded in {}: {} holds none of the recorded contents. The files are left "
        "as they are. To repair manually, compare the two files{} with the two contents in the record, restore the "
        "intended content of each of the two files, remove the record{}, and load the database again",
        record.old_path, record.new_path, record_path,
        tmp_exists ? fmt::format("the temporary file {} or one of the two files", tmp_path) : "one of the two files",
        tmp_exists ? " and the temporary file" : "",
        tmp_exists ? " and the temporary file" : "");
}

}

void checkNoMetadataExchangeToUndo(const DiskPtr & disk, const String & metadata_dir, const LoggerPtr & log)
{
    /// A record means that the server did not apply its exchange, see `undoInterruptedMetadataExchanges`. A load that cannot
    /// write cannot undo the exchange, so it loads the two files only while they hold their contents from before it.
    /// Every record is inspected before the advice to load from a writable disk, because that load would fail on an
    /// exchange whose files hold contents it did not record, wherever its record is in the listing.
    std::optional<std::pair<MetadataExchangeRecord, String>> to_undo;
    for (const auto & record_path : listMetadataExchangeFiles(disk, metadata_dir).record_paths)
    {
        auto record = readMetadataExchangeRecord(disk, record_path, metadata_dir, log);
        if (!record)
            continue;
        const auto state = inspectMetadataExchange(disk, *record);
        if (state == MetadataExchangeState::BeforeExchange)
            continue;
        if (state == MetadataExchangeState::Inconsistent)
            throwCannotUndoMetadataExchange(disk, *record, record_path);
        if (!to_undo)
            to_undo.emplace(std::move(*record), record_path);
    }

    if (to_undo)
    {
        const auto & [record, record_path] = *to_undo;
        throw Exception(
            ErrorCodes::INCORRECT_FILE_NAME,
            "Cannot load the metadata in {}: the exchange of {} and {} recorded in {} was not applied by the server, and the disk "
            "is read-only, so the load cannot undo it. Load the database once from a writable disk, which undoes the exchange",
            metadata_dir, record.old_path, record.new_path, record_path);
    }
}

void undoInterruptedMetadataExchanges(const DiskPtr & disk, const String & metadata_dir, bool fsync, const LoggerPtr & log)
{
    const auto [record_paths, leftover_paths] = listMetadataExchangeFiles(disk, metadata_dir);

    /// A record is complete before its exchange begins, so a leftover temporary record belongs to an exchange that did not begin.
    /// A leftover write of a finish or of an undo belongs to an exchange whose temporary file is still there, so undoing it below
    /// writes the file again.
    tryRemoveLeftoverMetadataExchangeFiles(disk, leftover_paths, log);

    bool removed_records = false;
    for (const auto & record_path : record_paths)
    {
        const auto record = readMetadataExchangeRecord(disk, record_path, metadata_dir, log);
        if (!record)
            continue;

        /// A record means that its exchange did not reach the tables in memory: an exchange removes its record, and checks that
        /// it is gone, before it renames the tables in memory, and when it cannot, it fences the two tables and leaves the record.
        /// So the query that ran the exchange did not succeed. After a crash, its client got no result. After an exchange that
        /// was fenced, the server served the two tables under their old names until the restart, and inserts reached them there.
        /// So the exchange is undone, not finished, which keeps each table under the name the server served it with. That holds
        /// with or without the temporary file: the moves or a finish may have completed before the crash or the fence.
        /// This needs `fsync_metadata`. Without it, a crash can lose the removal of the record of an exchange that succeeded,
        /// and this undoes that exchange. `DatabaseAtomic::renameTable` warns about such an exchange.
        switch (inspectMetadataExchange(disk, *record))
        {
            case MetadataExchangeState::BeforeExchange:
                break;
            case MetadataExchangeState::Interrupted:
            case MetadataExchangeState::AfterExchange:
                undoMetadataExchange(disk, *record, fsync, log);
                /// Durable before the record is removed below. Otherwise a crash could keep the removal and lose the undo.
                if (fsync)
                    syncMetadataExchangeDirectory(disk, record_path);
                LOG_WARNING(
                    log, "Undid the interrupted exchange of {} and {} recorded in {}: restored the contents the two files had before it",
                    record->old_path, record->new_path, record_path);
                break;
            case MetadataExchangeState::Inconsistent:
                throwCannotUndoMetadataExchange(disk, *record, record_path);
        }

        /// Not best effort: a record left after the load would make a later load refuse the database once DDL changes either
        /// file, so the load fails instead.
        LOG_INFO(log, "Removing file {}", record_path);
        disk->removeFileIfExists(record_path);
        removed_records = true;
    }

    /// For the same reason, the removals are durable before the database loads and DDL can change the files. All records are
    /// in `metadata_dir`, so the first one gives the directory to sync.
    if (fsync && removed_records)
        syncMetadataExchangeDirectory(disk, record_paths.front());
}

}
