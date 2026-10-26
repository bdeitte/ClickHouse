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
/// so a roll forward during a concurrent `ALTER` of the same table could put the `ALTER`'s content in place of the recorded one.
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

String writeMetadataExchangeRecord(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync)
{
    const String record_path = metadataExchangeRecordPath(record.old_path);
    writeMetadataFile(disk, record_path + ".tmp", serializeMetadataExchangeRecord(record), fsync);
    disk->replaceFile(record_path + ".tmp", record_path);
    return record_path;
}

void tryRemoveMetadataExchangeFiles(const DiskPtr & disk, const Strings & paths, const LoggerPtr & log, bool found_at_load) noexcept
{
    for (const auto & path : paths)
    {
        try
        {
            if (!disk->existsFile(path))
                continue;
            if (found_at_load)
                LOG_INFO(log, "Removing file {}", path);
            else
                LOG_DEBUG(log, "Removing file {}", path);
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

void finishMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync)
{
    replaceMetadataFile(disk, record.old_path, record.old_path_content, fsync);
    replaceMetadataFile(disk, record.new_path, record.new_path_content, fsync);
    /// Last, so that a crash before it leaves the exchange `Interrupted` and the next load finishes it again.
    disk->removeFileIfExists(metadataExchangeTmpPath(record.old_path));
}

void finishInterruptedMetadataExchanges(const DiskPtr & disk, const String & metadata_dir, bool fsync, const LoggerPtr & log)
{
    /// The directory is scanned without a mutex: a database's own load cannot overlap its swaps.
    /// At server start, the load runs before the server accepts queries. `ATTACH DATABASE` holds the exclusive DDL guard
    /// of the database while it loads, and every table DDL guard takes that guard's mutex in shared mode first.
    /// `metadata_dir` ends with a slash, and `parent_path` of a file in it does not.
    const fs::path dir = (fs::path(metadata_dir) / "x").parent_path();
    Strings record_paths;
    Strings leftover_paths;
    for (const auto it = disk->iterateDirectory(metadata_dir); it->isValid(); it->next())
    {
        const String file_name = it->name();
        if (!file_name.starts_with('.'))
            continue;
        const String path = (fs::path(metadata_dir) / file_name).string();
        if (file_name.ends_with(sql_exchange_record_suffix))
            record_paths.push_back(path);
        else if (file_name.ends_with(sql_exchange_record_tmp_suffix) || file_name.ends_with(sql_exchange_write_suffix))
            leftover_paths.push_back(path);
    }

    /// A record is complete before its exchange begins, so a leftover temporary record belongs to an exchange that did not begin.
    /// A leftover write of a finish belongs to an exchange whose temporary file is still there, so finishing it below writes it again.
    tryRemoveMetadataExchangeFiles(disk, leftover_paths, log, /* found_at_load */ true);

    for (const auto & record_path : record_paths)
    {
        const auto content = readMetadataFileIfExists(disk, record_path);
        if (!content)
            continue;

        const auto record = parseMetadataExchangeRecord(*content);
        if (!record
            || record_path != metadataExchangeRecordPath(record->old_path)
            || fs::path(record->old_path).parent_path() != dir
            || fs::path(record->new_path).parent_path() != dir)
        {
            LOG_WARNING(log, "{} is not an exchange record, leaving it as is", record_path);
            continue;
        }

        /// Without the temporary file, the moves either did not begin or all finished, and the record only has to be removed.
        /// The files are not compared with the record here: they can legitimately hold neither recorded state,
        /// for example after an `ALTER` that followed an exchange whose record was not removed.
        if (disk->existsFile(metadataExchangeTmpPath(record->old_path)))
        {
            if (inspectMetadataExchange(disk, *record) != MetadataExchangeState::Interrupted)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Cannot finish the interrupted exchange of {} and {} recorded in {}: the temporary file {} or one of the two files "
                    "holds none of the recorded contents. The files are left as they are. To repair manually, compare the two files "
                    "and the temporary file with the two contents in the record, restore the intended content of each of the two "
                    "files, remove the record and the temporary file, and load the database again",
                    record->old_path, record->new_path, record_path, metadataExchangeTmpPath(record->old_path));

            finishMetadataExchange(disk, *record, fsync);
            LOG_WARNING(
                log, "Finished the interrupted exchange of {} and {}, and removed the temporary file {}",
                record->old_path, record->new_path, metadataExchangeTmpPath(record->old_path));
        }

        tryRemoveMetadataExchangeFiles(disk, {record_path}, log, /* found_at_load */ true);
    }
}

}
