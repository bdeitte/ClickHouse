#include <Databases/MetadataExchange.h>

#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <filesystem>
#include <mutex>


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

std::mutex & metadataExchangeMutex()
{
    static std::mutex mutex;
    return mutex;
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

Strings writeMetadataExchangeRecords(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync)
{
    const String content = serializeMetadataExchangeRecord(record);

    /// A copy in the other database's directory, in case that database is loaded first.
    Strings record_paths{metadataExchangeRecordPath(record.old_path)};
    if (fs::path(record.old_path).parent_path() != fs::path(record.new_path).parent_path())
        record_paths.push_back(metadataExchangeRecordPath(record.new_path));

    for (const auto & path : record_paths)
        writeMetadataFile(disk, path + ".tmp", content, fsync);
    for (const auto & path : record_paths)
        disk->replaceFile(path + ".tmp", path);
    return record_paths;
}

void tryRemoveMetadataExchangeRecords(const DiskPtr & disk, const Strings & record_paths, const LoggerPtr & log, bool found_at_load) noexcept
{
    for (const auto & path : record_paths)
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
    /// The directory is scanned without the mutex: databases load in parallel, and most have no records,
    /// so a global mutex around every listing would serialize loading for nothing. No exchange writes records
    /// into this directory meanwhile: a record is written only after both tables of the exchange are resolved,
    /// and this load has not attached any table of this database yet. A load other than at server startup also
    /// needs a prior `DETACH DATABASE`, which takes the database mutex that a live exchange holds, and a fenced
    /// database refuses to detach, so kept records are only scanned again after a restart. The exchange writer
    /// already relies on this ordering, not on the mutex: it releases the mutex between writing its records and
    /// moving the files, for the Keeper commit.
    const String record_suffix = String(".sql") + exchange_record_suffix;
    const String record_tmp_suffix = record_suffix + ".tmp";
    const String write_suffix = String(".sql") + exchange_write_suffix;
    Strings record_paths;
    Strings leftover_paths;
    for (const auto it = disk->iterateDirectory(metadata_dir); it->isValid(); it->next())
    {
        const String file_name = it->name();
        if (!file_name.starts_with('.'))
            continue;
        const String path = (fs::path(metadata_dir) / file_name).string();
        if (file_name.ends_with(record_suffix))
            record_paths.push_back(path);
        else if (file_name.ends_with(record_tmp_suffix) || file_name.ends_with(write_suffix)
                 || file_name.starts_with(metadata_exchange_probe_prefix))
            leftover_paths.push_back(path);
    }

    if (record_paths.empty() && leftover_paths.empty())
        return;

    /// The two databases of an exchange between databases can be loaded at the same time, and both may act on its record.
    /// Each record found above is read again under the mutex, so a record the other database finished is skipped.
    std::lock_guard lock(metadataExchangeMutex());

    /// A record is complete before its exchange begins, so a leftover temporary record belongs to an exchange that did not begin.
    /// A leftover write of a finish belongs to an exchange whose temporary file is still there, so finishing it below writes it again.
    /// No probe runs while its database loads, so a probe file is a leftover of a crashed probe.
    tryRemoveMetadataExchangeRecords(disk, leftover_paths, log, /* found_at_load */ true);

    for (const auto & record_path : record_paths)
    {
        const auto content = readMetadataFileIfExists(disk, record_path);
        if (!content)
            continue;

        const auto record = parseMetadataExchangeRecord(*content);
        if (!record
            || (record_path != metadataExchangeRecordPath(record->old_path) && record_path != metadataExchangeRecordPath(record->new_path)))
        {
            LOG_WARNING(log, "{} is not an exchange record, leaving it as is", record_path);
            continue;
        }

        /// A newer exchange with the same `old_path` overwrites the primary, so a copy is acted on only while the primary is the same.
        const String primary_path = metadataExchangeRecordPath(record->old_path);
        if (record_path != primary_path && readMetadataFileIfExists(disk, primary_path) != content)
        {
            tryRemoveMetadataExchangeRecords(disk, {record_path}, log, /* found_at_load */ true);
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
            LOG_WARNING(log, "Finished the interrupted exchange of {} and {}", record->old_path, record->new_path);
        }

        Strings finished_record_paths{primary_path};
        const String copy_path = metadataExchangeRecordPath(record->new_path);
        if (copy_path != primary_path && readMetadataFileIfExists(disk, copy_path) == content)
            finished_record_paths.push_back(copy_path);
        tryRemoveMetadataExchangeRecords(disk, finished_record_paths, log, /* found_at_load */ true);
    }
}

}
