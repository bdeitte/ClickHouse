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
}

namespace
{

constexpr auto exchange_record_suffix = ".exchange";
constexpr auto exchange_record_header = "exchange v1";

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

}
