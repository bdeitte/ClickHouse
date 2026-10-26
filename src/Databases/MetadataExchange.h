#pragma once

#include <Core/Types.h>
#include <Disks/IDisk.h>
#include <Common/Logger.h>

#include <optional>


namespace DB
{

/// An exchange of two metadata files on a disk without an atomic exchange is three moves:
/// `old_path` to the temporary file `old_path + ".tmp_exchange"`, `new_path` to `old_path`, and the temporary file to `new_path`.
/// Before the moves, a record of the exchange is written next to `old_path`, so that loading the database can finish
/// an interrupted exchange.

/// The two metadata files of an exchange and what each of them holds after it. Paths are relative to the disk.
struct MetadataExchangeRecord
{
    String old_path;
    String new_path;
    String old_path_content;
    String new_path_content;
};

/// `dir/a.sql` -> `dir/.a.sql.exchange`: listings of metadata directories skip names starting with a dot.
String metadataExchangeRecordPath(const String & metadata_file_path);

/// The suffix of the temporary file of an exchange.
inline constexpr auto metadata_exchange_tmp_suffix = ".tmp_exchange";

/// The temporary file of an exchange: `old_path + ".tmp_exchange"`.
String metadataExchangeTmpPath(const String & old_path);

/// The header line `exchange v1`, then the two paths and the two contents, each escaped and on its own line.
String serializeMetadataExchangeRecord(const MetadataExchangeRecord & record);

/// Returns nothing when the content is not a record.
std::optional<MetadataExchangeRecord> parseMetadataExchangeRecord(const String & content);

/// Writes the record next to `record.old_path`, as `<record>.tmp` that is then put in place with `replaceFile`.
/// Returns the path of the record.
String writeMetadataExchangeRecord(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);

/// Best effort: a record without its temporary file is harmless, and the next load removes it.
/// A removal is logged at INFO when the load removes files a crash left (`found_at_load`),
/// and at DEBUG for the routine cleanup after an exchange.
void tryRemoveMetadataExchangeFiles(const DiskPtr & disk, const Strings & paths, const LoggerPtr & log, bool found_at_load) noexcept;

}
