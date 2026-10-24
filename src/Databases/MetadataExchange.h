#pragma once

#include <Core/Types.h>
#include <Disks/IDisk.h>
#include <Common/Logger.h>

#include <mutex>
#include <optional>


namespace DB
{

/// An exchange of two metadata files on a disk without an atomic exchange is three moves:
/// `old_path` to the temporary file `old_path + ".tmp_exchange"`, `new_path` to `old_path`, and the temporary file to `new_path`.
/// Before the moves, a record of the exchange is written next to `old_path` (the primary), plus a copy next to `new_path`
/// when the two files are in different directories, so that loading either database can finish an interrupted exchange.

/// The two metadata files of an exchange and what each of them holds after it. Paths are relative to the disk.
struct MetadataExchangeRecord
{
    String old_path;
    String new_path;
    String old_path_content;
    String new_path_content;
};

/// Held both by the loader in `finishInterruptedMetadataExchanges` and by the exchange writer, so that neither sees the other's records half done.
/// It is one mutex for the whole server, held across the moves of an exchange and the handling of their failure, so a move that hangs
/// on the filesystem blocks every other exchange with a record and every load that finds records. Such exchanges are rare,
/// and loads find records only after a crash or an exchange that could not be finished, so one mutex is kept simple on purpose.
std::mutex & metadataExchangeMutex();

/// `dir/a.sql` -> `dir/.a.sql.exchange`: listings of metadata directories skip names starting with a dot.
String metadataExchangeRecordPath(const String & metadata_file_path);

/// The temporary file of an exchange: `old_path + ".tmp_exchange"`.
String metadataExchangeTmpPath(const String & old_path);

/// The prefix of the scratch files that check whether a directory supports an atomic exchange.
inline constexpr auto metadata_exchange_probe_prefix = ".exchange_probe_";

/// The header line `exchange v1`, then the two paths and the two contents, each escaped and on its own line.
String serializeMetadataExchangeRecord(const MetadataExchangeRecord & record);

/// Returns nothing when the content is not a record.
std::optional<MetadataExchangeRecord> parseMetadataExchangeRecord(const String & content);

/// Writes the primary record and, when the parent directories differ, its copy.
/// Each is written as `<record>.tmp` and then put in place with `replaceFile`. Returns the paths written, primary first.
Strings writeMetadataExchangeRecords(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);

/// Best effort: a record without its temporary file is harmless, and the next load removes it.
/// A removal is logged at INFO when the load removes files a crash left (`found_at_load`),
/// and at DEBUG for the routine cleanup after an exchange or a probe.
void tryRemoveMetadataExchangeRecords(const DiskPtr & disk, const Strings & record_paths, const LoggerPtr & log, bool found_at_load) noexcept;

enum class MetadataExchangeState : uint8_t
{
    /// No temporary file, and both files hold their contents from before the exchange.
    BeforeExchange,
    /// The temporary file holds the content recorded for `new_path`, and each of the two files is absent
    /// or holds one of the two recorded contents.
    Interrupted,
    /// No temporary file, and both files hold their contents from after the exchange.
    AfterExchange,
    /// Anything else, including a missing file without the temporary file.
    Inconsistent,
};

MetadataExchangeState inspectMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record);

/// Writes each recorded content to `dir/.<file>.exchange_write` and puts it in place with `replaceFile`, then removes the temporary file.
void finishMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);

/// Called when a database on `disk` is loaded. Removes leftover temporary records, writes of a finish and probe files in `metadata_dir`,
/// finishes the interrupted exchanges recorded there, and removes their records.
/// Throws `CORRUPTED_DATA` and leaves the files as they are when an exchange cannot be finished safely.
void finishInterruptedMetadataExchanges(const DiskPtr & disk, const String & metadata_dir, bool fsync, const LoggerPtr & log);

}
