#pragma once

#include <Core/Types.h>
#include <Disks/IDisk.h>
#include <Common/Logger.h>

#include <optional>


namespace DB
{

/// An exchange of two metadata files on a disk without an atomic exchange is three moves:
/// `old_path` to the temporary file `old_path + ".tmp_exchange"`, `new_path` to `old_path`, and the temporary file to `new_path`.
/// Before the moves, a record of the exchange is written next to `old_path`, so that loading the database can undo
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

/// Syncs the directory of the file `file_path` to the storage device, so that the renames and removals of files in it are durable.
void syncMetadataExchangeDirectory(const DiskPtr & disk, const String & file_path);

/// Writes the record next to `record.old_path`, as `<record>.tmp` that is then put in place with `replaceFile`.
/// With `fsync`, it also syncs the directory, so that the record is durable before the first move. Returns the path of the record.
String writeMetadataExchangeRecord(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);

/// Removes leftover temporary records and writes of a finish or an undo, which a crash left. Best effort: they are harmless,
/// and the next load removes them again.
void tryRemoveLeftoverMetadataExchangeFiles(const DiskPtr & disk, const Strings & paths, const LoggerPtr & log) noexcept;

/// Whether a file in a metadata directory is one that `undoInterruptedMetadataExchanges` acts on: a record, a leftover
/// temporary record, a leftover write of a finish or an undo, or the temporary file of an exchange.
bool isMetadataExchangeFileName(const String & file_name);

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
/// For an exchange that a move interrupted with an error, after which the server applies the exchange in memory.
void finishMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);

/// Like `finishMetadataExchange`, but writes the contents from before the exchange: `new_path_content` to `old_path`,
/// and `old_path_content` to `new_path`. Without the temporary file, it first writes the temporary file, so that a crash
/// during the undo leaves the exchange `Interrupted`.
void undoMetadataExchange(const DiskPtr & disk, const MetadataExchangeRecord & record, bool fsync);

/// Called when a database on `disk` is loaded. Removes leftover temporary records and writes of a finish or an undo in
/// `metadata_dir`, undoes the exchanges recorded there whose files are not as before them, and removes their records.
/// Throws `CORRUPTED_DATA` and leaves the files as they are when the files of an exchange hold contents it did not record.
void undoInterruptedMetadataExchanges(const DiskPtr & disk, const String & metadata_dir, bool fsync, const LoggerPtr & log);

/// Called instead of `undoInterruptedMetadataExchanges` when a database on a read-only `disk` is loaded. Throws
/// `CORRUPTED_DATA`, as `undoInterruptedMetadataExchanges` does, when the files of an exchange recorded in `metadata_dir` hold
/// contents it did not record. Otherwise throws `INCORRECT_FILE_NAME` when such an exchange would need an undo. Changes no file.
void checkNoMetadataExchangeToUndo(const DiskPtr & disk, const String & metadata_dir, const LoggerPtr & log);

}
