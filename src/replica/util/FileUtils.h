/*
 * LSST Data Management System
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_REPLICA_FILEUTILS_H
#define LSST_QSERV_REPLICA_FILEUTILS_H

// System headers
#include <cstdio>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

// Forward declarations
namespace lsst::qserv::replica {
class DatabaseInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * The utility class encapsulating various operations with file systems.
 *
 * @note
 *   This class can't be instantiated
 */
class FileUtils {
public:
    /// The default number of bytes to be read during file I/O operations
    static constexpr size_t DEFAULT_RECORD_SIZE_BYTES = 1024 * 1024;

    /// The maximum number of bytes to be read during file I/O operations
    static constexpr size_t MAX_RECORD_SIZE_BYTES = 1024 * 1024 * 1024;

    // Default construction and copy semantics are prohibited

    FileUtils() = delete;
    FileUtils(FileUtils const&) = delete;
    FileUtils& operator=(FileUtils const&) = delete;

    ~FileUtils() = delete;

    /**
     * @param databaseInfo
     *   the description of the database and tables
     *
     * @param chunk
     *   the chunk number
     *
     * @return
     *   list of all file names representing partitioned tables
     *   of a database and a chunk.
     */
    static std::vector<std::string> partitionedFiles(DatabaseInfo const& databaseInfo, unsigned int chunk);

    /**
     * @param databaseInfo
     *   the description of the database and tables
     *
     * @return
     *   list of all file names representing regular tables of a database.
     */
    static std::vector<std::string> regularFiles(DatabaseInfo const& databaseInfo);

    /**
     * Parse the file name and if successful fill in a tuple with components of
     * the name. The file name are expected to match one of the following patterns:
     * @code
     *   <table>_<chunk>.<ext>
     *   <table>FullOverlap_<chunk>.<ext>
     * @code
     *
     * Where:
     *
     *   <table> - is the name of a valid partitioned table as per the database info
     *   <chunk> - is a numeric chunk number
     *   <ext>   - is one of the database file extensions
     *
     * @param parsed
     *   the tuple to be initialized upon the successful completion
     *
     * @param fileName
     *   the name of a file (no directory name) including its extension
     *
     * @param databaseInfo
     *   the database specification
     *
     * @return
     *   'true' if the file name matches one of the expected pattens. The tuple elements
     *   will be (in the order of their definition): the name of a table (including 'FullOverlap'
     *   where applies), the number of a chunk, and its extension (w/o the dot)
     */
    static bool parsePartitionedFile(std::tuple<std::string, unsigned int, std::string>& parsed,
                                     std::string const& fileName, DatabaseInfo const& databaseInfo);

    /**
     * Compute a simple control sum on the specified file
     *
     * @param fileName
     *   the name of a file to read
     *
     * @param recordSizeBytes
     *   desired record size
     *
     * @return
     *   the control sum of the file content
     *
     * @throws std::runtime_error
     *   if there was a problem with opening or reading the file
     *
     * @throws std::invalid_argument
     *   if the file name is empty or if the record size
     *   is 0 or too huge (more than FileUtils::MAX_RECORD_SIZE_BYTES)
     */
    static uint64_t compute_cs(std::string const& fileName,
                               size_t recordSizeBytes = DEFAULT_RECORD_SIZE_BYTES);

    /// @return user account under which the current process runs
    static std::string getEffectiveUser();

    /**
     * Create a temporary file with a unique name at the specified location.
     * The file will be empty. And it will be closed after completion of
     * the method. The final file name would look like:
     * @code
     * <baseDir>/<filePrefix><model-replaced-with-random-text><fileSuffix>
     * @code
     *
     * @param baseDir a location where the file will get created
     * @param prefix (optional) file name prefix
     * @param model (optional) random patter as as required by boost::filesystem::unique_path
     * @param suffix (optionaL) file name suffix (including an extention if needed)
     * @param maxRetries the maximum number of extra retries to generate the unique file
     * @return the name of the file
     * @throws std::invalid_argument if the \model is empty, or \maxRetries is less than 1
     * @throws std::range_error if the filename length exceeds 255 characters
     * @throws std::runtime_error to report failures with the file system operations,
     *   or if the maximum number of retries exceeded.
     */
    static std::string createTemporaryFile(std::string const& baseDir,
                                           std::string const& prefix = std::string(),
                                           std::string const& model = "%%%%-%%%%-%%%%-%%%%",
                                           std::string const& suffix = std::string(),
                                           unsigned int maxRetries = 1);

    /**
     * Check if each folder (given by its absolute path) in the input collection exists
     * and is write-enabled for an effective user of the current process. Create missing
     * folders if needed.
     *
     * @param requestorContext a context from which the operation was requested
     *   can be provided to improve error reporting and diagnostics
     * @param folders a collection of folders to be created/verified
     * @param createMissingFolders (optional) flag telling the method to create missing folders
     * @throws std::invalid_argument for empty or non-absolute path names found in
     *   the input collection
     * @throws std::runtime_error if any folder can't be created, or if it's not
     *   write-enabled for the effective current user
     */
    static void verifyFolders(std::string const& requestorContext, std::vector<std::string> const& folders,
                              bool createMissingFolders = false);
};

/**
 * Class FileCsComputeEngine incrementally computes a control sum of
 * the file content.
 *
 * Here is how the engine is supposed to be used:
 *
 * @code
 *   try {
 *       FileCsComputeEngine eng("myfile.dat");
 *       while (eng.execute()) {
 *           ctd::cout << "bytes read: " << eng.bytes() << "\n"
 *                     << "running cs: " << eng.cs() << "\n";
 *       }
 *       std::cout << "total bytes read: " << eng.bytes() << "\n"
 *                 << "final cs:         " << eng.cs() << "\n";
 *   } catch (std::exception const& ex) {
 *       std::cerr << ex.what() << std::endl;
 *   }
 * @endcode
 */
class FileCsComputeEngine {
public:
    // Default construction and copy semantics are prohibited

    FileCsComputeEngine() = delete;
    FileCsComputeEngine(FileCsComputeEngine const&) = delete;
    FileCsComputeEngine& operator=(FileCsComputeEngine const&) = delete;

    ~FileCsComputeEngine();

    /**
     * The normal constructor
     *
     * At each iteration of the engine (when method FileCsComputeEngine::execute()
     * is called) the engine will read up to 'recordSizeBytes' bytes from
     * the input file. The method will return 'false' when all bytes of the file
     * are read and the EOF is reached. Any attempts to read the file beyond that
     * will throw exception std::runtime_error.
     *
     * The engine will close a file immediately after reaching its EOF.
     *
     * @param fileName
     *   the name of a file to read
     *
     * @param recordSizeBytes
     *   desired record size
     *
     * @throw  std::runtime_error
     *   if there was a problem with opening or reading the file
     *
     * @throw std::invalid_argument
     *   if the file name is empty or if the record size
     *   is 0 or too huge (more than FileUtils::MAX_RECORD_SIZE_BYTES)
     */
    explicit FileCsComputeEngine(std::string const& fileName,
                                 size_t recordSizeBytes = FileUtils::DEFAULT_RECORD_SIZE_BYTES);

    /// @return the name of the file
    std::string const& fileName() const { return _fileName; }

    /// @return the number of bytes read so far
    size_t bytes() const { return _bytes; }

    /// @return the running (and the final one the file is fully read) control sum
    uint64_t cs() const { return _cs; }

    /**
     * Run the next iteration of reading the file and computing its control sum
     *
     * @return
     *   'true' (meaning 'done') when the EOF has been reached.
     *
     * @throw std::runtime_error
     *   there was a problem with reading the file
     *
     * @throw  std::logic_error
     *   an attempt to read the file after it was closed
     */
    bool execute();

private:
    /// The name of an input file
    std::string const _fileName;

    /// The desired record size when reading from the file
    size_t const _recordSizeBytes;

    /// The file pointer
    std::FILE* _fp;

    /// The record buffer
    uint8_t* _buf;

    /// The number of bytes read so far
    size_t _bytes;

    /// The running (and the final one the file is fully read) control sum
    uint64_t _cs;
};

/**
 * Class MultiFileCsComputeEngine would compute control sums and measure file sizes
 * for each file in a collection (of files).
 */
class MultiFileCsComputeEngine {
public:
    // Default construction and copy semantics are prohibited

    MultiFileCsComputeEngine() = delete;
    MultiFileCsComputeEngine(MultiFileCsComputeEngine const&) = delete;
    MultiFileCsComputeEngine& operator=(MultiFileCsComputeEngine const&) = delete;

    /// Destructor (non-trivial because some resources need to be properly released)
    ~MultiFileCsComputeEngine();

    /**
     * The normal constructor
     *
     * @param fileNames
     *   files to be processed
     *
     * @param recordSizeBytes
     *   record size (for reading from files)
     *
     * @throws std::runtime_error
     *   if there was a problem with opening the first file
     *
     * @throws std::invalid_argument
     *   if the record size is 0 or too huge (more than FileUtils::MAX_RECORD_SIZE_BYTES)
     */
    explicit MultiFileCsComputeEngine(std::vector<std::string> const& fileNames,
                                      size_t recordSizeBytes = FileUtils::DEFAULT_RECORD_SIZE_BYTES);

    /// @return the names of the files
    std::vector<std::string> const& fileNames() const { return _fileNames; }

    /**
     * Check the status of a file
     *
     * @param fileName
     *   the name of a file
     *
     * @return
     *   'true' if the specified file has been or is being processed
     *   so that its final or running checksum or the number of bytes can be
     *   be obtained.
     *
     * @throws std::invalid_argument
     *   unknown file name
     */
    bool processed(std::string const& fileName) const;

    /**
     * Get a size of a file
     *
     * @param fileName
     *   the name of a file
     *
     * @return
     *   the number of bytes read so far for the specified file.
     *
     * @throw std::invalid_argument
     *   unknown file name
     *
     * @throw std::logic_error
     *   if the file hasn't been processed
     */
    size_t bytes(std::string const& fileName) const;

    /**
     * Get the compute/check sum of a file
     *
     * @param fileName
     *   the name of a file
     *
     * @return
     *   the running (and the final one the file is fully read) control
     *   sum for the specified file.
     *
     * @throw std::invalid_argument
     *   unknown file name
     *
     * @throw std::logic_error
     *   if the file hasn't been processed
     */
    uint64_t cs(std::string const& fileName) const;

    /**
     * Run the next iteration of reading files and computing their control sums
     *
     * @return
     *   'true' (meaning 'done') when the EOF of the last file has been reached.
     *
     * @throw std::runtime_error
     *   there was a problem with reading a file
     *
     * @throw std::logic_error
     *   an attempt to read the last file after it was closed
     */
    bool execute();

private:
    /// The names of files to be processed
    std::vector<std::string> const _fileNames;

    /// The desired record size
    size_t const _recordSizeBytes;

    /// The number of a file which is being processed. The iterator
    /// is set to _fileNames.end() after finishing processing the very
    /// last file of the collection.
    std::vector<std::string>::const_iterator _currentFileItr;

    /// Files which has been or are being processed
    std::map<std::string, std::unique_ptr<FileCsComputeEngine>> _processed;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_FILEUTILS_H
