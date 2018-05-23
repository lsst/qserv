/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// FileUtils.h declares:
///
/// class FileUtils
/// class FileCsComputeEngine
///
/// (see individual class documentation for more information)

// System headers
#include <cstdio>       // C-style I/O
#include <cstdint>      // size_t, uint64_t
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class DatabaseInfo;

/**
  * The utility class encapsulating various operations with file systems.
  *
  * ATTENTION: this class can't be instantiated
  */
class FileUtils {

public:

    /// The default number of bytes to be read during file I/O operations
    static constexpr size_t DEFAULT_RECORD_SIZE_BYTES = 1024*1024;

    /// The maximum number of bytes to be read during file I/O operations
    static constexpr size_t MAX_RECORD_SIZE_BYTES = 1024*1024*1024;

    // Default construction and copy semantics are prohibited

    FileUtils() = delete;
    FileUtils(FileUtils const&) = delete;
    FileUtils& operator=(FileUtils const&) = delete;

    ~FileUtils() = delete;

    /**
     * @param databaseInfo - the description of the database and tables
     * @param chunk        - the chunk number
     *
     * @return list of all file names representing partitioned tables
     * of a database and a chunk.
     */
    static std::vector<std::string> partitionedFiles(DatabaseInfo const& databaseInfo,
                                                     unsigned int chunk);

    /**
     * @param databaseInfo - the description of the database and tables
     *
     * @return list of all file names representing regular tables
     * of a database.
     */
    static std::vector<std::string> regularFiles(DatabaseInfo const& databaseInfo);

    /**
     * Parse the file name and if successfull fill in a tuple with components of
     * the name. The file name are expected to matche one of the following patterns:
     *
     *   <table>_<chunk>.<ext>
     *   <table>FullOverlap_<chunk>.<ext>
     *
     * Where:
     *
     *   <table> - is the name of a valid partitioned table as per the database info
     *   <chunk> - is a numeric chunk number
     *   <ext>   - is one of the database file extentions
     *
     * @param parsed       - the tuple to be initialized upon the successfull completion
     * @param fileName     - the name of a file (no directory name) including its extention
     * @param databaseInfo - the database specification
     *
     * @return 'true' if the file name matches one of the expected pattens. The tuple's elements
     * will be (in the order of their definition): the name of a table (including 'FullOverlap'
     * where applies), the number of a chunk, adm its extention (w/o the dot)
     */
    static bool parsePartitionedFile(std::tuple<std::string, unsigned int, std::string>& parsed,
                                     std::string const& fileName,
                                     DatabaseInfo const& databaseInfo);

    /**
     * Compute a simple control sum on the specified file
     *
     * The method will throw one of the following exceptions:
     *   std::runtime_error    - if there was a problem with opening or reading
     *                           the file
     *   std::invalid_argument - if the file name is empty or if the record size
     *                           is 0 or too huge (more than FileUtils::MAX_RECORD_SIZE_BYTES)
     *
     * @param fileName        - the name of a file to read
     * @param recordSizeBytes - desired record size
     *
     * @return the control sum of the file content
     */
    static uint64_t compute_cs(std::string const& fileName,
                               size_t recordSizeBytes=DEFAULT_RECORD_SIZE_BYTES);

    /// @return user account uner which the current process runs
    static std::string getEffectiveUser();
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
     * Exceptions:
     *   std::runtime_error    - if there was a problem with opening or reading the file
     *   std::invalid_argument - if the file name is empty or if the record size
     *                           is 0 or too huge (more than FileUtils::MAX_RECORD_SIZE_BYTES)
     *
     * At each iteration of the engine (when method FileCsComputeEngine::execute()
     * is called) the engine will read up to 'recordSizeBytes' bytes from
     * the input file. The method will return 'false' when all bytes of the file
     * are read and the EOF is reached. Any attempts to read the file beyond that
     * will throw exception std::runtime_error.
     *
     * The engine will close a file immediatelly after reaching its EOF.
     *
     * @param fileName        - the name of a file to read
     * @param recordSizeBytes - desired record size
     *
     */
    explicit FileCsComputeEngine(std::string const& fileName,
                                 size_t recordSizeBytes=FileUtils::DEFAULT_RECORD_SIZE_BYTES);

    /// @return the name of the file
    std::string const& fileName() const { return _fileName; }
    
    /// @return the number of bytes read so far
    size_t bytes() const { return _bytes; }

    /// @return the running (and the final one the file is fully read) control sum
    uint64_t cs() const { return _cs; }

    /**
     * Run the next iteration of reading the file and computing its control sum
     *
     * Exceptions:
     *   std::runtime_error - there was a problem with reading the file
     *   std::logic_error   - an attempt to read the file after it was closed
     *
     * @return 'true' (meaning 'done') when the EOF has been reached.
     */
    bool execute();

private:

    std::string _fileName;          ///< the name of an input file
    size_t      _recordSizeBytes;   ///< desired record size

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
     * @param fileNames       - files to be processed
     * @param recordSizeBytes - record size (for reading from files)
     *
     * @throws std::runtime_error if there was a problem with opening the first file
     * @throws std::invalid_argument if the record size is 0 or too huge (more than FileUtils::MAX_RECORD_SIZE_BYTES)
     */
    explicit MultiFileCsComputeEngine(
                std::vector<std::string> const& fileNames,
                size_t recordSizeBytes=FileUtils::DEFAULT_RECORD_SIZE_BYTES);

    /// @return the names of the files
    std::vector<std::string> const& fileNames() const { return _fileNames; }
    
    /**
     * @return 'true' if the specified file has been or is being proccessed
     * so that its final or running checksime or the number of bytes can be
     * be obtained.
     *
     * @throws std::invalid_argument unkown file name
     *
     * @param fileName - the name of a file
     */
    bool processed(std::string const& fileName) const;

    /**
     * @return the number of bytes read so far for the specified file.
     *
     * @throws std::invalid_argument unkown file name
     * @throws std::logic_error the file hasn't been proccessed
     *
     * @param fileName - the name of a file
     */
    size_t bytes(std::string const& fileName) const;

   /**
     * @return the running (and the final one the file is fully read) control
     * sum for the specified file.
     *
     * @throws std::invalid_argument unkown file name
     * @throws std::logic_error the file hasn't been proccessed
     *
     * @param fileName - the name of a file
     */
    uint64_t cs(std::string const& fileName) const;

    /**
     * Run the next iteration of reading files and computing their control sums
     *
     * Exceptions:
     *   std::runtime_error - there was a problem with reading a file
     *   std::logic_error   - an attempt to read the last file after it was closed
     *
     * @return 'true' (meaning 'done') when the EOF of the last file has been reached.
     */
    bool execute();

private:

    std::vector<std::string> _fileNames;        ///< The names of files to be processed
    size_t                   _recordSizeBytes;  ///< desired record size

    /// The number of a file which is being processed. The iterator
    /// is set to _fileNames.end() after finishing processing the very
    /// last file of the collection.
    std::vector<std::string>::iterator _currentFileItr;

    /// Files which has been or are being processed
    std::map<std::string, std::unique_ptr<FileCsComputeEngine>> _processed;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FILEUTILS_H