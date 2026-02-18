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

/// \file
/// \brief Operations with the "secondary" index

#ifndef LSST_PARTITION_OBJECTINDEX_H
#define LSST_PARTITION_OBJECTINDEX_H

// System headers
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>

// LSST headers
#include "partition/Csv.h"

// Forward declarations
namespace lsst::partition {
class ChunkLocation;
}  // namespace lsst::partition

// This header declarations
namespace lsst::partition {

/**
 * Class ObjectIndex is a frontend to the file-based index which maps object identifiers
 * to their partitioning locations. The index is meant to be used as a "director" index for
 * partitioned data-sets.
 *
 * Objects of the class can be open in two modes:
 * - WRITE: for writing into an index file at the specified location.
 * - READ: reading from an index via the specified source.
 *
 * Files written in the WRITE mode are open in the "append" mode, hence their previous
 * content won't be truncated. It's up to a client to ensure the files get removed if
 * the clean starting state is required.
 *
 * The source specification in the READ node is a URL-style resource. Presently, only the file-based
 * resource is supported. Hence, the file URL is expected
 * to look like this:
 * @code
 *   file:///<path-to-the-index-file>
 * @code
 * Normally the path specified in the resource would be a file which was produced
 * in the WRITE mode. The path could have a relative or absolute value. It could also be
 * located on a local or remote filesystem. Since the file would be read sequentially then
 * no specific requirements for file locking or direct access are imposed by this implementation.
 *
 * All methods of the class are thread-safe.
 */
class ObjectIndex {
public:
    ObjectIndex() = default;

    ObjectIndex(ObjectIndex const&) = delete;
    ObjectIndex& operator=(ObjectIndex const&) = delete;

    /// The non-trivial destructor is needed to close and release resources used by the index.
    ~ObjectIndex();

    /// Modes for opening the index.
    enum Mode { READ, WRITE };

    /// @return A value of 'true' if the index is open or created.
    bool isOpen() const { return _isOpen; }

    /// @return A mode the index was open/created in.
    Mode mode() const { return _mode; }

    /**
     * Open (or create) index for writing into a local file.
     *
     * @param fileName  A path to a local file to be open/created.
     * @param editor  A CSV "editor" which is used for formatting output products.
     * @param idFieldName  The name of an output field representing object identifiers.
     * @param chunkIdFieldName  The name of an output field representing chunk identifiers.
     * @param subChunkIdFieldName  The name of an output field representing sub-chunk identifiers.
     * @throw std::invalid_argument  Invalid specification of the fields.
     * @throw std::runtime_error  If the file opening/creation operation failed for another reason.
     */
    void create(std::string const& fileName, csv::Editor const& editor, std::string const& idFieldName,
                std::string const& chunkIdFieldName, std::string const& subChunkIdFieldName);

    /**
     * Open the index.
     *
     * Note that parameter 'dialect' should be normally the same one which is
     * used for making output products of the partitioning tool. This is based on an assumption
     * that all products made by the partitioning tool in a context of a given catalog would be
     * partitioned in the same way. If that's not the case
     *
     * @param url  An index specification.
     * @param dialect  A dialect for parsing index specifications.
     * @throw std::invalid_argument  Invalid specification of the index.
     * @throw std::runtime_error  Failed to open the index.
     */
    void open(std::string const& url, csv::Dialect const& dialect);

    /**
     * Write a record into the index.
     *
     * @param id  An identifier of an object.
     * @param location  A location of the object (including chunk and sub-chunk identifiers).
     * @throw std::logic_error  If the index was not created or if it's not open in Mode::WRITE.
     * @throw std::invalid_argument  Invalid values of the input parameters.
     */
    void write(std::string const& id, ChunkLocation const& location);

    /**
     * Locate chunkId and subChunkId for a given object identifier.
     *
     * @param id  An identifier to be located in the index.
     * @return A pair of a chunkId and a subChunkId for the identifier.
     * @throw std::logic_error  If the index is not open or if it's not open in Mode::READ.
     * @throw std::invalid_argument  Invalid value of the identifier.
     * @throw std::out_of_range  If the specified identifier wasn't found in the index.
     */
    std::pair<int32_t, int32_t> read(std::string const& id);

private:
    // The genera state of the index
    bool _isOpen = false;
    Mode _mode = Mode::READ;

    /// The mutex is used for thread-safety of the public API of the index instances
    std::mutex _mtx;

    // Attributes of the index open in Mode::READ
    std::string _inUrl;
    std::map<std::string, std::pair<int32_t, int32_t>> _inIndexMap;

    // Attributes of the index open in Mode::WRITE
    int _outIdField = -1;
    int _outChunkIdField = -1;
    int _outSubChunkIdField = -1;
    std::unique_ptr<csv::Editor> _outEditorPtr;
    std::unique_ptr<char[]> _outBuf;
    std::string _outFileName;
    std::ofstream _outFile;
};

}  // namespace lsst::partition

#endif  // LSST_PARTITION_OBJECTINDEX_H
