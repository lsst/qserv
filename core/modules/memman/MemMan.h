// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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

#ifndef LSST_QSERV_MEMMAN_MEMMAN_H
#define LSST_QSERV_MEMMAN_MEMMAN_H

// System headers
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace memman {

//-----------------------------------------------------------------------------
//! @brief Describe a table that can be potentially locked in memory.
//!
//! A table marked as MANDATORY downgrades to FLEXIBLE if the same table was
//! previously added and marked FLEXIBLE. Tables marked FLEXIBLE are locked if
//! there is sufficient memory. Otherwise, the required memory is reserved and
//! a lock attempt is made when the table is encountered in the future.
//-----------------------------------------------------------------------------

class TableInfo {
public:

    std::string tableName;    //< Name of the table

    enum class LockType {
        NOLOCK   = 0,         //< Item should not be locked
        MUSTLOCK = 1,         //< Item must be locked or declare failure
        MANDATORY= 1,         //< Item must be locked or declare failure
        FLEXIBLE = 2,         //< Item may  be locked but memory is reserved
        OPTIONAL = 3          //< Item may  be locked if possible or ignored
    };

    LockType theData;         //< Lock options for the table's data
    LockType theIndex;        //< Lock options for the table's index, if any

    //-----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param  tabName   is the name of the table.
    //! @param  optData   lock options for the table's data
    //! @param  optIndex  lock options for the table's index
    //-----------------------------------------------------------------------------

    TableInfo(std::string const& tabName,
              LockType optData=LockType::MUSTLOCK,
              LockType optIndex=LockType::NOLOCK)
             : tableName(tabName), theData(optData), theIndex(optIndex)
             {}
};

//-----------------------------------------------------------------------------
//! @brief The memory manager.
//!
//! The MemMan is an abstract class the defines the interface to the memory
//! manager that is used to lock database chunks in memory.
//-----------------------------------------------------------------------------

class MemMan {
public:
    using Ptr = std::shared_ptr<MemMan>;

    //-----------------------------------------------------------------------------
    //! @brief Create a memory manager and initialize for processing.
    //!
    //! @param  maxBytes   - Maximum amount of memory that can be used
    //! @param  dbPath     - Path to directory where the database resides
    //!
    //! @return !0: The pointer to the memory manager.
    //! @return  0: A manager could not be created.
    //-----------------------------------------------------------------------------

    static MemMan* create(uint64_t maxBytes, std::string const& dbPath);

    //-----------------------------------------------------------------------------
    //! @brief Lock a set of tables in memory for a particular chunk.
    //!
    //! @param  tables - Reference to the tables to process.
    //! @param  chunk  - The chunk number associated with the tables.
    //!
    //! @return =0     - Nothing was locked. The errno variable holds the
    //!                  reason, as follows:
    //!                  xxxxxx - filesystem or memory error
    //!                  ENOENT - a chunk was missing
    //!                  ENOMEM - insufficient memory to fully satisfy request
    //! @return !0     - Is the resource handle associated with this request.
    //-----------------------------------------------------------------------------

    using Handle = uint64_t;

    struct HandleType {
        static const Handle INVALID=0;
        static const Handle ISEMPTY=1;
    };

    virtual Handle lock(std::vector<TableInfo> const& tables, int chunk) = 0;

    //-----------------------------------------------------------------------------
    //! @brief Unlock a set of tabes previously locked by the lock() method.
    //!
    //! @param  handle  - The resource handle returned by lock().
    //!
    //! @return false: The resource was not found.
    //! @return true:  The the memory associated with the resource has been
    //!                release. If this is the last usage of the resource,
    //!                the memory associated with the resource is unlocked.
    //-----------------------------------------------------------------------------

    virtual bool  unlock(Handle handle) = 0;

    //-----------------------------------------------------------------------------
    //! @brief Release all resources and unlock all locked memory.
    //!
    //! This method effectively calls unlock() on each resource handle.
    //-----------------------------------------------------------------------------

    virtual void  unlockAll() = 0;

    //-----------------------------------------------------------------------------
    //! @brief Obtain statistics about this memory manager.
    //!
    //! @return The statistics.
    //-----------------------------------------------------------------------------

    struct Statistics {
        uint64_t bytesLockMax; //!< Maximum number of bytes to lock
        uint64_t bytesLocked;  //!< Current number of bytes locked
        uint64_t bytesReserved;//!< Current number of bytes reserved
        uint32_t numFSets;     //!< Global  number of active file sets
        uint32_t numFiles;     //!< Global  number of active files
        uint32_t numReqdFiles; //!< Number  required files encountered
        uint32_t numFlexFiles; //!< Number  flexible files encountered
        uint32_t numFlexLock;  //!< Number  flexible files that were locked
        uint32_t numLocks;     //!< Number of calls to lock()
        uint32_t numErrors;    //!< Number of calls that failed
    };

    virtual Statistics getStatistics() = 0;

    //-----------------------------------------------------------------------------
    //! @brief Obtain resource status.
    //!
    //! @param  handle  - The handle returned by lock().
    //!
    //! @return The query status. If the resource was not found numTables is
    //!         set to zero.
    //-----------------------------------------------------------------------------

    struct Status {
        uint64_t bytesLock; //!< Number of resource bytes locked
        uint32_t numFiles;  //!< Number of files resource has
        int      chunk;     //!< Chunk number associated with resource
    };

    virtual Status getStatus(Handle handle) = 0;

    //-----------------------------------------------------------------------------

    MemMan & operator=(const MemMan&) = delete;
    MemMan(const MemMan&) = delete;

                  MemMan() {}
    virtual      ~MemMan() {}
};

}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMMAN_H

