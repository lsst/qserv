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

#ifndef LSST_QSERV_MEMMAN_MEMFILESET_H
#define LSST_QSERV_MEMMAN_MEMFILESET_H

// System headers
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unistd.h>

// Qserv headers
#include "memman/MemMan.h"
#include "memman/MemFile.h"

namespace lsst {
namespace qserv {
namespace memman {

class Memory;

//-----------------------------------------------------------------------------
//! @brief Encapsulation of a memory database file set.
//-----------------------------------------------------------------------------

class MemFileSet {
public:

    //-----------------------------------------------------------------------------
    //! @brief Add a file to a file set.
    //!
    //! @param  tabname - The table name in question.
    //! @param  chunk   - Associated chunk number.
    //! @param  iFile,  - When true  this is an index file, else a data file.
    //! @param  mustLK  - When true  file is added to the mandatory list.
    //!                   When false file is added to the flexible  list.
    //!
    //! @return =0        Corresponding file added to fileset.
    //! @return !0        Corresponding file not added, errno value returned.
    //-----------------------------------------------------------------------------

    int    add(std::string const& tabname, int chunk, bool iFile, bool mustLK);

    //-----------------------------------------------------------------------------
    //! @brief Determine ownership.
    //!
    //! @param  memory  - Reference to the memory object that should own fileset
    //!
    //! @return true  Supplied memory object matches our memory object.
    //! @return false Ownership does not match.
    //-----------------------------------------------------------------------------

    bool   isOwner(Memory const& memory) {return &memory == &_memory;}

    //-----------------------------------------------------------------------------
    //! @bried Lock all of the required tables in a table set and as many
    //!        flexible files as possible.
    //!
    //! @param strict- When true, if a required table could not be locked, its
    //!                memory remains reserved but otherwise locking continues.
    //!                When false, execution stops when an error is encountered.
    //!
    //! @return =0 all required bytes that could be locked were locked.
    //! @return !0 A required file could not be locked, errno value is returned.
    //-----------------------------------------------------------------------------

    int    lockAll(bool strict);

    //-----------------------------------------------------------------------------
    //! @bried Map all of the required tables in a table set and as many
    //!        flexible files as possible.
    //!
    //! @return =0 all required tables that could be mapped were mapped.
    //! @return !0 A required file could not be mapped, errno value is returned.
    //-----------------------------------------------------------------------------

    int    mapAll();

    //-----------------------------------------------------------------------------
    //! @brief Control serial access to this object. When obtaining a lock,
    //!        this method should be called with a common mutex locked.
    //!
    //! @param dolok If true, obtains a mutex on this object to serialize access.
    //!              Otherwise, the lock is released.
    //!
    //! @return true  Normal return.
    //! @return false lktst was true and serialization mutex was already locked.
    //-----------------------------------------------------------------------------

    bool serialize(bool dolok) {
        if (dolok) {
            _setMutex.lock(); 
            _mtxLocked = true;
        } else {
            if (_mtxLocked) {
                _mtxLocked = false;
                _setMutex.unlock();
            }
        }
        return true;
    }

    //-----------------------------------------------------------------------------
    //! @brief Retrn status.
    //!
    //! @return Status information.
    //-----------------------------------------------------------------------------

    MemMan::Status status();

    //-----------------------------------------------------------------------------
    //! @brief Constructor
    //!
    //! @param  memory  - Memory object that owns this file set.
    //! @param  numLock - Initial allocation for lock files vector.
    //! @param  numFlex - Initial allocation for flex files vector.
    //! @param  chunk   - The associated chunk number.
    //-----------------------------------------------------------------------------

    MemFileSet(Memory& memory, int numLock, int numFlex, int chunk)
              : _memory(memory), _lockBytes(0), _numFiles(0), _chunk(chunk),
                _mtxLocked(false) {
                _lockFiles.reserve(numLock);
                _flexFiles.reserve(numFlex);
              }

    ~MemFileSet();

private:
    std::mutex            _setMutex;
    Memory&               _memory;
    std::vector<MemFile*> _lockFiles;
    std::vector<MemFile*> _flexFiles;
    uint64_t              _lockBytes;     // Total bytes locked
    uint32_t              _numFiles;
    int                   _chunk;
    std::atomic_bool      _mtxLocked;     // true -> _setMutex is locked
};

}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMFILESET_H

