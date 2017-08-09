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

#ifndef LSST_QSERV_MEMMAN_MEMFILE_H
#define LSST_QSERV_MEMMAN_MEMFILE_H

// System headers
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unistd.h>

// Qserv headers
#include "memman/Memory.h"

namespace lsst {
namespace qserv {
namespace memman {

//-----------------------------------------------------------------------------
//! @brief Description of a memory based file.
//! This class serializes all the appropriate methods in the memory object.
//! It is the only class allowed to call non MT-safe memory methods!
//-----------------------------------------------------------------------------

class MemFile {
public:

    //-----------------------------------------------------------------------------
    //! @brief Lock database file in memory.
    //!
    //! @return MLResult  When bLocked > 0 this number of bytes locked.
    //!                   When bLocked = 0 no bytes were locked and retc holds
    //!                   the reason. When retc = 0 there was not enough memory
    //!                   and the table was marked flexible.
    //-----------------------------------------------------------------------------

    struct MLResult {
        uint64_t bLocked;
        int      retc;
        MLResult() : bLocked(0), retc(0) {}
        MLResult(uint64_t lksz, int rc) : bLocked(lksz), retc(rc) {}
    };

    MLResult    memLock();

    //-----------------------------------------------------------------------------
    //! @brief Map database file in memory.
    //!
    //! @return =0      - File succesfully mapped and memory reserved, if so
    //!                   required (flexible files are not so required).
    //!         !0        A required file could not be mapped in memory. The
    //!                   returned value is the errno describing the error.
    //-----------------------------------------------------------------------------

    int         memMap();

    //-----------------------------------------------------------------------------
    //! @brief Get number of active files (global count).
    //!
    //! @return The number of files.
    //-----------------------------------------------------------------------------

    static uint32_t numFiles();

    //-----------------------------------------------------------------------------
    //! @brief Obtain an object describing a in-memory file.
    //!
    //! @param  rc      - Reference to the place for an error code.
    //! @param  fPath   - The path to the file.
    //! @param  mem     - Reference to the memory object to use for the file.
    //! @param  isFlex  - Tag file as flexible or not (only if new file).
    //!
    //! @return MFResult  When mfP is zero or retc is not zero, the MemFile
    //!                   object could not be obtained and retc holds errno.
    //-----------------------------------------------------------------------------

    struct MFResult {
        MemFile* mfP;
        int      retc;
        MFResult() : mfP(nullptr), retc(0) {}
        MFResult(MemFile* mfp, int rc) : mfP(mfp), retc(rc) {}
    };

    static MFResult obtain(std::string const& fPath, Memory& mem, bool isFlex);

    //-----------------------------------------------------------------------------
    //! @brief Release this table. Upon return it may not be references by
    //!        the caller as it may have been deleted.
    //-----------------------------------------------------------------------------

    void release();

private:
    //-----------------------------------------------------------------------------
    //! @brief Constructor. Only obtain() can allocate a MemFile object.
    //!
    //! @param  fPath   - The path to the file.
    //! @param  mem     - Reference to the associated memory object.
    //! @param  mInfo   - Initial value of the MemInfo object for the file.
    //! @param  isFlex  - Tag file as flexible or not (for statistical reasons).
    //-----------------------------------------------------------------------------

    MemFile(std::string const& fPath,
            Memory&            mem,
            MemInfo const&     minfo,
            bool               isFlex)
           : _fPath(fPath), _memory(mem), _memInfo(minfo), _isFlex(isFlex) {}

   ~MemFile() {}

    std::mutex  _fileMutex;
    std::string _fPath;
    Memory&     _memory;
    MemInfo     _memInfo;              // Protected by _fileMutex
    int         _refs = 1;             // Protected by cacheMutex
    bool        _isMapped   = false;   // Protected by _fileMutex
    bool        _isReserved = false;   // Ditto
    bool        _isLocked   = false;   // Ditto
    bool        _isFlex;               // Set once at object creation
};

}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMFILE_H

