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
#include <mutex>
#include <string>
#include <unistd.h>

// Qserv headers
#include "memman/Memory.h"

// System headers
#include <atomic>
#include <string>

namespace lsst {
namespace qserv {
namespace memman {

//-----------------------------------------------------------------------------
//! @brief Description of a memory based file.
//-----------------------------------------------------------------------------

class MemFile {
public:

    //-----------------------------------------------------------------------------
    //! @brief Get file path for this table.
    //!
    //! @return The filename.
    //-----------------------------------------------------------------------------

    std::string filePath() {return _fPath;}

    //-----------------------------------------------------------------------------
    //! @brief Lock database file in memory.
    //!
    //! @param  maxBytes- maximum file size       for locking the table file.
    //!                   A value of zero skips the check.
    //! @param  minRefs - minimum reference count for locking the table file.
    //!
    //! @return MLResult  When bLocked > 0 this number of bytes locked.
    //!                   When bLocked = 0 no bytes were locked and retc holds
    //!                   the reason as follows:
    //! @return =0        retc = 0 the file did not meet minRefs restriction;
    //!                   otherwise, it holds the errno failure code.
    //-----------------------------------------------------------------------------

    struct MLResult {size_t bLocked;
                     int    retc;
                     MLResult() {}
                     MLResult(size_t lksz, int rc) : bLocked(lksz), retc(rc) {}
                    };

    MLResult    memLock(size_t maxBytes=0, int minRefs=0);

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
    //!
    //! @return MFResult  When mfP is zero or retc is not zero, the MemFile
    //!                   object could not be obtained and retc holds errno.
    //-----------------------------------------------------------------------------

    struct MFResult {MemFile* mfP;
                     int      retc;
                     MFResult() {}
                     MFResult(MemFile* mfp, int rc) : mfP(mfp), retc(rc) {}
                    };

    static MFResult obtain(std::string const& fPath, Memory& mem);

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
    //-----------------------------------------------------------------------------

           MemFile(std::string const& fPath, Memory& mem, MemInfo const& minfo)
                   : _fPath(fPath), _memory(mem), _memInfo(minfo),
                     _refs(1), _isLocked(0) {}

          ~MemFile() {}

std::string _fPath;
Memory&     _memory;
MemInfo     _memInfo;
int         _refs;      // Protected by cacheMutex
bool        _isLocked;  // Ditto
};

}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMFILE_H

