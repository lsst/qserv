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

// Class header
#include "memman/MemFileSet.h"

// System Headers
#include <errno.h>

// Qserv Headers
#include "memman/MemFile.h"
#include "memman/Memory.h"

namespace lsst {
namespace qserv {
namespace memman {

/******************************************************************************/
/*                            D e s t r u c t o r                             */
/******************************************************************************/
  
MemFileSet::~MemFileSet() {

    // Unreference every fle in our file set. This action will also cause
    // memory to be unlocked if no one else is using the file then the file
    // object will be deleted as well.
    //
    for (auto mfP : _lockFiles) {mfP->release();}
    for (auto mfP : _flexFiles) {mfP->release();}
}

/******************************************************************************/
/*                                   a d d                                    */
/******************************************************************************/

int MemFileSet::add(std::string const& tabname, int chunk,
                    bool iFile, bool mustLK) {

    std::string fPath(_memory.filePath(tabname, chunk, iFile));

    // Obtain a memory file object for this table and chunk
    //
    MemFile::MFResult mfResult = MemFile::obtain(fPath, _memory, !mustLK);
    if (mfResult.mfP == 0) return mfResult.retc;

    // Add to the appropriate file set
    //
    if (mustLK) {
        _lockFiles.push_back(mfResult.mfP);
    } else {
        _flexFiles.push_back(mfResult.mfP);
    }
    _numFiles++;
    return 0;
}
  
/******************************************************************************/
/*                               l o c k A l l                                */
/******************************************************************************/

int MemFileSet::lockAll() {

    MemFile::MLResult mlResult;
    uint64_t bytesLocked, bytesMax, freeBytes;

    // Calculate the number of bytes available at this point. Note that we force
    // freeBytes to be atleast 1 to make memlock check before it tries locking
    // the file to avoid a useless memory map operation if it can't be locked.
    // By the time we get here someone else may have already locked the file.
    //
    bytesMax    = _memory.bytesMax();
    bytesLocked = _memory.bytesLocked();
    if (bytesMax <= bytesLocked) {
        freeBytes = 1;
    } else {
        freeBytes = bytesMax - bytesLocked;
    }

    // Try to lock all of the required tables
    //
    for (auto mfP : _lockFiles) {
        mlResult = mfP->memLock(freeBytes);
        if (mlResult.retc != 0) return mlResult.retc;
        _lockBytes += mlResult.bLocked;
        if (freeBytes  > mlResult.bLocked) {
            freeBytes -= mlResult.bLocked;
        } else {
            freeBytes = 1;
        }
    }

    // Try locking as many flexible files as we can. We only lock the file table
    // if the reference count >= 2 to optimize memory usage. At some point we
    // will place unlocked flex files on a "want to lock" queue. FUTURE!!!
    //
    for (auto mfP : _flexFiles) {
        mlResult = mfP->memLock(freeBytes, 2);
        if (mlResult.bLocked == 0) continue;
        _lockBytes += mlResult.bLocked;
        if (freeBytes  > mlResult.bLocked) {
            freeBytes -= mlResult.bLocked;
        } else {
            freeBytes = 1;
        }
    }

    // All done
    //
    return 0;
}

/******************************************************************************/
/*                                s t a t u s                                 */
/******************************************************************************/
  
MemMan::Status MemFileSet::status() {

    MemMan::Status myStatus;

    // Fill out status information and return it.
    //
    myStatus.bytesLock = _lockBytes;
    myStatus.numFiles  = _numFiles;
    myStatus.chunk     = _chunk;
    return myStatus;
}
}}} // namespace lsst:qserv:memman

