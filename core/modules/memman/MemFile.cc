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
#include "memman/MemFile.h"

// System Headers
#include <errno.h>
#include <unordered_map>

namespace lsst {
namespace qserv {
namespace memman {

/******************************************************************************/
/*                  L o c a l   S t a t i c   O b j e c t s                   */
/******************************************************************************/
  
namespace {
std::mutex                                cacheMutex;
std::unordered_map<std::string, MemFile*> fileCache;
}

/******************************************************************************/
/*                               m e m L o c k                                */
/******************************************************************************/

MemFile::MLResult MemFile::memLock() {

    // The _fileMutex is used here to serialize multiple calls to lock the same
    // file as a file may appear in multiple file sets. This mutex is held for
    // duration of all operations here. It also serialized memory unmapping.
    //
    std::lock_guard<std::mutex> guard(_fileMutex);
    int rc;

    // If the file is already locked, indicate success
    //
    if (_isLocked) {
        MLResult aokResult(_memInfo.size(), 0);
        return aokResult;
    }

    // Lock this table in memory if possible. If not, simulate an ENOMEM.
    //
    if (!_isMapped) rc = ENOMEM;
        else {
        rc = _memory.memLock(_memInfo, _isFlex);
        if (rc == 0) {
            MLResult aokResult(_memInfo.size(),0);
            _isLocked = true;
            return aokResult;
        }
    }
 
    // If this is a flexible table, we can ignore this error.
    //
    if (_isFlex) {
        MLResult nilResult(0,0);
        return nilResult;
    }

    // Diagnose any errors
    //
    MLResult errResult(0, rc);
    return errResult;
}

/******************************************************************************/
/*                                m e m M a p                                 */
/******************************************************************************/

int MemFile::memMap() {

    std::lock_guard<std::mutex> guard(_fileMutex);

    // If the file is already mapped, indicate success
    //
    if (_isMapped) return 0;

    // Check if we need to verify there is enough memory for this table. If it's
    // already reserved (unlikely) then there is no need to check.
    //
    if (!_isReserved) {
        if (_memInfo.size() > _memory.bytesFree()) return (_isFlex ? 0 : ENOMEM);
        _memory.memReserve(_memInfo.size());
        _isReserved = true;
    }

    // Map this table in memory if possible.
    //
    MemInfo mInfo = _memory.mapFile(_fPath);

    // If we successfully mapped this file, return success (memory reserved).
    //
    if (mInfo.isValid()) {
        _memInfo  = mInfo;
        _isMapped = true;
        return 0;
    }

    // If this is a flex table, ignore mapping failures but keep storage reserved.
    //
    if (_isFlex && mInfo.errCode() == ENOMEM) return 0;

    // Remove storage reservation as we failed to map in this file and it can
    // never be locked at this point.
    //
    _memory.memRestore(_memInfo.size());
    _isReserved = false;

    // Return the error code
    //
    return mInfo.errCode();
}

/******************************************************************************/
/*                              n u m F i l e s                               */
/******************************************************************************/

uint32_t MemFile::numFiles() {

    std::lock_guard<std::mutex> guard(cacheMutex);

    // Simply return the size of our file cache
    //
    return fileCache.size();
}

/******************************************************************************/
/*                                o b t a i n                                 */
/******************************************************************************/
  
MemFile::MFResult MemFile::obtain(std::string const& fPath,
                                  Memory& mem, bool isFlex) {

    std::lock_guard<std::mutex> guard(cacheMutex);

    // First look up if this table already exists in our cache and is using the
    // the same memory object (error if not). If so, up the reference count and
    // return the object as it may be shared. Note: it->second == MemFile*!
    //
    auto it = fileCache.find(fPath);
    if (it != fileCache.end()) {
        if (&(it->second->_memory) != &mem) {
            MFResult errResult(nullptr, EXDEV);
            return errResult;
        }
        it->second->_refs++;
        MFResult aokResult(it->second,0);
        return aokResult;
    }

    // Validate the file and get its size
    //
    MemInfo mInfo = mem.fileInfo(fPath);
    if (!mInfo.isValid()) {
        MFResult errResult(nullptr, mInfo.errCode());
        return errResult;
    }

    // Get a new file object and insert it into the map
    //
    MemFile* mfP = new MemFile(fPath, mem, mInfo, isFlex);
    fileCache.insert({fPath, mfP});

    // Return the pointer to the file object
    //
    MFResult aokResult(mfP,0);
    return aokResult;
}

/******************************************************************************/
/*                               r e l e a s e                                */
/******************************************************************************/

void MemFile::release() {

    // Obtain the cache mutex as it protects the cache and the ref count
    //
    {    std::lock_guard<std::mutex> guard(cacheMutex);

         // Decrease the reference count. If there are still references, return
         //
         _refs--;
         if (_refs > 0) return;

         // Remove the object from our cache
         //
         auto it = fileCache.find(_fPath);
         if (it != fileCache.end()) fileCache.erase(it);
    }

    // We lock the file mutex. We also get the size of the file as memRel()
    // destroys the _memInfo object.
    //
    _fileMutex.lock();
    uint64_t fSize = _memInfo.size();

    // Release the memory if mapped and unreserve the memory if reserved.
    //
    if (_isMapped) {
        _memory.memRel(_memInfo, _isLocked);
        _isLocked = false;
        _isMapped = false;
    }
    if (_isReserved) {
        _memory.memRestore(fSize);
        _isReserved = false;
    }

    // Delete ourselves as we are done
    //
    _fileMutex.unlock();
    delete this;
}
}}} // namespace lsst:qserv:memman

