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
#include <mutex>
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

    std::lock_guard<std::mutex> guard(cacheMutex);

    // If the file is already locked, indicate success
    //
    if (_isLocked) {
        if (_isFlex) _memory.flexNum(1);
        MLResult aokResult(_memInfo.size(), 0);
        return aokResult;
    }

    // Check if there is enough memory for this table
    //
    uint64_t freeBytes = _memory.bytesFree();
    if (_isReserved) freeBytes += _memInfo.size();
    if (_memInfo.size() > freeBytes) {
        if (_isFlex) {
            if (!_isReserved) {
                _memory.memReserve(_memInfo.size());
                _isReserved = true;
            }
            MLResult nilResult(0,0);
            return nilResult;
        } else {
            MLResult bigResult(0, ENOMEM);
            return bigResult;
        }
    }

    // Lock this table in memory if possible.
    //
    MemInfo mInfo = _memory.memLock(_fPath, _isFlex);

    // If we successfully locked this file, then indicate so, update the
    // memory information and return. If memory was previously reserved for
    // this file then credit the reserve count using the original size.
    //
    if (mInfo.isValid()) {
        MLResult aokResult(mInfo.size(),0);
        if (_isReserved) {
            _memory.memRestore(_memInfo.size());
            _isReserved = false;
        }
        _isLocked = true;
        _memInfo = mInfo;
        return aokResult;
    }

    // If this is a flex table and there was not enough memory and storage
    // was not yet reserved for it, do so now.
    //
    if (_isFlex && mInfo.errCode() == ENOMEM) {
        if (!_isReserved) {
            _memory.memReserve(_memInfo.size());
            _isReserved = true;
        }
        MLResult nilResult(0,0);
        return nilResult;
    }

    // TODO: Find a better solution for systems where mmap is not viable, which
    // manifests as no bytes being locked but bytes are reserved.
    // Fake mmap as the configuration isn't working properly.
    // At this point, there is enough free space, it was not a flex table,
    // but mmap failed. So,this will fake it being a flexilock file and
    // reserve space for it.
    if (_memInfo.size() < freeBytes) {
        if (!_isReserved) {
            _memory.memReserve(_memInfo.size());
            _isReserved = true;
        }
        MLResult nilResult(0,0);
        return nilResult;
    }

    // Diagnose any errors
    //
    MLResult errResult(0, mInfo.errCode());
    return errResult;
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

    std::lock_guard<std::mutex> guard(cacheMutex);

    // Decrease the reference count. If there are still references, return
    //
    _refs--;
    if (_refs > 0) return;

    // Release the memory
    //
    if (_isLocked) {
        _memory.memRel(_memInfo);
        _isLocked = false;
    } else {
        if (_isReserved) {
            _memory.memRestore(_memInfo.size());
            _isReserved = false;
        }
    }

    // Remove the object from our cache
    //
    auto it = fileCache.find(_fPath);
    if (it != fileCache.end()) fileCache.erase(it);

    // Delete ourselves as we are done
    //
    delete this;
}
}}} // namespace lsst:qserv:memman

