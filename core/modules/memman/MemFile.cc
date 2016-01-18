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
std::mutex                                 cacheMutex;
std::unordered_map<std::string, MemFile *> fileCache;
};

/******************************************************************************/
/*                               m e m L o c k                                */
/******************************************************************************/

MemFile::MLResult MemFile::memLock(size_t maxBytes, int minRefs) {

MemInfo mInfo;
std::lock_guard<std::mutex> gaurd(cacheMutex);

    // If the file is already locked, indicate success
    //
    if (_isLocked)
       {MLResult aokResult(_memInfo.size(), 0);
        return aokResult;
       }

    // If the file doesn't meet the refcount restrictio, don't lock it
    //
    if (_refs < minRefs)
       {MLResult nilResult(0,0);
        return nilResult;
       }

    // If a space is wanted, do so now before we attemptto lock the file
    //
    if (maxBytes != 0 && _memInfo.size() > maxBytes)
       {MLResult bigResult(0, ENOMEM);
        return bigResult;
       }

    // Lock this table in memory if possible.
    //
    mInfo = _memory.memLock(_fPath);

    // If we successfully locked this file, then indicate so, update the
    // memory information and return.
    //
    if (mInfo.isValid())
       {MLResult aokResult(mInfo.size(),0);
        _isLocked = 1;
        _memInfo = mInfo;
        return aokResult;
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

std::lock_guard<std::mutex> gaurd(cacheMutex);

    // Simply return the size of our file cache
    //
    return fileCache.size();
}

/******************************************************************************/
/*                                o b t a i n                                 */
/******************************************************************************/
  
MemFile::MFResult MemFile::obtain(std::string const& fPath, Memory& mem) {

MemFile *mfP;
MemInfo  mInfo;
std::lock_guard<std::mutex> gaurd(cacheMutex);

    // First look up if this table already exists in our cache and is using the
    // the same memory object (error if not). If so, up the reference count and
    // return the object as it may be shared.
    //
    auto it = fileCache.find(fPath);
    if (it != fileCache.end())
       {if (&(it->second->_memory) != &mem)
           {MFResult errResult(0, EXDEV);
            return errResult;
           }
        it->second->_refs++;
        MFResult aokResult(&(*(it->second)),0);
        return aokResult;
       }

    // Validate the file and get its size
    //
    mInfo = mem.fileInfo(fPath);
    if (!mInfo.isValid())
       {MFResult errResult(0, mInfo.errCode());
        return errResult;
       }

    // Get a new file object and insert it into the map
    //
    mfP = new MemFile(fPath, mem, mInfo);
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

std::lock_guard<std::mutex> gaurd(cacheMutex);

    // Decrease the reference count. If there are still references, return
    //
    _refs--;
    if (_refs > 0) return;

    // Release the memory
    //
    _memory.memRel(_memInfo);

    // Remove the object from our cache
    //
    auto it = fileCache.find(_fPath);
    if (it != fileCache.end()) fileCache.erase(it);

    // Delete ourselves as we are done
    //
    delete this;
}
}}} // namespace lsst:qserv:memman

