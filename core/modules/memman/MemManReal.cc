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
#include "memman/MemManReal.h"

// System Headers
#include <errno.h>
#include <mutex>
#include <string.h>
#include <unordered_map>

// Qserv Headers
#include "memman/MemFile.h"
#include "memman/MemFileSet.h"

/******************************************************************************/
/*                  L o c a l   S t a t i c   O b j e c t s                   */
/******************************************************************************/
  
namespace {

std::mutex hanMutex;

std::unordered_map<lsst::qserv::memman::MemMan::Handle,
                   lsst::qserv::memman::MemFileSet*> hanCache;

lsst::qserv::memman::MemMan::Handle handleNum
                             = lsst::qserv::memman::MemMan::HandleType::ISEMPTY;
}

namespace lsst {
namespace qserv {
namespace memman {
  
/******************************************************************************/
/*                         g e t S t a t i s t i c s                          */
/******************************************************************************/
  
MemMan::Statistics MemManReal::getStatistics() {

    Statistics stats;

    // Get all the needed information and return it
    //
    stats.bytesLockMax = _memory.bytesMax();
    stats.bytesLocked  = _memory.bytesLocked();
    stats.bytesReserved= _memory.bytesReserved();
    stats.numFlexLock  = _memory.flexNum();
    stats.numLocks     = _numLocks;
    stats.numErrors    = _numErrors;
    stats.numFiles     = MemFile::numFiles();

    // The following requires a lock
    //
    hanMutex.lock();
    stats.numFSets = hanCache.size();
    stats.numReqdFiles = _numReqdFiles;
    stats.numFlexFiles = _numFlexFiles;
    hanMutex.unlock();
    return stats;
}

/******************************************************************************/
/*                             g e t S t a t u s                              */
/******************************************************************************/

MemMan::Status MemManReal::getStatus(Handle handle) {

    Status status;

    // First check if this is a valid handle and, if so, find it in our cache.
    // Once found, get its real status from the file set object.
    //
    if (handle != HandleType::INVALID && handle != HandleType::ISEMPTY) {
       hanMutex.lock();
       auto it = hanCache.find(handle);
       if (it != hanCache.end() && it->second->isOwner(_memory)) {
          status = it->second->status();
          hanMutex.unlock();
          return status;
       }
       hanMutex.unlock();
     }

    // Return null status
    //
    memset(&status, 0, sizeof(status));
    return status;
}
  
/******************************************************************************/
/*                                  l o c k                                   */
/******************************************************************************/
  
MemMan::Handle MemManReal::lock(std::vector<TableInfo> const& tables, int chunk) {

    int  lockNum, flexNum, retc = 0;
    bool mustLock;

    // Pass 1: determine the number of files needed in the file set
    //
    lockNum = flexNum = 0;
    for (auto&& tab : tables) {
        if (         tab.theData  == TableInfo::LockType::MUSTLOCK) lockNum++;
            else if (tab.theData  == TableInfo::LockType::FLEXIBLE) flexNum++;
        if (         tab.theIndex == TableInfo::LockType::MUSTLOCK) lockNum++;
            else if (tab.theIndex == TableInfo::LockType::FLEXIBLE) flexNum++;
    }

    // If we don't need to lock anything then indicate success but return a
    // a special file handle that indicates the file set is empty.
    //
    if (lockNum == 0 && flexNum == 0) return HandleType::ISEMPTY;

    // Allocate an empty file set sized to handle this request
    //
    MemFileSet* fileSet = new MemFileSet(_memory, lockNum, flexNum, chunk);

    // Pass 2: Add required files to the file set
    //
    for (auto&& tab : tables) {
        mustLock =      tab.theData  == TableInfo::LockType::MUSTLOCK;
        if (mustLock || tab.theData  == TableInfo::LockType::FLEXIBLE) {
           retc = fileSet->add(tab.tableName, chunk, false, mustLock);
            if (retc) break;
        }
        mustLock =      tab.theIndex == TableInfo::LockType::MUSTLOCK;
        if (mustLock || tab.theIndex == TableInfo::LockType::FLEXIBLE) {
           retc = fileSet->add(tab.tableName, chunk, true,  mustLock);
           if (retc) break;
        }
     }

    // If we ended with no errors then try to memlock the file set. We do this
    // with a global mutex to make sure we have a predictable view of memory.
    //
    if (retc == 0) {
       std::lock_guard<std::mutex> guard(hanMutex);

       // Lock all required tables and any flexible tables we can. Upon success
       // (with global lock held) update statistics, generate a file handle,
       // add it to the handle cache, and return the handle.
       //
       retc = fileSet->lockAll();
       if (retc == 0) {
          _numReqdFiles += lockNum;
          _numFlexFiles += flexNum;
          handleNum++;
          hanCache.insert({handleNum, fileSet});
          return handleNum;
       }
    }

    // If we wind up here we failed to perform the operation; return an error.
    //
    _numErrors++;
    delete fileSet;
    errno = retc;
    return HandleType::INVALID;
}
  
/******************************************************************************/
/*                                u n l o c k                                 */
/******************************************************************************/

bool MemManReal::unlock(Handle handle) {

    std::lock_guard<std::mutex> guard(hanMutex);

    // If this is a nill handle, then we need not do anything more. If this is
    // a bad handle, return failure.
    //
    if (handle == HandleType::ISEMPTY) return true;
    if (handle == HandleType::INVALID) return false;

    // Find the table set in the set cache
    //
    auto it = hanCache.find(handle);
    if (it == hanCache.end() || !(it->second->isOwner(_memory))) return false;

    // Delete the file set and remove it from the map
    //
    delete it->second;
    hanCache.erase(it);
    return true;
}

/******************************************************************************/
/*                             u n l o c k A l l                              */
/******************************************************************************/
  
void MemManReal::unlockAll() {

    std::lock_guard<std::mutex> guard(hanMutex);

    // Delete all of the file set entries that we own via handle cache. The
    // file set destructor will unlock any memory that it needs to unlock.
    //
    auto it = hanCache.begin();

    while(it != hanCache.end()) {
         if (it->second->isOwner(_memory)) {
            delete it->second;
            it = hanCache.erase(it);
         } else it++;
    }
}
}}} // namespace lsst:qserv:memman

