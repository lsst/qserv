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

#ifndef LSST_QSERV_MEMMAN_MEMMANNONE_H
#define LSST_QSERV_MEMMAN_MEMMANNONE_H

// System headers
#include <errno.h>
#include <memory.h>

// Qserv Headers
#include "MemMan.h"

namespace lsst {
namespace qserv {
namespace memman {

// This class defines a memory manager implementation that basically does
// nothing. If a table needs to be locked it says that there is no memory to
// do so. However, flexible locking is allowed. Eventually, this will be
// replaced by an actual implementation. For now, this allows testing.

class MemManNone : public MemMan {
public:

    Handle lock(std::vector<TableInfo> const& tables, int chunk) override {
               (void)chunk;
               if (_alwaysLock) return HandleType::ISEMPTY;
               for (auto it=tables.begin() ; it != tables.end(); it++) {
                   if (it->theData  == TableInfo::LockType::MUSTLOCK
                   ||  it->theIndex == TableInfo::LockType::MUSTLOCK)
                      {errno = ENOMEM; return HandleType::INVALID;}
               }
               return HandleType::ISEMPTY;
           }

    bool  unlock(Handle handle) override {(void)handle; return true;}

    void  unlockAll() override {}

    Statistics getStatistics() override {return _myStats;}

    Status getStatus(Handle handle) override {(void)handle; return _status;}

    int waitFor(Handle handle) override {return 0;}

    MemManNone & operator=(const MemManNone&) = delete;
    MemManNone(const MemManNone&) = delete;

    // @param alwaysLock - When true, always return ISEMPTY for all lock requests.
    MemManNone(uint64_t maxBytes, bool alwaysLock) : _alwaysLock(alwaysLock)
              {memset(&_myStats, 0, sizeof(_myStats));
               _myStats.bytesLockMax = maxBytes;
               _myStats.bytesLocked  = maxBytes;
               memset(&_status, 0, sizeof(_status));
              }

    ~MemManNone() override {}

private:
    Statistics _myStats;
    Status     _status;
    int        _alwaysLock{false};
};

}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMMANNONE_H

