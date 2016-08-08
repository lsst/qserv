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

#ifndef LSST_QSERV_MEMMAN_MEMMANREAL_H
#define LSST_QSERV_MEMMAN_MEMMANREAL_H

// System headers
#include <atomic>
#include <mutex>
#include <unordered_map>

// Qserv Headers
#include "memman/MemMan.h"
#include "memman/Memory.h"

namespace lsst {
namespace qserv {
namespace memman {

class MemFileSet;

//! @brief This class defines a memory manager implementation.

class MemManReal : public MemMan {
public:

    int    lock(Handle handle, bool strict=false) override;

    Handle prepare(std::vector<TableInfo> const& tables, int chunk) override;

    bool   unlock(Handle handle) override;

    void   unlockAll() override;

    Statistics getStatistics() override;

    Status     getStatus(Handle handle) override;

    MemManReal & operator=(const MemManReal&) = delete;
    MemManReal(const MemManReal&) = delete;

    MemManReal(std::string const& dbPath, uint64_t maxBytes)
              : _memory(dbPath, maxBytes), _numErrors(0), _numLkerrs(0),
                _numLocks(0), _numReqdFiles(0), _numFlexFiles(0) {}

    ~MemManReal() override {unlockAll();}

private:

    Memory           _memory;
    std::atomic_uint _numErrors;
    std::atomic_uint _numLkerrs;
    uint32_t         _numLocks;      // Under control of hanMutex
    uint32_t         _numReqdFiles;  // Ditto
    uint32_t         _numFlexFiles;  // Ditto
};

}}} // namespace lsst:qserv:memman
#endif  // LSST_QSERV_MEMMAN_MEMMANREAL_H

