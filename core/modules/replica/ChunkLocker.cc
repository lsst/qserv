/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/ChunkLocker.h"

// System headers
#include <algorithm>
#include <stdexcept>
#include <tuple>        // std::tie

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////////
//                Chunk              //
///////////////////////////////////////

bool Chunk::operator==(Chunk const& rhs) const {
    return  std::tie(databaseFamily, number) ==
            std::tie(rhs.databaseFamily, rhs.number);
}

bool Chunk::operator<(Chunk const& rhs) const {
    return  std::tie(databaseFamily, number) <
            std::tie(rhs.databaseFamily, rhs.number);
}

std::ostream& operator<<(std::ostream& os, Chunk const& chunk) {
    os  << "Chunk (" << chunk.databaseFamily << ":" << chunk.number << ")";
    return os;
}

/////////////////////////////////////////////
//                ChunkLocker              //
/////////////////////////////////////////////

bool ChunkLocker::isLocked(Chunk const& chunk) const {
    util::Lock mLock(_mtx, "ChunkLocker::isLocked(chunk)");
    return _chunk2owner.count(chunk);
}

bool ChunkLocker::isLocked(Chunk const& chunk,
                           std::string& owner) const {

    util::Lock mLock(_mtx, "ChunkLocker::isLocked(chunk,owner)");

    auto itr = _chunk2owner.find(chunk);
    if (itr != _chunk2owner.end()) {
        owner = itr->second;
        return true;
    }
    return false;
}

ChunkLocker::OwnerToChunks ChunkLocker::locked(std::string const& owner) const {

    util::Lock mLock(_mtx, "ChunkLocker::locked");

    OwnerToChunks owner2chunks;
    lockedImpl(mLock,
               owner,
               owner2chunks);

    return owner2chunks;
}

void ChunkLocker::lockedImpl(util::Lock const& mLock,
                             std::string const& owner,
                             ChunkLocker::OwnerToChunks& owner2chunks) const {

    for (auto&& entry: _chunk2owner) {
        Chunk       const& chunk      = entry.first;
        std::string const& chunkOwner = entry.second;

        if (owner.empty() or (owner == chunkOwner)) {
            owner2chunks[chunkOwner].push_back(chunk);
        }
    }
}

bool ChunkLocker::lock(Chunk const&       chunk,
                       std::string const& owner) {

    util::Lock mLock(_mtx, "ChunkLocker::lock");

    if (owner.empty()) {
        throw std::invalid_argument("ChunkLocker::lock  empty owner");
    }
    auto itr = _chunk2owner.find(chunk);
    if (itr != _chunk2owner.end()) return owner == itr->second;

    _chunk2owner[chunk] = owner;
    return true;
}

bool ChunkLocker::release(Chunk const& chunk) {

    util::Lock mLock(_mtx, "ChunkLocker::release(chunk)");

    // An owner (if set) will be ignored by the current method

    std::string owner;
    return releaseImpl(mLock, chunk, owner);
}

bool ChunkLocker::release(Chunk const& chunk,
                          std::string& owner) {

    util::Lock mLock(_mtx, "ChunkLocker::release(chunk,owner)");
    return releaseImpl(mLock, chunk, owner);
}

bool ChunkLocker::releaseImpl(util::Lock const& mLock,
                              Chunk const& chunk,
                              std::string& owner) {

    auto itr = _chunk2owner.find(chunk);
    if (itr == _chunk2owner.end()) return false;

    // ATTENTION: remove the chunk from this map _only_ after
    //            getting its owner

    owner = itr->second;
    _chunk2owner.erase(itr);

    return true;
}

std::list<Chunk> ChunkLocker::release(std::string const& owner) {

    util::Lock mLock(_mtx, "ChunkLocker::release(owner)");

    if (owner.empty()) {
        throw std::invalid_argument("ChunkLocker::release  empty owner");
    }

    // Get rid of chunks owned by the specified owner, and also collect
    // those (removed) chunks into a vector to be returned to a caller.

    OwnerToChunks owner2chunks;
    lockedImpl(mLock,
               owner,
               owner2chunks);

    std::list<Chunk> chunks = owner2chunks[owner];
    for (auto&& chunk: chunks) {
        _chunk2owner.erase(chunk);
    }
    return chunks;
}

}}} // namespace lsst::qserv::replica
