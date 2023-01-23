/*
 * LSST Data Management System
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
#include <tuple>

using namespace std;

namespace lsst::qserv::replica {

///////////////////////////////////////
//                Chunk              //
///////////////////////////////////////

bool Chunk::operator==(Chunk const& rhs) const {
    return tie(databaseFamily, number) == tie(rhs.databaseFamily, rhs.number);
}

bool Chunk::operator<(Chunk const& rhs) const {
    return tie(databaseFamily, number) < tie(rhs.databaseFamily, rhs.number);
}

ostream& operator<<(ostream& os, Chunk const& chunk) {
    os << "Chunk (" << chunk.databaseFamily << ":" << chunk.number << ")";
    return os;
}

/////////////////////////////////////////////
//                ChunkLocker              //
/////////////////////////////////////////////

bool ChunkLocker::isLocked(Chunk const& chunk) const {
    replica::Lock mLock(_mtx, "ChunkLocker::" + string(__func__) + "(chunk)");
    return _chunk2owner.count(chunk);
}

bool ChunkLocker::isLocked(Chunk const& chunk, string& owner) const {
    replica::Lock mLock(_mtx, "ChunkLocker::" + string(__func__) + "(chunk,owner)");

    auto itr = _chunk2owner.find(chunk);
    if (itr != _chunk2owner.end()) {
        owner = itr->second;
        return true;
    }
    return false;
}

ChunkLocker::OwnerToChunks ChunkLocker::locked(string const& owner) const {
    replica::Lock mLock(_mtx, "ChunkLocker::" + string(__func__));

    OwnerToChunks owner2chunks;
    _lockedImpl(mLock, owner, owner2chunks);

    return owner2chunks;
}

void ChunkLocker::_lockedImpl(replica::Lock const& mLock, string const& owner,
                              ChunkLocker::OwnerToChunks& owner2chunks) const {
    for (auto&& entry : _chunk2owner) {
        Chunk const& chunk = entry.first;
        string const& chunkOwner = entry.second;

        if (owner.empty() or (owner == chunkOwner)) {
            owner2chunks[chunkOwner].push_back(chunk);
        }
    }
}

bool ChunkLocker::lock(Chunk const& chunk, string const& owner) {
    replica::Lock mLock(_mtx, "ChunkLocker::" + string(__func__));

    if (owner.empty()) {
        throw invalid_argument("ChunkLocker::" + string(__func__) + "  empty owner");
    }
    auto itr = _chunk2owner.find(chunk);
    if (itr != _chunk2owner.end()) return owner == itr->second;

    _chunk2owner[chunk] = owner;
    return true;
}

bool ChunkLocker::release(Chunk const& chunk) {
    replica::Lock mLock(_mtx, "ChunkLocker::" + string(__func__) + "(chunk)");

    // An owner (if set) will be ignored by the current method

    string owner;
    return _releaseImpl(mLock, chunk, owner);
}

bool ChunkLocker::release(Chunk const& chunk, string& owner) {
    replica::Lock mLock(_mtx, "ChunkLocker::" + string(__func__) + "(chunk,owner)");
    return _releaseImpl(mLock, chunk, owner);
}

bool ChunkLocker::_releaseImpl(replica::Lock const& mLock, Chunk const& chunk, string& owner) {
    auto itr = _chunk2owner.find(chunk);
    if (itr == _chunk2owner.end()) return false;

    // ATTENTION: remove the chunk from this map _only_ after
    //            getting its owner

    owner = itr->second;
    _chunk2owner.erase(itr);

    return true;
}

list<Chunk> ChunkLocker::release(string const& owner) {
    replica::Lock mLock(_mtx, "ChunkLocker::" + string(__func__) + "(owner)");

    if (owner.empty()) {
        throw invalid_argument("ChunkLocker::" + string(__func__) + "  empty owner");
    }

    // Get rid of chunks owned by the specified owner, and also collect
    // those (removed) chunks into a vector to be returned to a caller.

    OwnerToChunks owner2chunks;
    _lockedImpl(mLock, owner, owner2chunks);

    list<Chunk> chunks = owner2chunks[owner];
    for (auto&& chunk : chunks) {
        _chunk2owner.erase(chunk);
    }
    return chunks;
}

}  // namespace lsst::qserv::replica
