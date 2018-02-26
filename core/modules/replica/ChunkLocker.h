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
#ifndef LSST_QSERV_REPLICA_CHUNK_LOCKER_H
#define LSST_QSERV_REPLICA_CHUNK_LOCKER_H

/// ChunkLocker.h declares:
///
/// struct Chunk
/// class  ChunkLocker
///
/// (see individual class documentation for more information)

// System headers
#include <list>
#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Structure Chunk is an abstraction groupping together database families and
 * chunk numbers. This is needed to support chunk replication operations which
 * require chunk collocation.
 */
struct Chunk {
    std::string databaseFamily;
    unsigned int number;
};

/**
 * The overloaded operator for comparing objects of struct Chunk.
 *
 * The operator is used where the equality of chunks is needed.
 *
 * @return 'true' if the chunk is 'equal' to the other one.
 */
bool operator== (Chunk const& lhs, Chunk const& rhs);

/**
 * The overloaded operator for comparing objects of struct Chunk.
 *
 * This operator is needed for using objects of the struct
 * as a key in ordered (map, set, ) or unordered (unordred_map,unordered_set,)
 * asociative containers.
 *
 * @return 'true' if the chunk is 'less' than the other one.
 */
bool operator< (Chunk const& lhs, Chunk const& rhs);

/// The overloaded operator for dumping objects of type Chunk
std::ostream& operator<< (std::ostream& os, Chunk const& chunk);


/**
 * Class ChunkLocker provides a thread-safe mechanism allowing
 * owners (represented by unique string-based identifiers) to claim
 * exclusive 'locks' (ownership claims) on chunks.
 */
class ChunkLocker {

public:

    /// The type for a collection of locked chunks groupped by owners
    typedef std::map<std::string, std::list<Chunk>> ChunksByOwners;

    /// The default constructor
    ChunkLocker () = default;

    // The copy semantics is prohibited

    ChunkLocker (ChunkLocker const&) = delete;
    ChunkLocker& operator= (ChunkLocker const&) = delete;

    /// Destructor
    ~ChunkLocker () = default;

    /**
     * Return 'true' if a chunk is locked
     *
     * @param chunk - a chunk to be tested
     */
    bool isLocked (Chunk const& chunk) const;

    /**
     * Return 'true' if the chunk is locked and set an identifier of
     * an owner which locked the chunk.
     *
     * @param chunk - a chunk to be tested
     * @param owner - a reference to a string which will be initialized with
     *                an identifier of an owner of the chunk if the chunk is found
     *                locked
     */
    bool isLocked (Chunk const& chunk,
                   std::string& ownerId) const;


    /**
     * Return chunks which are loacked by a particular owner (if provided),
     * or by all owners.
     *
     * @param owner - an optional owner. If the owner is not provided then
     *                a;; chunks will be returned
     *
     * @return a collection of chunks groupped by owners
     */
    ChunksByOwners locked (std::string const& owner=std::string()) const;

    /**
     * Lock a chunk to a specific owner and return 'true' of the operation
     * was successfull.
     *
     * NOTE: if an attempt to claim the chunk by the same owner which already
     *       owns it is made then the method will return 'true'.
     *
     * @param chunk - a chunk to be locked
     * @param owner - an identifier of an owner claiming thr chunk
     *
     * @throw std::invalid_argument - if the ownerId is an empty string
     */
    bool lock (Chunk const&       chunk,
               std::string const& owner);

    /**
     * Release a chunk and return 'true' if the operation was successfull
     *
     * @param chunk - a chunk to be released
     */
    bool release (Chunk const& chunk);

    /**
     * Release a chunk and return 'true' if the operation was successfull.
     * In the latter case also set an identifier of an owner which previously
     * 'claimed' the chunk.
     *
     * @param chunk - a chunk to be released
     * @param owner - a reference to a string which will be initialized with
     *                an identifier of an owner which had a claim on the chunk
     *                at a time of the method call if the chunk was found locked
     */
    bool release (Chunk const& chunk,
                  std::string& owner);

    /**
     * Release all chunks which were found claimed by the specified owner
     * and return a collection of those chunks
     *
     * @throw std::invalid_argument - if the owner is an empty string
     */
    std::vector<Chunk> release (std::string const& owner);

private:

    /**
     * Release a chunk and return 'true' if the operation was successfull.
     * In the latter case also set an identifier of an owner which previously
     * 'claimed' the chunk.
     *
     * NOTE: this metod is not thread-safe. It's up to its callers to ensure
     *       proper synchronization context before invoking the method.
     *
     * @param chunk - a chunk to be released
     * @param owner - a reference to a string which will be initialized with
     *                an identifier of an owner which had a claim on the chunk
     *                at a time of the method call if the chunk was found locked
     */
    bool releaseImpl (Chunk const& chunk,
                      std::string& owner);

private:

    /// Mapping a chunk to its "owner" (the one which holds the lock)
    std::map<Chunk, std::string> _chunk2owner;

    /// Mapping an owner to a list of chunks "clamed" by that owner
    /// NOTE: using the list container for the better performance of
    ///       the insert/erase operations over the lists of chunks
    ChunksByOwners _owner2chunks;

    /// For thread safety where it's required
    mutable std::mutex _mtx;

};


/// Overloaded streaming operator for a collection of locked chunks
/// groupped by owners

std::ostream& operator << (std::ostream& os, ChunkLocker::ChunksByOwners const& chunks);

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CHUNK_LOCKER_H