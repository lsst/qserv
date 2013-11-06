/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/// \file
/// \brief A class for tracking the number of records in
///        the chunks and sub-chunks of a partitioned data-set.

#ifndef LSST_QSERV_ADMIN_DUPR_CHUNKINDEX_H
#define LSST_QSERV_ADMIN_DUPR_CHUNKINDEX_H

#include <stdint.h>
#include <ostream>
#include <string>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/unordered_map.hpp"

#include "Chunker.h"


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

/// A chunk index tracks how many records and overlap records are in each
/// chunk and sub-chunk of a partitioned input data set. It also provides
/// methods to compute summary statistics over chunks or sub-chunks.
///
/// A chunk index has an implementation-defined binary file format with
/// the following property: the concatenation of two index files containing
/// chunks and sub-chunks derived from identical partitioning parameters
/// produces a valid index file that is equivalent to the index of the
/// union of the original input data sets.
class ChunkIndex {
public:
    /// An index entry.
    struct Entry {
        uint64_t numRecords;
        uint64_t numOverlapRecords;

        Entry() : numRecords(0), numOverlapRecords(0) { }

        Entry & operator+=(Entry const & e) {
             numRecords += e.numRecords;
             numOverlapRecords += e.numOverlapRecords;
             return *this;
        }
    };

private:
    typedef boost::unordered_map<int32_t, Entry> ChunkMap;
    typedef boost::unordered_map<int64_t, Entry> SubChunkMap;
    typedef ChunkMap::const_iterator ChunkIter;
    typedef SubChunkMap::const_iterator SubChunkIter;

public:
    /// Summary statistics for chunks or sub-chunks.
    struct Stats {
        uint64_t nrec;        ///< Total record count.
        uint64_t n;           ///< Number of chunks or sub-chunks.
        uint64_t min;         ///< Minimum record count.
        uint64_t max;         ///< Maximum record count.
        uint64_t quartile[3]; ///< Record count quartiles.
        double mean;          ///< Mean record count.
        double sigma;         ///< Standard deviation of the record count.
        double skewness;      ///< Skewness of the record count.
        double kurtosis;      ///< Kurtosis of the record count.

        Stats() { clear(); }
        void clear();
        /// Compute statistics from population counts. Sorts `counts`
        /// in-place, but does not otherwise modify it.
        void computeFrom(std::vector<uint64_t> & counts);
        void write(std::ostream & os, std::string const & indent) const;
    };

    /// Create an empty chunk index.
    ChunkIndex();
    /// Read a chunk index from a file.
    explicit ChunkIndex(boost::filesystem::path const & path);
    /// Read and merge a list of chunk index files.
    explicit ChunkIndex(std::vector<boost::filesystem::path> const & paths);

    ChunkIndex(ChunkIndex const & idx);

    ~ChunkIndex();

    ChunkIndex & operator=(ChunkIndex const & idx) {
        if (this != &idx) {
            ChunkIndex tmp(idx);
            swap(tmp);
        }
        return *this;
    }

    /// Return the number of records with the given location.
    uint64_t operator()(ChunkLocation const & loc) const {
        SubChunkIter i = _subChunks.find(
            _key(loc.chunkId, loc.subChunkId));
        if (i == _subChunks.end()) {
            return 0u;
        }
        return loc.overlap ? i->second.numOverlapRecords : i->second.numRecords;
    }

    /// Return record counts for the given chunk.
    Entry const & operator()(int32_t chunkId) const {
        ChunkIter i = _chunks.find(chunkId);
        if (i == _chunks.end()) {
            return EMPTY;
        }
        return i->second;
    }

    /// Return record counts for the given sub-chunk.
    Entry const & operator()(int32_t chunkId, int32_t subChunkId) const {
        SubChunkIter i = _subChunks.find(_key(chunkId, subChunkId));
        if (i == _subChunks.end()) {
            return EMPTY;
        }
        return i->second;
    }

    /// Get summary statistics for chunks or overlap chunks.
    Stats const & getChunkStats(bool overlap) {
        if (_modified) {
            _computeStats();
        }
        return overlap ? _overlapChunkStats : _chunkStats;
    }
    /// Get summary statistics for sub-chunks or overlap sub-chunks.
    Stats const & getSubChunkStats(bool overlap) {
        if (_modified) {
            _computeStats();
        }
        return overlap ? _overlapSubChunkStats : _subChunkStats;
    }

    /// Return the number of non-empty chunks in the index.
    size_t size() const { return _chunks.size(); }
    bool empty() const { return _chunks.empty(); }

    /// Write or append the index to a binary file.
    void write(boost::filesystem::path const & path, bool truncate) const;
    /// Write the index to a stream in human readable format. If
    /// `verbosity < 0`, print statistics only. If `verbosity = 0`,
    /// also print record counts for each chunk. If `verbosity > 0`,
    /// additionally print record counts for each sub-chunk (warning:
    /// output will be voluminous).
    void write(std::ostream & os, int verbosity=0) const;

    /// Add `n` records to the index.
    void add(ChunkLocation const & loc, size_t n=1);

    /// Add or merge the entries in the given index with the entries in
    /// this one.
    void merge(ChunkIndex const & idx);

    void clear();
    void swap(ChunkIndex & idx);

private:
    static Entry const EMPTY;
    static int const ENTRY_SIZE = 8*3;

    static int64_t _key(int32_t chunkId, int32_t subChunkId) {
        return (static_cast<int64_t>(chunkId) << 32) + subChunkId;
    }

    void _read(boost::filesystem::path const & path);
    void _computeStats() const;

    ChunkMap _chunks;
    SubChunkMap _subChunks;

    bool mutable _modified;
    Stats mutable _chunkStats;
    Stats mutable _overlapChunkStats;
    Stats mutable _subChunkStats;
    Stats mutable _overlapSubChunkStats;
};

inline void swap(ChunkIndex & a, ChunkIndex & b) {
    a.swap(b);
}
inline std::ostream & operator<<(std::ostream & os, ChunkIndex const & idx) {
    idx.write(os, -1);
    return os;
}

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_CHUNKINDEX_H
