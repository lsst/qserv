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
/// \brief A class for tracking the number and size of records in
///        the triangles of a Hierarchical Triangular Mesh.

#ifndef LSST_QSERV_ADMIN_DUPR_HTMINDEX_H
#define LSST_QSERV_ADMIN_DUPR_HTMINDEX_H

#include <stdint.h>
#include <ostream>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/unordered_map.hpp"


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

/// An HTM index tracks how many records of an input data set are contained
/// in all HTM triangles of a given subdivision level L. It also provides a
/// mapping from the set of all level-L HTM IDs to the set of level-L HTM
/// IDs for non-empty triangles.
///
/// An HTM index has an implementation-defined binary file format with the
/// following property: the concatenation of two index files with the same
/// subdivision level produces a valid index file that is equivalent to the
/// index of the union of the original input data sets.
class HtmIndex {
public:
    /// Create an empty HTM index at the given subdivision level.
    explicit HtmIndex(int level);
    /// Read an HTM index from a file.
    explicit HtmIndex(boost::filesystem::path const & path);
    /// Read and merge a list of HTM index files.
    explicit HtmIndex(std::vector<boost::filesystem::path> const & paths);
    HtmIndex(HtmIndex const & idx);

    ~HtmIndex();

    HtmIndex & operator=(HtmIndex const & idx) {
        if (this != &idx) {
            HtmIndex tmp(idx);
            swap(tmp);
        }
        return *this;
    }

    /// Return the subdivision level of the index.
    int getLevel() const { return _level; }
    /// Return the total number of records tracked by the index.
    uint64_t getNumRecords() const { return _numRecords; }

    uint64_t operator()(uint32_t id) const {
        Map::const_iterator i = _map.find(id);
        if (i == _map.end()) {
            return 0;
        }
        return i->second;
    }
    /// Return the number of non-empty triangles in the index.
    size_t size() const { return _map.size(); }
    bool empty() const { return size() == 0; }

    /// Map the given triangle to a non-empty triangle in a deterministic way.
    uint32_t mapToNonEmpty(uint32_t id) const;

    /// Write or append the index to a binary file.
    void write(boost::filesystem::path const & path, bool truncate) const;
    /// Write the index to a stream in human readable format.
    void write(std::ostream & os) const;

    /// Add or merge the given triangle with this index, returning a reference
    /// to the newly added or updated triangle.
    void add(uint32_t id, uint64_t numRecords);

    /// Add or merge the triangles in the given index with the triangles in
    /// this one.
    void merge(HtmIndex const & idx);

    void clear();
    void swap(HtmIndex & idx);

private:
    typedef boost::unordered_map<uint32_t, uint64_t> Map;

    static int const ENTRY_SIZE = 4 + 8; // HTM ID: 4 bytes, count: 8 bytes

    void _read(boost::filesystem::path const & path);

    uint64_t _numRecords;
    Map _map;
    std::vector<uint32_t> mutable _keys; // derived from _map on-demand
    int _level;
};

inline void swap(HtmIndex & a, HtmIndex & b) {
    a.swap(b);
}
inline std::ostream & operator<<(std::ostream & os, HtmIndex const & idx) {
    idx.write(os);
    return os;
}

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_HTMINDEX_H
