// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
#ifndef LSST_QSERV_QPROC_CHUNKSPEC_H
#define LSST_QSERV_QPROC_CHUNKSPEC_H
/**
  * @file
  *
  * @brief ChunkSpec, ChunkSpecFragmenter, and ChunkSpecSingle declarations
  *
  * @author Daniel L. Wang, SLAC
  */
#include <ostream>
#include <list>
#include <vector>
#include <stdint.h>

namespace lsst {
namespace qserv {
namespace qproc {

/// ChunkSpec is a value class that bundles the per-chunk information that is
/// used to compose a concrete chunk query for a specific chunk from an input
/// parsed query statement. Contains A specification of chunkId and subChunkId
/// list.
struct ChunkSpec {
public:
    ChunkSpec() : chunkId(-1) {}
    int32_t chunkId;
    std::vector<int32_t> subChunks;
    void addSubChunk(int s) { subChunks.push_back(s); }
    bool shouldSplit() const;
};
std::ostream& operator<<(std::ostream& os, ChunkSpec const& c);

typedef std::list<ChunkSpec> ChunkSpecList;
typedef std::vector<ChunkSpec> ChunkSpecVector;

/// An iterating fragmenter to reduce the number of subChunkIds per ChunkSpec
class ChunkSpecFragmenter {
public:
    ChunkSpecFragmenter(ChunkSpec const& s);
    ChunkSpec get() const;
    void next();
    bool isDone();
private:
    typedef std::vector<int32_t>::const_iterator Iter;
    ChunkSpec _original;
    Iter _pos;

};
/// A specification of ChunkSpec with only one subChunk
/// TODO: Consider renaming this. (SubChunkSpec?)
class ChunkSpecSingle {
public:
    typedef std::list<ChunkSpecSingle> List;
    ChunkSpecSingle() : chunkId(-1), subChunkId(-1) {}
    int32_t chunkId;
    int32_t subChunkId;
    static List makeList(ChunkSpec const& spec);
};
std::ostream& operator<<(std::ostream& os, ChunkSpecSingle const& c);

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_CHUNKSPEC_H

