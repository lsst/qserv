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
/**
  * @file ChunkSpec.cc
  *
  * @brief Implementation of helper printers for ChunkSpec
  *
  * @author Daniel L. Wang, SLAC
  */

#include "lsst/qserv/master/ChunkSpec.h"
#include <iterator>
#include <cassert>
#include <stdexcept>

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
/// A "good" number of subchunks to include in a chunk query.  This is
/// a guess. The best value is an open question
int const GOOD_SUBCHUNK_COUNT = 20; 
}

namespace lsst { 
namespace qserv { 
namespace master {
std::ostream& operator<<(std::ostream& os, ChunkSpec const& c) {
    os << "ChunkSpec[" 
       << "chunkId=" << c.chunkId
       << " subChunks:";
    std::copy(c.subChunks.begin(), c.subChunks.end(), 
              std::ostream_iterator<int>(os, ","));
    os << "]";
    return os;
}

////////////////////////////////////////////////////////////////////////
// ChunkSpec
////////////////////////////////////////////////////////////////////////
bool ChunkSpec::shouldSplit() const {
    return subChunks.size() > (unsigned)GOOD_SUBCHUNK_COUNT;
}
////////////////////////////////////////////////////////////////////////
// ChunkSpecFragmenter
////////////////////////////////////////////////////////////////////////
ChunkSpecFragmenter::ChunkSpecFragmenter(ChunkSpec const& s) 
    : _original(s), _pos(_original.subChunks.begin()) {
}
ChunkSpec ChunkSpecFragmenter::get() const {
    ChunkSpec c;
    c.chunkId = _original.chunkId;
    Iter posEnd = _pos + GOOD_SUBCHUNK_COUNT;
    Iter end = _original.subChunks.end();
    if(posEnd >= end) {
        posEnd = end;
    }
    c.subChunks.resize(posEnd - _pos);
    std::copy(_pos, posEnd, c.subChunks.begin());
    return c;
}

void ChunkSpecFragmenter::next() {
    _pos += GOOD_SUBCHUNK_COUNT;
}

bool ChunkSpecFragmenter::isDone() {
    return _pos >= _original.subChunks.end();
}
////////////////////////////////////////////////////////////////////////
// ChunkSpecSingle
////////////////////////////////////////////////////////////////////////
// precondition: !spec.subChunks.empty() 
ChunkSpecSingle::List ChunkSpecSingle::makeList(ChunkSpec const& spec) {
    List list;
    if(spec.subChunks.empty()) {
        throw std::logic_error("Attempted subchunk spec list without subchunks.");
    }
    ChunkSpecSingle s;
    s.chunkId = spec.chunkId;
    std::vector<int>::const_iterator i;
    for(i = spec.subChunks.begin();
        i != spec.subChunks.end(); ++i) {
        s.subChunkId = *i;
        list.push_back(s);
    }    
    return list;
}

std::ostream& operator<<(std::ostream& os, ChunkSpecSingle const& c) {
    os << "(" << c.chunkId << "," << c.subChunkId << ")";
    return os;
}

}}} // namespace lsst::qserv::master

