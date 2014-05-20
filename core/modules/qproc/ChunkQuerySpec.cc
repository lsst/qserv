/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
  * @file
  *
  * @brief Implementation of ostream << operator for ChunkQuerySpec
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qproc/ChunkQuerySpec.h"

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>

namespace lsst {
namespace qserv {
namespace qproc {

////////////////////////////////////////////////////////////////////////
// class ChunkQuerySpec
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, ChunkQuerySpec const& c) {
    os << "ChunkQuerySpec(db=" << c.db << " chunkId=" << c.chunkId
       << "sTables=";
    std::copy(c.subChunkTables.begin(), c.subChunkTables.end(),
              std::ostream_iterator<std::string>(os, ","));
    os << " : ";
    for(ChunkQuerySpec const* frag = &c;
        frag != NULL;
        frag = frag->nextFragment.get()) {
        os << "[q=";
        std::copy(frag->queries.begin(), frag->queries.end(),
                  std::ostream_iterator<std::string>(os, ","));
        os << " sIds=";
        std::copy(frag->subChunkIds.begin(), frag->subChunkIds.end(),
                  std::ostream_iterator<int>(os, ","));
        os << "] ";
    }
    os << ")";
    return os;
}

}}} // namespace lsst::qserv::qproc

