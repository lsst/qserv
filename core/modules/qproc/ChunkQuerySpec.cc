// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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

// Class header
#include "qproc/ChunkQuerySpec.h"

// System headers
#include <algorithm>
#include <iostream>
#include <iterator>

// Qserv headers
#include "util/IterableFormatter.h"

namespace lsst {
namespace qserv {
namespace qproc {

////////////////////////////////////////////////////////////////////////
// class ChunkQuerySpec
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, ChunkQuerySpec const& c) {

    for(ChunkQuerySpec const* frag = &c;
        frag != NULL;
        frag = frag->nextFragment.get()) {
        os << "ChunkQuerySpec(db=" << frag->db << ", chunkId=" << frag->chunkId << ", ";
        os << "sTables=" << util::printable(frag->subChunkTables) << ", ";
        os << "queries=" << util::printable(frag->queries) << ", ";
        os << "subChunkIds=" << util::printable(frag->subChunkIds);
        os << ")";
    }
    return os;
}

}}} // namespace lsst::qserv::qproc

