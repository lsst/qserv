// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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
  * @brief Implementation of IndexMap
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qproc/IndexMap.h"

// System headers
#include <algorithm>
#include <cassert>
#include <iterator>
#include <stdexcept>

// Third-party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "global/stringTypes.h"
#include "qproc/fakeGeometry.h"
#include "query/Constraint.h"

using lsst::qserv::StringVector;
using lsst::qserv::qproc::Region;
using lsst::qserv::qproc::BoxRegion;
using lsst::qserv::qproc::CircleRegion;
using lsst::qserv::qproc::EllipseRegion;
using lsst::qserv::qproc::ConvexPolyRegion;
using lsst::qserv::qproc::Coordinate;

namespace { // File-scope helpers
template <typename T>
std::vector<T> convertVec(StringVector const& v) {
    std::vector<T> out;
    std::transform(v.begin(), v.end(), std::back_inserter(out),
                   boost::lexical_cast<T, std::string>);
    return out;
}
template <typename T>
boost::shared_ptr<Region> make(StringVector const& v) {    
        return boost::shared_ptr<Region>(new T(convertVec<Coordinate>(v)));
}

typedef boost::shared_ptr<Region>(*MakeFunc)(StringVector const& v);

struct FuncMap {
    FuncMap() {        
        fMap["box"] = make<BoxRegion>;
        fMap["circle"] = make<CircleRegion>;
        fMap["ellipse"] = make<EllipseRegion>;
        fMap["poly"] = make<ConvexPolyRegion>;
        fMap["qserv_areaspec_box"]  = make<BoxRegion>;
        fMap["qserv_areaspec_circle"] = make<CircleRegion>;
        fMap["qserv_areaspec_ellipse"] = make<EllipseRegion>;
        fMap["qserv_areaspec_poly"] = make<ConvexPolyRegion>;
    }
    std::map<std::string, MakeFunc> fMap;
};

static FuncMap funcMap;
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {

boost::shared_ptr<Region> getRegion(query::Constraint const& c) {
    return funcMap.fMap[c.name](c.params);
}

ChunkSpec convertChunkTuple(ChunkTuple const& ct) {
    ChunkSpec cs;
    cs.chunkId = ct.chunkId;
    cs.subChunks.resize(ct.subChunkIds.size());
    std::copy(ct.subChunkIds.begin(), ct.subChunkIds.end(),
              cs.subChunks.begin());
    return cs;
}

ChunkSpecVector IndexMap::getIntersect(query::ConstraintVector const& cv) {
    RegionPtrVector rv;
    std::transform(cv.begin(), cv.end(), std::back_inserter(rv), getRegion);
    ChunkRegion cr = _pm->getIntersect(rv);
    ChunkSpecVector csv;
    std::transform(cr.begin(), cr.end(),
                   std::back_inserter(csv), convertChunkTuple);
    return csv;
}


}}} // namespace lsst::qserv::qproc


