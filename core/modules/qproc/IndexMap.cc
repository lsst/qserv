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
#include <cassert>
#include <iterator>
#include <stdexcept>

#include "global/stringTypes.h"

using lsst::qserv::StringVector;
namespace { // File-scope helpers
template <typename T>
std::vector<T> convertVec(StringVector const& v) {
}

template <typename T>
boost::shared_ptr<Region> make(StringVector const& v) {    
    return boost::shared_ptr<Region>(new T(convertVec<Coordinate>(v)))
}
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
    std::map<std::string, int> fMap;
};
static FuncMap funcMap;
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {

}}} // namespace lsst::qserv::qproc


boost::shared_ptr<Region> getRegion(Constraint const& c) {
    return funcMap.fMap[c.name](c.params);
}
