// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
#ifndef LSST_QSERV_QPROC_GEOMADAPTER_H
#define LSST_QSERV_QPROC_GEOMADAPTER_H
/**
  * @file
  *
  * @brief Geometry adapter interface code
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <vector>

// Other external headers
#include "boost/make_shared.hpp"

// LSST headers
#include "sg/Box.h"
#include "sg/Circle.h"
#include "sg/Ellipse.h"
#include "sg/ConvexPolygon.h"

// Qserv headers
#include "css/StripingParams.h"
#include "qproc/QueryProcessingError.h"

namespace lsst {
namespace qserv {
namespace qproc {

inline boost::shared_ptr<sg::Box>
getBoxFromParams(std::vector<double> const& params) {
    if(params.size() != 4) {
        throw QueryProcessingError("Invalid number of parameters for box");
    }
    return boost::make_shared<sg::Box>(sg::Box::fromDegrees(params[0], params[1], params[2], params[3]));
}

inline boost::shared_ptr<sg::Circle>
getCircleFromParams(std::vector<double> const& params) {
    // lon, lat radius_deg
    if(params.size() != 3) {
        throw QueryProcessingError("Invalid number of parameters for circle");
    }
    sg::LonLat center = sg::LonLat::fromDegrees(params[0], params[1]);
    sg::Angle a = sg::Angle::fromDegrees(params[2]);
    return boost::make_shared<sg::Circle>(sg::UnitVector3d(center), a);
}

inline boost::shared_ptr<sg::Ellipse>
getEllipseFromParams(std::vector<double> const& params) {
    // lon, lat, semimajang, semiminang, posangle
    if(params.size() != 5) {
        throw QueryProcessingError("Invalid number of parameters for ellips");
    }
    sg::UnitVector3d center(sg::LonLat::fromDegrees(params[0], params[1]));
    return boost::make_shared<sg::Ellipse>(
        center,
        sg::Angle::fromDegrees(params[2]),
        sg::Angle::fromDegrees(params[3]),
        sg::Angle::fromDegrees(params[4]));
}

inline boost::shared_ptr<sg::ConvexPolygon>
getConvexPolyFromParams(std::vector<double> const& params) {
    // polygon vertices, min 3 vertices, must get even number of params
    if((params.size() <= 6) || ((params.size() & 1) != 0)) {
        throw QueryProcessingError("Invalid number of parameters for polygon");
    }
    std::vector<sg::UnitVector3d> uv3;
    for(unsigned i=0; i < params.size(); i += 2) {
        sg::LonLat vx = sg::LonLat::fromDegrees(params[i], params[i+1]);
        uv3.push_back(sg::UnitVector3d(vx));
    }
    return boost::make_shared<sg::ConvexPolygon>(uv3);
}

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_GEOMADAPTER_H

