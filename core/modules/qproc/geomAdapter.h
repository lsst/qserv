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

// LSST headers
#include "lsst/sphgeom/Box.h"
#include "lsst/sphgeom/Circle.h"
#include "lsst/sphgeom/Ellipse.h"
#include "lsst/sphgeom/ConvexPolygon.h"

// Qserv headers
#include "css/StripingParams.h"
#include "qproc/QueryProcessingError.h"

namespace lsst {
namespace qserv {
namespace qproc {

inline std::shared_ptr<lsst::sphgeom::Box>
getBoxFromParams(std::vector<double> const& params) {
    if(params.size() != 4) {
        throw QueryProcessingError("Invalid number of parameters for box");
    }
    return std::make_shared<lsst::sphgeom::Box>(lsst::sphgeom::Box::fromDegrees(params[0], params[1], params[2], params[3]));
}

inline std::shared_ptr<lsst::sphgeom::Circle>
getCircleFromParams(std::vector<double> const& params) {
    // lon, lat radius_deg
    if(params.size() != 3) {
        throw QueryProcessingError("Invalid number of parameters for circle");
    }
    lsst::sphgeom::LonLat center = lsst::sphgeom::LonLat::fromDegrees(params[0], params[1]);
    lsst::sphgeom::Angle a = lsst::sphgeom::Angle::fromDegrees(params[2]);
    return std::make_shared<lsst::sphgeom::Circle>(lsst::sphgeom::UnitVector3d(center), a);
}

inline std::shared_ptr<lsst::sphgeom::Ellipse>
getEllipseFromParams(std::vector<double> const& params) {
    // center lon, center lat, semi major axe angle (rad), semi minor axe angle (rad), orientation angle (rad)
    if(params.size() != 5) {
        throw QueryProcessingError("Invalid number of parameters for ellipse");
    }
    lsst::sphgeom::UnitVector3d center(lsst::sphgeom::LonLat::fromDegrees(params[0], params[1]));
    return std::make_shared<lsst::sphgeom::Ellipse>(
        center,
        lsst::sphgeom::Angle::fromDegrees(params[2]),
        lsst::sphgeom::Angle::fromDegrees(params[3]),
        lsst::sphgeom::Angle::fromDegrees(params[4]));
}

inline std::shared_ptr<lsst::sphgeom::ConvexPolygon>
getConvexPolyFromParams(std::vector<double> const& params) {
    // polygon vertices, min 3 vertices, must get even number of params
    if((params.size() < 6) || ((params.size() & 1) != 0)) {
        throw QueryProcessingError("Invalid number of parameters for convex polygon");
    }
    std::vector<lsst::sphgeom::UnitVector3d> uv3;
    for(unsigned i=0; i < params.size(); i += 2) {
        lsst::sphgeom::LonLat vx = lsst::sphgeom::LonLat::fromDegrees(params[i], params[i+1]);
        uv3.push_back(lsst::sphgeom::UnitVector3d(vx));
    }
    return std::make_shared<lsst::sphgeom::ConvexPolygon>(uv3);
}

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_GEOMADAPTER_H

