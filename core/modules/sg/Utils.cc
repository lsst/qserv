/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

/// \file
/// \brief This file contains utility method implementations.

#include "Utils.h"

#include <cmath>

#include "UnitVector3d.h"


namespace lsst {
namespace sg {

double getMinSquaredChordLength(Vector3d const & v,
                                Vector3d const & a,
                                Vector3d const & b,
                                Vector3d const & n)
{
    Vector3d vxn = v.cross(n);
    if (vxn.dot(a) > 0.0 && vxn.dot(b) < 0.0) {
        // v is in the lune defined by the half great circle passing through
        // n and a and the half great circle passing through n and b, so p
        // is in the interior of the great circle segment from a to b. The
        // angle θ between p and v satisfies ‖v‖ ‖n‖ sin θ = |v·n|,
        // and ‖v‖ ‖n‖ cos θ = ‖v × n‖. The desired squared chord length is
        // 4 sin²(θ/2).
        double s = std::fabs(v.dot(n));
        double c = vxn.getNorm();
        double theta = (c == 0.0) ? 0.5 * PI : std::atan(s / c);
        double d = std::sin(0.5 * theta);
        return 4.0 * d * d;
    }
    return 4.0;
}

double getMaxSquaredChordLength(Vector3d const & v,
                                Vector3d const & a,
                                Vector3d const & b,
                                Vector3d const & n)
{
    Vector3d vxn = v.cross(n);
    if (vxn.dot(a) < 0.0 && vxn.dot(b) > 0.0) {
        // v is in the lune defined by the half great circle passing through
        // n and -a and the half great circle passing through n and -b, so p
        // is in the interior of the great circle segment from a to b. The
        // angle θ between p and v satisfies ‖v‖ ‖n‖ sin θ = |v·n|,
        // and ‖v‖ ‖n‖ cos θ = -‖v × n‖. The desired squared chord length is
        // 4 sin²(θ/2).
        double s = std::fabs(v.dot(n));
        double c = - vxn.getNorm();
        double d = std::sin(0.5 * std::atan2(s, c));
        return 4.0 * d * d;
    }
    return 0.0;
}

Vector3d getWeightedCentroid(UnitVector3d const & v0,
                             UnitVector3d const & v1,
                             UnitVector3d const & v2)
{
    // For the details, see:
    //
    // The centroid and inertia tensor for a spherical triangle
    // John E. Brock
    // 1974, Naval Postgraduate School, Monterey Calif.
    //
    // https://openlibrary.org/books/OL25493734M/The_centroid_and_inertia_tensor_for_a_spherical_triangle

    Vector3d x01 = v0.robustCross(v1); // twice the cross product of v0 and v1
    Vector3d x12 = v1.robustCross(v2);
    Vector3d x20 = v2.robustCross(v0);
    double s01 = 0.5 * x01.normalize(); // sine of the angle between v0 and v1
    double s12 = 0.5 * x12.normalize();
    double s20 = 0.5 * x20.normalize();
    double c01 = v0.dot(v1); // cosine of the angle between v0 and v1
    double c12 = v1.dot(v2);
    double c20 = v2.dot(v0);
    double a0 = (s12 == 0.0 && c12 == 0.0) ? 0.0 : std::atan2(s12, c12);
    double a1 = (s20 == 0.0 && c20 == 0.0) ? 0.0 : std::atan2(s20, c20);
    double a2 = (s01 == 0.0 && c01 == 0.0) ? 0.0 : std::atan2(s01, c01);
    return 0.5 * (x01 * a2 + x12 * a0 + x20 * a1);
}

}} // namespace lsst::sg
