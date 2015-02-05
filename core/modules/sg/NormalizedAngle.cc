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
/// \brief This file contains the NormalizedAngle class implementation.

#include "NormalizedAngle.h"

#include "LonLat.h"
#include "Vector3d.h"


namespace lsst {
namespace sg {

NormalizedAngle NormalizedAngle::between(NormalizedAngle const & a,
                                         NormalizedAngle const & b)
{
    NormalizedAngle x;
    double a1 = std::fabs(a.asRadians() - b.asRadians());
    double a2 = 2.0 * PI - a1;
    x._a = Angle(std::min(a1, a2));
    return x;
}

NormalizedAngle NormalizedAngle::center(NormalizedAngle const & a,
                                        NormalizedAngle const & b)
{
    NormalizedAngle x;
    double c = 0.5 * (a.asRadians() + b.asRadians());
    if (a <= b) {
        x._a = Angle(c);
    } else {
        // The result is (a + b + 2π) / 2, normalized to [0, 2π)
        x._a = Angle((c < PI) ? (c + PI) : (c - PI));
    }
    return x;
}

NormalizedAngle::NormalizedAngle(LonLat const & p1, LonLat const & p2) {
    double x = sin((p1.getLon() - p2.getLon()) * 0.5);
    x *= x;
    double y = sin((p1.getLat() - p2.getLat()) * 0.5);
    y *= y;
    double z = cos((p1.getLat() + p2.getLat()) * 0.5);
    z *= z;
    // Compute the square of the sine of half of the desired angle. This is
    // easily shown to be be one fourth of the squared Euclidian distance
    // (chord length) between p1 and p2.
    double sha2 = (x * (z - y) + y);
    // Avoid domain errors in asin and sqrt due to rounding errors.
    if (sha2 < 0.0) {
        _a = Angle(0.0);
    } else if (sha2 >= 1.0) {
        _a = Angle(PI);
    } else {
        _a = Angle(2.0 * std::asin(std::sqrt(sha2)));
    }
}

NormalizedAngle::NormalizedAngle(Vector3d const & v1, Vector3d const & v2) {
    double s = v1.cross(v2).getNorm();
    double c = v1.dot(v2);
    if (s == 0.0 && c == 0.0) {
        // Avoid the atan2(±0, -0) = ±PI special case.
        _a = Angle(0.0);
    } else {
        _a = Angle(std::atan2(s, c));
    }
}

}} // namespace lsst::sg
