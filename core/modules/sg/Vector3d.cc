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
/// \brief This file contains the Vector3d class implementation.

#include "Vector3d.h"

#include <cstdio>
#include <ostream>

#include "Angle.h"
#include "UnitVector3d.h"


namespace lsst {
namespace sg {

double Vector3d::normalize() {
    double scale = 1.0;
    double invScale = 1.0;
    double n2 = getSquaredNorm();
    if (n2 < 4.008336720017946e-292) {
        // If n2 is below 2^(-1022 + 54), i.e. close to the smallest normal
        // double precision value, scale each component by 2^563 and
        // recompute the squared norm.
        scale = 3.019169939857233e+169;
        invScale = 3.312168642111238e-170;
        n2 = ((*this) * scale).getSquaredNorm();
        if (n2 == 0.0) {
            throw std::runtime_error("Cannot normalize zero vector");
        }
    } else if (n2 == std::numeric_limits<double>::infinity()) {
        // In case of overflow, scale each component by 2^-513 and
        // recompute the squared norm.
        scale = 3.7291703656001034e-155;
        invScale = 2.6815615859885194e+154;
        n2 = ((*this) * scale).getSquaredNorm();
    }
    double norm = std::sqrt(n2);
    _v[0] = (_v[0] * scale) / norm;
    _v[1] = (_v[1] * scale) / norm;
    _v[2] = (_v[2] * scale) / norm;
    return norm * invScale;
}

Vector3d Vector3d::rotatedAround(UnitVector3d const & k, Angle a) const {
    // Use Rodrigues' rotation formula.
    Vector3d const & v = *this;
    double s = sin(a);
    double c = cos(a);
    return v * c + k.cross(v) * s + k * (k.dot(v) * (1.0 - c));
}

std::ostream & operator<<(std::ostream & os, Vector3d const & v) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "Vector3d(%.17g, %.17g, %.17g)",
                  v.x(), v.y(), v.z());
    return os << buf;
}

}} // namespace lsst::sg
