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

#ifndef LSST_SG_ORIENTATION_H_
#define LSST_SG_ORIENTATION_H_

/// \file
/// \brief This file declares functions for orienting points on the sphere.

#include "UnitVector3d.h"


namespace lsst {
namespace sg {

/// `orientationExact` computes and returns the orientations of 3 vectors a, b
/// and c, which need not be normalized but are assumed to have finite
/// components. The return value is +1 if the vectors a, b, and c are in
/// counter-clockwise orientation, 0 if they are coplanar, colinear, or
/// identical, and -1 if they are in clockwise orientation. The implementation
/// uses arbitrary precision arithmetic to avoid floating point rounding error,
/// underflow and overflow.
int orientationExact(Vector3d const & a,
                     Vector3d const & b,
                     Vector3d const & c);

/// `orientation` computes and returns the orientations of 3 unit vectors a, b
/// and c. The return value is +1 if the vectors a, b, and c are in counter-
/// clockwise orientation, 0 if they are coplanar, colinear or identical, and
/// -1 if they are in clockwise orientation.
///
/// This is equivalent to computing the scalar triple product a · (b x c),
/// which is the sign of the determinant of the 3x3 matrix with a, b
/// and c as columns/rows.
///
/// The implementation proceeds by first computing a double precision
/// approximation, and then falling back to arbitrary precision arithmetic
/// when necessary. Consequently, the result is exact.
int orientation(UnitVector3d const & a,
                UnitVector3d const & b,
                UnitVector3d const & c);

}} // namespace lsst::sg

#endif // LSST_SG_ORIENTATION_H_
