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

#ifndef LSST_SG_CONSTANTS_H_
#define LSST_SG_CONSTANTS_H_

/// \file
/// \brief This file contains common constants.


namespace lsst {
namespace sg {

// Note: given a compiler that does correctly rounded decimal to
// binary floating point conversions, PI = 0x1.921fb54442d18p1 in
// IEEE double precision format. This is less than π.
static double const PI = 3.1415926535897932384626433832795;
static double const RAD_PER_DEG = 0.0174532925199432957692369076849;
static double const DEG_PER_RAD = 57.2957795130823208767981548141;

// The maximum error of std::asin in IEEE double precision arithmetic,
// assuming 1 ulp of error in its argument, is about
// π/2 - arcsin (1 - 2⁻⁵³), or a little less than 1.5e-8 radians
// (3.1 milliarcsec).
static double const MAX_ASIN_ERROR = 1.5e-8;

// The computation of squared chord length between two unit vectors
// involves 8 elementary operations on numbers with magnitude ≤ 4. Its
// maximum error can be shown to be < 2.5e-15.
static double const MAX_SCL_ERROR = 2.5e-15;

}} // namespace lsst::sg

#endif // LSST_SG_CONSTANTS_H_
