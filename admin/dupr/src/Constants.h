/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/// \file
/// \brief Broadly useful constants and defines.

#ifndef LSST_QSERV_ADMIN_DUPR_CONSTANTS_H
#define LSST_QSERV_ADMIN_DUPR_CONSTANTS_H

/// Maximum supported size of a line of text (bytes).
#ifndef MAX_LINE_SIZE
#   define MAX_LINE_SIZE 65536 - 24
#endif

/// Maximum supported size of a CSV field (bytes).
#ifndef MAX_FIELD_SIZE
#   define MAX_FIELD_SIZE 255
#endif

#if MAX_FIELD_SIZE >= MAX_LINE_SIZE
#   error Maximum field size must be strictly less than the maximum line size.
#endif

/// Size in bytes of a cache-line.
#ifndef CACHE_LINE_SIZE
#   define CACHE_LINE_SIZE 64
#endif


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

size_t const MiB = 1024*1024;
size_t const GiB = 1024*MiB;

double const DEG_PER_RAD = 57.2957795130823208767981548141;   ///< 180/π
double const RAD_PER_DEG = 0.0174532925199432957692369076849; ///< π/180
double const EPSILON_DEG = 0.001 / 3600;                      ///< 1 mas

/// Maximum HTM subdivision level such that an ID requires less than 32 bits.
int const HTM_MAX_LEVEL = 13;

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_CONSTANTS_H

