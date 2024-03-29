// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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

#ifndef LSST_QSERV_CSS_SCANTABLEPARAMS_H
#define LSST_QSERV_CSS_SCANTABLEPARAMS_H

#include <string>

namespace lsst::qserv::css {

/// A container for shared scan-table metadata.
struct ScanTableParams {
    ScanTableParams() {}
    ScanTableParams(bool lockInMem_, int scanRating_) : lockInMem(lockInMem_), scanRating(scanRating_) {}

    bool lockInMem{false};  ///< True if table should be locked in memory for shared scan
    int scanRating{0};      ///< Speed of shared scan. 1-fast, 2-medium, 3-slow
};

}  // namespace lsst::qserv::css

#endif  // LSST_QSERV_CSS_SCANTABLEPARAMS_H
