// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_CSS_STRIPINGPARAMS_H
#define LSST_QSERV_CSS_STRIPINGPARAMS_H

namespace lsst {
namespace qserv {
namespace css {

class StripingParams {
public:
    StripingParams() : stripes(0), subStripes(0), partitioningId(0), overlap(0.0) {}
    StripingParams(int stripes_, int subStripes_, int partitioningId_, double overlap_) :
        stripes(stripes_), subStripes(subStripes_), partitioningId(partitioningId_),
        overlap(overlap_){}
    int stripes;
    int subStripes;
    int partitioningId;
    double overlap;     // default overlap for tables that do not specify their own overlap
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_STRIPINGPARAMS_H
