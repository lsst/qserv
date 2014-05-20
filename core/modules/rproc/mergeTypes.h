// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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

#ifndef LSST_QSERV_RPROC_MERGETYPES_H
#define LSST_QSERV_RPROC_MERGETYPES_H
/**
  * @file
  *
  * @brief MergeFixup is a value class for exchanging query merge
  * information from Python to C++. It has not been streamlined with
  * the newer query model.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <string>

namespace lsst {
namespace qserv {
namespace rproc {

/// class MergeFixup - A value class that specifies the SQL predicates
/// to use when merging subqueries into final results.
class MergeFixup {
public:
    MergeFixup(std::string select_,
               std::string post_,
               std::string orderBy_,
               int limit_,
               bool needsFixup_)
        : select(select_), post(post_),
          orderBy(orderBy_), limit(limit_),
          needsFixup(needsFixup_)
    {}
    MergeFixup() : limit(-1), needsFixup(false) {}

    std::string select;
    std::string post;
    std::string orderBy;
    int limit;
    bool needsFixup;
};

}}} // namespace lsst::qserv::rproc

#endif // LSST_QSERV_RPROC_MERGETYPES_H
