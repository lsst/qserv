// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

/**
  * @file
  *
  * @brief Empty-chunks tracker. Reads an on-disk file from cwd, but
  * should ideally query (and cache) table state.
  *
  * @Author Daniel L. Wang, SLAC
  */

#ifndef LSST_QSERV_CSS_EMPTYCHUNKS_H
#define LSST_QSERV_CSS_EMPTYCHUNKS_H

// System headers
// #include <iostream>
// #include <string>
// #include <vector>

// Third-party headers
#include "boost/shared_ptr.hpp"
#include "boost/thread/mutex.hpp"

// Local headers
#include "global/intTypes.h"
// #include "css/KvInterface.h"
// #include "css/MatchTableParams.h"
// #include "css/StripingParams.h"

namespace lsst {
namespace qserv {
namespace css {

/// High-level empty-chunk-tracking class. Tracks empty chunks
/// per-database. In the future, we will likely migrate to a
/// per-partitioning-group scheme, at which point, we will re-think
/// the db-based dispatch as well (user tables in the partitioning
/// group may be extremely sparse).
class EmptyChunks {
public:
    EmptyChunks(std::string const& path=".") {}

    // accessors

    /// @return set of empty chunks for this db
    boost::shared_ptr<IntSet const> getEmpty(std::string const& db);

    /// @return true if db/chunk is empty
    bool isEmpty(std::string const& db, int chunk);

    // Convenience types
    typedef boost::shared_ptr<IntSet> IntSetPtr;
    typedef boost::shared_ptr<IntSet const> IntSetConstPtr;

private:
    typedef std::map<std::string, IntSetPtr> IntSetMap;

    std::string _path; ///< Search path for empty chunks files
    IntSetMap _sets; ///< Container for empty chunks sets
    boost::mutex _setsMutex;
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_EMPTYCHUNKS_H
