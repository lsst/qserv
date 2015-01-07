// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 AURA/LSST.
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
  * @brief (obsolete)SWIG-exported interface to dispatching queries.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "ccontrol/dispatcher.h"

// System headers
#include <fstream>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "query/Constraint.h"

namespace lsst {
namespace qserv {
namespace ccontrol {
#if 0
struct mergeStatus {
    mergeStatus(bool& success, bool shouldPrint_=false, int firstN_=5)
        : isSuccessful(success), shouldPrint(shouldPrint_),
          firstN(firstN_) {
        isSuccessful = true;
    }
    void operator() (AsyncQueryManager::Result const& x) {
        if(!x.second.isSuccessful()) {
            if(shouldPrint || (firstN > 0)) {
                LOGF_INFO("Chunk %1% error " % x.first);
                LOGF_INFO("open: %1% qWrite: %2% read: %3% lWrite: %4%"
                          % x.second.open % x.second.queryWrite
                          % x.second.read % x.second.localWrite);
                --firstN;
            }
            isSuccessful = false;
        } else {
            if(shouldPrint) {
                LOGF_INFO("Chunk %1% OK (%2%)\t"
                          % x.first % x.second.localWrite);
            }
        }
    }
    bool& isSuccessful;
    bool shouldPrint;
    int firstN;
};
#endif

query::Constraint
getC(int base) {
    // SWIG test.
    std::stringstream ss;
    query::Constraint c;
    ss << "box" << base; c.name = ss.str(); ss.str("");
    ss << base << "1"; c.params.push_back(ss.str()); ss.str("");
    ss << base << "2"; c.params.push_back(ss.str()); ss.str("");
    ss << base << "3"; c.params.push_back(ss.str()); ss.str("");
    ss << base << "4"; c.params.push_back(ss.str()); ss.str("");
    return c; // SWIG test.
 }

}}} // namespace lsst::qserv::ccontrol
