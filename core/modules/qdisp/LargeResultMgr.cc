// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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

// Class header
#include "qdisp/LargeResultMgr.h"

// System headers

// Third-party headers
#include "XrdSsi/XrdSsiRequest.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.LargeResultMgr");

}

namespace lsst {
namespace qserv {
namespace qdisp {


/// Set the value of the xrootd semaphore for tracking large response blocks to start.
void LargeResultMgr::_setup() {
    for (int j = 0; j < _runningCountMax; ++j) {
        auto rdrInfo = XrdSsiRequest::RestartDataResponse(XrdSsiRequest::RDR_Post);
        LOGS(_log, LOG_LVL_DEBUG, "LargeResultMgr::_setup runningCountMax" << _runningCountMax
                << " rdrInfo[qCount=" << rdrInfo.qCount << " rCount=" << rdrInfo.rCount
                << " iAllow=" << rdrInfo.iAllow << " fAllow=" << rdrInfo.fAllow << "]");
    }
}


/// Check if any large result blocks can be run.
void LargeResultMgr::startBlock(std::string const& jobId) {
    int count = ++_blockCount;
    LOGS(_log, LOG_LVL_DEBUG, jobId << " LargeResultMgr::start blocks=" << count);
}


/// Decrement the number of running blocks and see if any can be started
void LargeResultMgr::finishBlock(std::string const& jobId) {
    int count = --_blockCount;
    auto rdrInfo = XrdSsiRequest::RestartDataResponse(XrdSsiRequest::RDR_Post);
    LOGS(_log, LOG_LVL_DEBUG, jobId << " LargeResultMgr::finish blocks=" << count
            << " rdrInfo[qCount=" << rdrInfo.qCount << " rCount=" << rdrInfo.rCount
            << " iAllow=" << rdrInfo.iAllow << " fAllow=" << rdrInfo.fAllow << "]");
}


}}} // namespace lsst::qserv::qdisp
