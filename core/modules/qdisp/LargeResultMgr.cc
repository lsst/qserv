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

/// Check if any large result blocks can be run.
void LargeResultMgr::startBlock() {
    LOGS(_log, LOG_LVL_DEBUG, "LargeResultMgr::start");
    std::lock_guard<std::mutex> lg(_mx);
    _restartSome();
}


/// Decrement the number of running blocks and see if any can be started
void LargeResultMgr::finishBlock() {
    LOGS(_log, LOG_LVL_DEBUG, "LargeResultMgr::finishBlock runningCount=" << _runningCount);
    std::lock_guard<std::mutex> lg(_mx);
    --_runningCount;
    _restartSome();
}


/// Start large request blocks until the queue is empty or maximum number are running.
/// Precondition: _mx must be held before calling this procedure.
void LargeResultMgr::_restartSome() {
    XrdSsiRequest::RDR_Info rdrInfo = XrdSsiRequest::RestartDataResponse(XrdSsiRequest::RDR_Query);
    LOGS(_log, LOG_LVL_DEBUG, "LargeResultMgr::_restartSome begin runningCount=" << _runningCount
            << " rdrInfo[qCount=" << rdrInfo.qCount << " rCount=" << rdrInfo.rCount
            << " iAllow=" << rdrInfo.iAllow << " fAllow=" << rdrInfo.fAllow << "]");
    bool nothingToDo = false;
    while (!nothingToDo && _runningCount < _runningCountMax
           && (rdrInfo.qCount > 0 || (rdrInfo.qCount == 0 && rdrInfo.iAllow == 0))) {
        rdrInfo = XrdSsiRequest::RestartDataResponse(XrdSsiRequest::RDR_Post);
        LOGS(_log, LOG_LVL_DEBUG, "LargeResultMgr::_restartSome Post runningCount=" << _runningCount
                    << " rdrInfo[qCount=" << rdrInfo.qCount << " rCount=" << rdrInfo.rCount
                    << " iAllow=" << rdrInfo.iAllow << " fAllow=" << rdrInfo.fAllow << "]");
        int restarted = rdrInfo.rCount;
        nothingToDo = (restarted == 0); // nothing more to do if nothing was restarted.
        _runningCount += restarted;
    }
    LOGS(_log, LOG_LVL_DEBUG, "LargeResultMgr::_restartSome end runningCount=" << _runningCount
            << " rdrInfo[qCount=" << rdrInfo.qCount << " rCount=" << rdrInfo.rCount
            << " iAllow=" << rdrInfo.iAllow << " fAllow=" << rdrInfo.fAllow << "]");
}

}}} // namespace lsst::qserv::qdisp
