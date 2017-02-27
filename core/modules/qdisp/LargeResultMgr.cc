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
    std::lock_guard<std::mutex> lck(_mtx);
    for (int j = 0; j < _runningCountMax; ++j) {
        _post("initialization");
    }
}


/// Check if any large result blocks can be run.
void LargeResultMgr::startBlock(std::string const& jobId) {
    std::lock_guard<std::mutex> lck(_mtx);
    ++_blockCount;
    LOGS(_log, LOG_LVL_DEBUG, jobId << " LargeResultMgr::start blockCount=" << _blockCount);
}


/// Decrement the number of running blocks and see if any can be started
void LargeResultMgr::finishBlock(std::string const& jobId) {
    std::lock_guard<std::mutex> lck(_mtx);
    --_blockCount;
    if (_delayAllPosts) {
        _delayPost(jobId);
    } else {
        _post(jobId);
        _freeDelayedPosts();
    }
}


/// Increase the count of user queries going out and possible delay large results.
void LargeResultMgr::incrOutGoingQueries() {
    std::lock_guard<std::mutex> lck(_mtx);
    ++_outGoingQueries;
    LOGS(_log, LOG_LVL_DEBUG, "incrOutGoingQueries outGoingQueries=" << _outGoingQueries);
    _setDelayAllPosts(true);
}


/// Decrease the count of outgoing queries and possibly allow incoming large results to continue.
void LargeResultMgr::decrOutGoingQueries() {
    std::lock_guard<std::mutex> lck(_mtx);
    --_outGoingQueries;
    LOGS(_log, LOG_LVL_DEBUG, "decrOutGoingQueries outGoingQueries=" << _outGoingQueries);
    // Allow some large result processing whenever a query finishes being sent out.
    _freeDelayedPosts();
    _setDelayAllPosts(_outGoingQueries > 0);
}


/// If delayAll is true, all incoming large results will be delayed
void LargeResultMgr::_setDelayAllPosts(bool delayAll) {
    /// Must hold _mtx when calling this function.
    if (delayAll == _delayAllPosts) return;
    _delayAllPosts = delayAll;
    LOGS(_log, LOG_LVL_INFO, " change delayAll=" << _delayAllPosts);
    if (!_delayAllPosts) {
        _freeDelayedPosts();
    }
}


/// Increment xrootd's large result semaphore to allow another large result to run.
/// Must hold _mtx when calling this function.
void LargeResultMgr::_post(std::string const& jobId) {
    auto rdrInfo = XrdSsiRequest::RestartDataResponse(XrdSsiRequest::RDR_Post);
    LOGS(_log, LOG_LVL_DEBUG, jobId << " LargeResultMgr::finish blocks=" << _blockCount
            << " rdrInfo[qCount=" << rdrInfo.qCount << " rCount=" << rdrInfo.rCount
            << " iAllow=" << rdrInfo.iAllow << " fAllow=" << rdrInfo.fAllow << "]");
}


/// Track that a post has been delayed.
/// Must hold _mtx when calling this function.
void LargeResultMgr::_delayPost(std::string const& jobid) {
    ++_delayedPosts;
    LOGS(_log, LOG_LVL_DEBUG, jobid << " delayedPosts=" << _delayedPosts);
}


/// Free some number of delayed posts
/// Must hold _mtx when calling this function.
void LargeResultMgr::_freeDelayedPosts() {
    LOGS(_log, LOG_LVL_DEBUG, "freeDelayedPosts delayedPosts=" << _delayedPosts);
    while (_delayedPosts > 0) {
        _post("freeDelayedPosts");
        --_delayedPosts;
    }
}


}}} // namespace lsst::qserv::qdisp
