/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
 *
 */

#ifndef LSST_QSERV_QDISP_LARGERESULTMGR_H
#define LSST_QSERV_QDISP_LARGERESULTMGR_H

// System headers
#include <atomic>
#include <memory>
#include <mutex>

// Qserv headers

namespace lsst {
namespace qserv {
namespace qdisp {

/// This class is used to manage how many large result blocks are being handled
/// at any given time on a czar. This should keep large results from
/// bogging down the czar and allow new queries to be sent out.
///
/// This uses xrootd's XrdSsiRequest::RestartDataResponse function and
/// the ProcessResponseData callback return value (see QueryRequest::ProcessResponseData).
///
/// - For any job, any data block beyond _blockCountThreshold is 'held' by
///   xrootd by having the QueryRequest::ProcessResponseData function return PRD_HOLD.
///   xrootd will not spend any communication resources on that block until
///   it is restarted by a call to XrdSsiRequest::RestartDataResponse.
///
/// - XrdSsiRequest::RestartDataResponse with RDR_Post increases the number of
///   'held' blocks that can be restarted by xrootd by 1, and the function
///   returns the number of blocks it restarted, which is added to _runningCount.
///
/// - Since the signal to 'hold' a block is the return value of a xrootd callback,
///   xrootd checks if any 'held' blocks can be started when ProcessResponseData
///   returns. So, XrdSsiRequest::RestartDataResponse(RDR_Post) needs to be called
///   inside ProcessResponseData function whenever it is going to return PRD_HOLD.
///
/// - The value of _runningCount needs to be handled PERFECTLY. Any problems
///   eventually either bog down the system as it tries to handle too many
///   large result blocks at once, or not allow any large responses to run,
///   never allowing a large response to finish.
///   Good conditions for a reset of _runningCount would be extremely useful.
///
class LargeResultMgr {
public:
    using Ptr = std::shared_ptr<LargeResultMgr>;

    LargeResultMgr(int runningCountMax) : _runningCountMax{runningCountMax} {};

    int getBlockCountThreshold() {return _blockCountThreshold;}
    void startBlock();
    void finishBlock();

private:
    void _restartSome();

    std::mutex _mx;
    int _runningCount{0}; ///< Number of large result blocks being run.
    int _runningCountMax{1}; ///< Max number of large result blocks that should be running at one time.

    std::atomic<int> _blockCountThreshold{1}; ///< Number of blocks that can be read before a query is a large result. &&& make this a constant

};


}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_LARGERESULTMGR_H

