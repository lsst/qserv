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
/// XrdSsiRequest keeps a semaphore of how many blocks can be started.
/// Every time it starts a held block, it decrements the semaphore. When
/// this program finishes a block it increases the semaphore with
/// RDR_Post.
class LargeResultMgr {
public:
    using Ptr = std::shared_ptr<LargeResultMgr>;

    LargeResultMgr(int runningCountMax) : _runningCountMax{runningCountMax} { _setup(); };
    LargeResultMgr() { _setup(); };

    void startBlock(std::string const& jobId);
    void finishBlock(std::string const& jobId);

private:
    void _setup();

    std::atomic<int> _blockCount{0}; ///< Number of large result blocks in the system.
    std::atomic<int> _runningCountMax{1}; ///< Max number of large result blocks to run concurrently.
};


}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_LARGERESULTMGR_H

