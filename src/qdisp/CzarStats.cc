// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#include "qdisp/CzarStats.h"

// System headers
#include <chrono>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "qdisp/QdispPool.h"
#include "util/Bug.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::chrono_literals;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.CzarStats");
}

namespace lsst::qserv::qdisp {

CzarStats::Ptr CzarStats::_globalCzarStats;
util::Mutex CzarStats::_globalMtx;

void CzarStats::setup(qdisp::QdispPool::Ptr const& qdispPool) {
    std::lock_guard<util::Mutex> lg(_globalMtx);
    if (_globalCzarStats != nullptr || qdispPool == nullptr) {
        throw util::Bug(ERR_LOC, "Error CzarStats::setup called after global pointer set or qdispPool=null.");
    }
    _globalCzarStats = Ptr(new CzarStats(qdispPool));
}

CzarStats::CzarStats(qdisp::QdispPool::Ptr const& qdispPool)
        : _qdispPool(qdispPool), _startTimeMs(util::TimeUtils::now()) {
    auto bucketValsRates = {128'000.0,       512'000.0,       1'024'000.0,     16'000'000.0,
                            128'000'000.0,   256'000'000.0,   512'000'000.0,   768'000'000.0,
                            1'000'000'000.0, 2'000'000'000.0, 4'000'000'000.0, 8'000'000'000.0};
    _histXRootDSSIRecvRate = util::HistogramRolling::Ptr(
            new util::HistogramRolling("XRootDSSIRecvRateBytesPerSec", bucketValsRates, 1h, 10000));
    _histMergeRate = util::HistogramRolling::Ptr(
            new util::HistogramRolling("MergeRateBytesPerSec", bucketValsRates, 1h, 10000));
    _histFileReadRate = util::HistogramRolling::Ptr(
            new util::HistogramRolling("FileReadRateBytesPerSec", bucketValsRates, 1h, 10000));
    auto bucketValsTimes = {0.1, 1.0, 10.0, 100.0, 1000.0};
    _histRespSetup = util::HistogramRolling::Ptr(
            new util::HistogramRolling("RespSetupTime", bucketValsTimes, 1h, 10000));
    _histRespWait = util::HistogramRolling::Ptr(
            new util::HistogramRolling("RespWaitTime", bucketValsTimes, 1h, 10000));
    _histRespProcessing = util::HistogramRolling::Ptr(
            new util::HistogramRolling("RespProcessingTime", bucketValsTimes, 1h, 10000));
}

CzarStats::Ptr CzarStats::get() {
    std::lock_guard<util::Mutex> lg(_globalMtx);
    if (_globalCzarStats == nullptr) {
        throw util::Bug(ERR_LOC, "Error CzarStats::get called before CzarStats::setup.");
    }
    return _globalCzarStats;
}

void CzarStats::endQueryRespConcurrentSetup(TIMEPOINT start, TIMEPOINT end) {
    --_queryRespConcurrentSetup;
    std::chrono::duration<double> secs = end - start;
    _histRespSetup->addEntry(end, secs.count());
}

void CzarStats::endQueryRespConcurrentWait(TIMEPOINT start, TIMEPOINT end) {
    --_queryRespConcurrentWait;
    std::chrono::duration<double> secs = end - start;
    _histRespWait->addEntry(end, secs.count());
}

void CzarStats::endQueryRespConcurrentProcessing(TIMEPOINT start, TIMEPOINT end) {
    --_queryRespConcurrentProcessing;
    std::chrono::duration<double> secs = end - start;
    _histRespProcessing->addEntry(end, secs.count());
}

void CzarStats::addXRootDSSIRecvRate(double bytesPerSec) {
    _histXRootDSSIRecvRate->addEntry(bytesPerSec);
    LOGS(_log, LOG_LVL_TRACE,
         "CzarStats::" << __func__ << " " << bytesPerSec << " " << _histXRootDSSIRecvRate->getString(""));
}

void CzarStats::addMergeRate(double bytesPerSec) {
    _histMergeRate->addEntry(bytesPerSec);
    LOGS(_log, LOG_LVL_TRACE,
         "CzarStats::" << __func__ << " " << bytesPerSec << " " << _histMergeRate->getString("")
                       << " jsonA=" << getTransmitStatsJson() << " jsonB=" << getQdispStatsJson());
}

void CzarStats::addFileReadRate(double bytesPerSec) {
    _histFileReadRate->addEntry(bytesPerSec);
    LOGS(_log, LOG_LVL_TRACE,
         "CzarStats::" << __func__ << " " << bytesPerSec << " " << _histFileReadRate->getString(""));
}

nlohmann::json CzarStats::getQdispStatsJson() const {
    nlohmann::json result;
    result["QdispPool"] = _qdispPool->getJson();
    result["queryRespConcurrentSetupCount"] = _queryRespConcurrentSetup.load();
    result["queryRespConcurrentWaitCount"] = _queryRespConcurrentWait.load();
    result["queryRespConcurrentProcessingCount"] = _queryRespConcurrentProcessing.load();
    result[_histRespSetup->label()] = _histRespSetup->getJson();
    result[_histRespWait->label()] = _histRespWait->getJson();
    result[_histRespProcessing->label()] = _histRespProcessing->getJson();
    result["totalQueries"] = _totalQueries.load();
    result["totalJobs"] = _totalJobs.load();
    result["totalResultFiles"] = _totalResultFiles.load();
    result["totalResultMerges"] = _totalResultMerges.load();
    result["totalBytesRecv"] = _totalBytesRecv.load();
    result["totalRowsRecv"] = _totalRowsRecv.load();
    result["numQueries"] = _numQueries.load();
    result["numJobs"] = _numJobs.load();
    result["numResultFiles"] = _numResultFiles.load();
    result["numResultMerges"] = _numResultMerges.load();
    result["startTimeMs"] = _startTimeMs;
    result["snapshotTimeMs"] = util::TimeUtils::now();
    return result;
}

nlohmann::json CzarStats::getTransmitStatsJson() const {
    nlohmann::json result;
    result[_histXRootDSSIRecvRate->label()] = _histXRootDSSIRecvRate->getJson();
    result[_histMergeRate->label()] = _histMergeRate->getJson();
    result[_histFileReadRate->label()] = _histFileReadRate->getJson();
    return result;
}

}  // namespace lsst::qserv::qdisp
