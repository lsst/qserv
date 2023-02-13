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
// Generic timer class

// Class header
#include "qdisp/CzarStats.h"

#include <cstdio>
#include <float.h>
#include <set>

// qserv headers
#include "qdisp/QdispPool.h"
#include "util/Bug.h"

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

CzarStats::CzarStats(qdisp::QdispPool::Ptr const& qdispPool) : _qdispPool(qdispPool) {
    _histTrmitRecvRate = util::HistogramRolling::Ptr(new util::HistogramRolling(
            "TransmitRecvRateBytesPerSec", {1'000, 1'000'000, 500'000'000, 1'000'000'000}, 1h, 10000));
    auto bucketVals = {0.1, 1.0, 10.0, 100.0, 1000.0};
    _histRespSetup =
            util::HistogramRolling::Ptr(new util::HistogramRolling("RespSetupTime", bucketVals, 1h, 10000));
    _histRespWait =
            util::HistogramRolling::Ptr(new util::HistogramRolling("RespWaitTime", bucketVals, 1h, 10000));
    _histRespProcessing = util::HistogramRolling::Ptr(
            new util::HistogramRolling("RespProcessingTime", bucketVals, 1h, 10000));
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

void CzarStats::addTrmitRecvRate(double bytesPerSec) {
    _histTrmitRecvRate->addEntry(bytesPerSec);
    LOGS(_log, LOG_LVL_TRACE,
         "czarstats::addTrmitRecvRate " << bytesPerSec << " " << _histTrmitRecvRate->getString("")
                                        << " jsonA=" << getTransmitStatsJson()
                                        << " jsonB=" << getQdispStatsJson());
}

nlohmann::json CzarStats::getQdispStatsJson() const {
    nlohmann::json js;
    js["QdispPool"] = _qdispPool->getJson();
    js["queryRespConcurrentSetupCount"] = static_cast<int16_t>(_queryRespConcurrentSetup);
    js["queryRespConcurrentWaitCount"] = static_cast<int16_t>(_queryRespConcurrentWait);
    js["queryRespConcurrentProcessingCount"] = static_cast<int16_t>(_queryRespConcurrentProcessing);
    js["histRespSetup"] = _histRespSetup->getJson();
    js["histRespWait"] = _histRespWait->getJson();
    js["histRespProcessing"] = _histRespProcessing->getJson();
    return js;
}

nlohmann::json CzarStats::getTransmitStatsJson() const {
    nlohmann::json js;
    js["TransmitRecvRate"] = _histTrmitRecvRate->getJson();
    return js;
}

}  // namespace lsst::qserv::qdisp
