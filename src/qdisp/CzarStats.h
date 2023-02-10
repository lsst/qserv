// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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
#ifndef LSST_QSERV_QDISP_CZARSTATS_H
#define LSST_QSERV_QDISP_CZARSTATS_H

// System headers
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <queue>
#include <sys/time.h>
#include <time.h>
#include <vector>

// qserv headers
#include "util/Histogram.h"
#include "util/Mutex.h"

// Third party headers
#include <nlohmann/json.hpp>

namespace lsst::qserv::qdisp {

class QdispPool;

/// &&& doc
class CzarStats : std::enable_shared_from_this<CzarStats> {
public:
    using Ptr = std::shared_ptr<CzarStats>;

    //&&&CzarStats() = delete;
    CzarStats(CzarStats const&) = delete;
    CzarStats& operator=(CzarStats const&) = delete;

    ~CzarStats() = default;

    /// &&& doc
    void setQdispPool(std::shared_ptr<qdisp::QdispPool> const& qdispPool);

    /// Setup the global CzarStats instance
    /// @throws Bug if global has already been set or qdispPool is null.
    static void setup(std::shared_ptr<qdisp::QdispPool> const& qdispPool);

    /// Return a pointer to the global CzarStats instance.
    /// @throws Bug if get() is called before setup()
    static Ptr get();

    /// Add a bytes per second entry
    void addTrmitRecvRate(double bytesPerSec);
    //&&&void addTrmitRecvTime();

    /// &&& doc
    void addQueryRespConcurrentSetup(int64_t val) { _queryRespConcurrentSetup += val; }
    /// &&& doc
    void addQueryRespConcurrentWait(int64_t val) { _queryRespConcurrentWait += val; }
    /// &&& doc
    void addQueryRespConcurrentProcessing(int64_t val) { _queryRespConcurrentProcessing += val; }

    /// Get a json object describing the current state of the query dispatch thread pool.
    nlohmann::json getQdispStatsJson() const;

    /// Get a json object describing the current transmit/merge stats for this czar.
    nlohmann::json getTransmitStatsJson() const;

private:
    //&&&CzarStats(std::shared_ptr<qdisp::QdispPool> const& qdispPool);
    CzarStats();
    static Ptr _globalCzarStats;    ///< Pointer to the global instance.
    static util::Mutex _globalMtx;  ///< Protects `_globalCzarStats`

    /// Connection to get information about the czar's pool of dispatch threads.
    std::shared_ptr<qdisp::QdispPool> _qdispPool;

    /// Histogram for tracking  receive rate in bytes per second.
    util::HistogramRolling::Ptr _histTrmitRecvRate;

    std::atomic<int64_t> _queryRespConcurrentSetup{0};       ///< Number of request currently being setup
    std::atomic<int64_t> _queryRespConcurrentWait{0};        ///< Number of requests currently waiting
    std::atomic<int64_t> _queryRespConcurrentProcessing{0};  ///< Number of requests currently processing
};

/// RAII class to help track a changing sum through a begin and end time.
template <typename TType>
class TimeCountTracker {
public:
    using Ptr = std::shared_ptr<TimeCountTracker>;
    using CLOCK = std::chrono::system_clock;
    using TIMEPOINT = std::chrono::time_point<CLOCK>;
    using CALLBACKFUNC = std::function<void(TIMEPOINT start, TIMEPOINT end, TType sum, bool success)>;
    TimeCountTracker() = delete;

    /// doc &&&
    TimeCountTracker(CALLBACKFUNC callback) : _callback(callback) {
        auto now = CLOCK::now();
        _startTime = now;
        _endTime = now;
    }

    /// doc &&&
    ~TimeCountTracker() {
        TType sum;
        {
            std::lock_guard lg(_mtx);
            _endTime = CLOCK::now();
            sum = _sum;
        }
        _callback(_startTime, _endTime, sum, _success);
    }

    /// doc &&&
    void addToValue(double val) {
        std::lock_guard lg(_mtx);
        _sum += val;
    }

    /// doc &&&
    void setSuccess() { _success = true; }

private:
    TIMEPOINT _startTime;
    TIMEPOINT _endTime;
    TType _sum = 0;  ///< atomic double doesn't support +=
    std::atomic<bool> _success{false};
    CALLBACKFUNC _callback;
    std::mutex _mtx;
};

}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_QDISP_CZARSTATS_H
