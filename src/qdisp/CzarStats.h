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
#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <queue>
#include <sys/time.h>
#include <time.h>
#include <vector>
#include <unordered_map>

// Qserv headers
#include "global/intTypes.h"
#include "util/Histogram.h"
#include "util/Mutex.h"
#include "util/Raii.h"

// Third party headers
#include <nlohmann/json.hpp>

namespace lsst::qserv::util {
class QdispPool;
}

namespace lsst::qserv::qdisp {

class CzarStats;

class CzarStatsRaii {
public:
    CzarStatsRaii(std::shared_ptr<CzarStats> const& czarStats, std::atomic<int64_t>& count)
    : counter(count), czarStats(czarStats)  {
        ++counter;
    }
    virtual ~CzarStatsRaii() { --counter; }
protected:
    std::atomic<int64_t>& counter;
    std::shared_ptr<CzarStats> czarStats;  ///< This keeps _count valid.
};



/// This class is used to track statistics for the czar.
/// setup() needs to be called before get().
/// The primary information stored is for the QdispPool of threads and queues,
/// which is a good indicator of how much work the czar needs to do,
/// and with some knowledge of the priorities, what kind of work the czar
/// is trying to do.
/// It also tracks statistics about receiving data from workers and merging
/// results.
class CzarStats : std::enable_shared_from_this<CzarStats> {
public:
    using Ptr = std::shared_ptr<CzarStats>;

    CzarStats() = delete;
    CzarStats(CzarStats const&) = delete;
    CzarStats& operator=(CzarStats const&) = delete;

    ~CzarStats() = default;

    /// Setup the global CzarStats instance
    /// @throws Bug if global has already been set or qdispPool is null.
    static void setup(std::shared_ptr<util::QdispPool> const& qdispPool);

    /// Return a pointer to the global CzarStats instance.
    /// @throws Bug if get() is called before setup()
    static Ptr get();

    /// Add a bytes per second entry for query result transmits received over XRootD/SSI
    void addDataRecvRate(double bytesPerSec);

    /// Add a bytes per second entry for result merges
    void addMergeRate(double bytesPerSec);

    /// Add a bytes per second entry for query results read from files
    void addFileReadRate(double bytesPerSec);

    /// Increase the count of requests being setup.
    void startQueryRespConcurrentSetup() { ++_queryRespConcurrentSetup; }
    /// Decrease the count and add the time taken to the histogram.
    void endQueryRespConcurrentSetup(TIMEPOINT start, TIMEPOINT end);

    /// Increase the count of requests waiting.
    void startQueryRespConcurrentWait() { ++_queryRespConcurrentWait; }
    /// Decrease the count and add the time taken to the histogram.
    void endQueryRespConcurrentWait(TIMEPOINT start, TIMEPOINT end);

    /// Increment the total number of queries by 1
    void addQuery() {
        ++_totalQueries;
        ++_numQueries;
    }

    /// Decrement the total number of queries by 1
    void deleteQuery() { --_numQueries; }

    /// Increment the total number of incomplete jobs by 1
    void addJob() {
        ++_totalJobs;
        ++_numJobs;
    }

    /// Decrememnt the total number of incomplete jobs by the specified number
    void deleteJobs(uint64_t num = 1) { _numJobs -= num; }

    /// Increment the total number of the operatons with result files by 1
    void addResultFile() {
        ++_totalResultFiles;
        ++_numResultFiles;
    }

    /// Decrement the total number of the operatons with result files by 1
    void deleteResultFile() { --_numResultFiles; }

    /// Increment the total number of the on-going result merges by 1
    void addResultMerge() {
        ++_totalResultMerges;
        ++_numResultMerges;
    }

    /// Decrement the total number of the on-going result merges by 1
    void deleteResultMerge() { --_numResultMerges; }

    /// Increment the total number of bytes received from workers
    void addTotalBytesRecv(uint64_t bytes) { _totalBytesRecv += bytes; }

    /// Increment the total number of rows received from workers
    void addTotalRowsRecv(uint64_t rows) { _totalRowsRecv += rows; }

    /// Increase the count of requests being processed.
    void startQueryRespConcurrentProcessing() { ++_queryRespConcurrentProcessing; }

    /// Decrease the count and add the time taken to the histogram.
    void endQueryRespConcurrentProcessing(TIMEPOINT start, TIMEPOINT end);

    /// Get a json object describing the current state of the query dispatch thread pool.
    nlohmann::json getQdispStatsJson() const;

    /// Get a json object describing the current transmit/merge stats for this czar.
    nlohmann::json getTransmitStatsJson() const;

    typedef util::RaiiCounter<std::int64_t> RaiiC;
    RaiiC::Ptr getNumTotalUberJobs() { return _numTotalUberJobs; }
    RaiiC::Ptr getNumCreatedUberJobs() { return _numCreatedUberJobs; }
    RaiiC::Ptr getNumSentUberJobs() { return _numSentUberJobs; }
    RaiiC::Ptr getNumCollectingUberJobs() { return _numCollectingUberJobs; }
    RaiiC::Ptr getNumMergingUberJobs() { return _numMergingUberJobs; }
    RaiiC::Ptr getNumDoneUberJobs() { return _numDoneUberJobs; }
    RaiiC::Ptr getNumCancelledUberJobs() { return _numCancelledUberJobs; }

#if 0 //&&&
    class UberJobTotalRaii : public CzarStatsRaii {
    public:
        UberJobTotalRaii(CzarStats::Ptr const& czarStats) : CzarStatsRaii(czarStats, czarStats->_numTotalUberJobs) {}
        virtual ~UberJobTotalRaii() {}
    };

    class UberJobCreatedRaii : public CzarStatsRaii {
    public:
        UberJobCreatedRaii(CzarStats::Ptr const& czarStats) : CzarStatsRaii(czarStats, czarStats->_numCreatedUberJobs) {}
        virtual ~UberJobCreatedRaii() {}
    };

    class UberJobSentRaii : public CzarStatsRaii {
    public:
        UberJobSentRaii(CzarStats::Ptr const& czarStats) : CzarStatsRaii(czarStats, czarStats->_numSentUberJobs) {}
        virtual ~UberJobSentRaii() {}
    };

    class UberJobCollectingRaii : public CzarStatsRaii {
    public:
        UberJobCollectingRaii(CzarStats::Ptr const& czarStats) : CzarStatsRaii(czarStats, czarStats->_numCollectingUberJobs) {}
        virtual ~UberJobCollectingRaii() {}
    };

    class UberJobMergingRaii : public CzarStatsRaii {
    public:
        UberJobMergingRaii(CzarStats::Ptr const& czarStats) : CzarStatsRaii(czarStats, czarStats->_numMergingUberJobs) {}
        virtual ~UberJobMergingRaii() {}
    };

    class UberJobDoneRaii : public CzarStatsRaii {
    public:
        UberJobDoneRaii(CzarStats::Ptr const& czarStats) : CzarStatsRaii(czarStats, czarStats->_numDoneUberJobs) {}
        virtual ~UberJobDoneRaii() {}
    };

    class UberJobCancelledRaii : public CzarStatsRaii {
    public:
        UberJobCancelledRaii(CzarStats::Ptr const& czarStats) : CzarStatsRaii(czarStats, czarStats->_numCancelledUberJobs) {}
        virtual ~UberJobCancelledRaii() {}
    };
#endif //&&&

private:
    CzarStats(std::shared_ptr<util::QdispPool> const& qdispPool);

    static Ptr _globalCzarStats;  ///< Pointer to the global instance.
    static VMUTEX _globalMtx;     ///< Protects `_globalCzarStats`

    /// Connection to get information about the czar's pool of dispatch threads.
    std::shared_ptr<util::QdispPool> _qdispPool;

    /// The start up time (milliseconds since the UNIX EPOCH) of the status collector.
    uint64_t const _startTimeMs = 0;

    /// Histogram for tracking XROOTD/SSI receive rate in bytes per second.
    util::HistogramRolling::Ptr _histDataRecvRate;

    /// Histogram for tracking merge rate in bytes per second.
    util::HistogramRolling::Ptr _histMergeRate;

    /// Histogram for tracking result file read rate in bytes per second.
    util::HistogramRolling::Ptr _histFileReadRate;

    std::atomic<uint64_t> _queryRespConcurrentSetup{0};       ///< Number of request currently being setup
    util::HistogramRolling::Ptr _histRespSetup;               ///< Histogram for setup time
    std::atomic<uint64_t> _queryRespConcurrentWait{0};        ///< Number of requests currently waiting
    util::HistogramRolling::Ptr _histRespWait;                ///< Histogram for wait time
    std::atomic<uint64_t> _queryRespConcurrentProcessing{0};  ///< Number of requests currently processing
    util::HistogramRolling::Ptr _histRespProcessing;          ///< Histogram for processing time

    // Integrated totals (since the start time of Czar)
    std::atomic<uint64_t> _totalQueries{0};       ///< The total number of queries
    std::atomic<uint64_t> _totalJobs{0};          ///< The total number of registered jobs across all queries
    std::atomic<uint64_t> _totalResultFiles{0};   ///< The total number of the result files ever read
    std::atomic<uint64_t> _totalResultMerges{0};  ///< The total number of the results merges ever attempted
    std::atomic<uint64_t> _totalBytesRecv{0};     ///< The total number of bytes received from workers
    std::atomic<uint64_t> _totalRowsRecv{0};      ///< The total number of rows received from workers

    // Running counters
    std::atomic<uint64_t> _numQueries{0};       ///< The current number of queries being processed
    std::atomic<uint64_t> _numJobs{0};          ///< The current number of incomplete jobs across all queries
    std::atomic<uint64_t> _numResultFiles{0};   ///< The current number of the result files being read
    std::atomic<uint64_t> _numResultMerges{0};  ///< The current number of the results being merged

    /// UberJob counts
    /// The total number of existing UberJobs for all queries
    RaiiC::Ptr _numTotalUberJobs = RaiiC::create("numTotalUberJobs");
    /// The current number of UberJobs created but not sent
    RaiiC::Ptr _numCreatedUberJobs = RaiiC::create("numCreatedUberJobs");
    /// The current number of UberJobs sent to workers
    RaiiC::Ptr _numSentUberJobs = RaiiC::create("numSentUberJobs");
    /// The current number of UberJobs that need to collect results
    RaiiC::Ptr _numCollectingUberJobs = RaiiC::create("numCollectingUberJobs");
    /// The current number of UberJobs that are merging results
    RaiiC::Ptr _numMergingUberJobs = RaiiC::create("numMergingUberJobs");
    /// The current number of UberJobs that are done and have results on the czar
    RaiiC::Ptr _numDoneUberJobs = RaiiC::create("numDoneUberJobs");
    /// The current number of UberJobs that have been cancelled
    RaiiC::Ptr _numCancelledUberJobs = RaiiC::create("numCancelledUberJobs");

};

}  // namespace lsst::qserv::qdisp

#endif  // LSST_QSERV_QDISP_CZARSTATS_H
