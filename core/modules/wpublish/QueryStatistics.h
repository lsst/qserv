// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 LSST Corporation.
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

#ifndef LSST_QSERV_WSCHED_QUERYSTATISTICS_H
#define LSST_QSERV_WSCHED_QUERYSTATISTICS_H

// System headers

// Qserv headers
#include "wbase/Task.h"

namespace lsst {
namespace qserv {
namespace wpublish {

class Queries;

class QueryStatistics {
public:
    using Ptr = std::shared_ptr<QueryStatistics>;
    explicit QueryStatistics(QueryId const& queryId) : _queryId{_queryId} {}

    void addTask(wbase::Task::Ptr const& task);

    friend class Queries;

private:
    std::mutex _mx;
    QueryId const _queryId;
    std::chrono::system_clock::time_point _touched{std::chrono::system_clock::now()};

    int _tasksCompleted{0};
    int _tasksRunning{0};
    int _tasksBooted{0}; ///< Number of Tasks booted for being too slow.

    double _totalCompletionTime{0.0};

    std::map<int, wbase::Task::Ptr> _taskMap;
};


class ChunkTableStatistics {
public:
    using Ptr = std::shared_ptr<ChunkTableStatistics>;

    static std::string makeTableName(std::string const& db, std::string const& table) {
        return db + ":" + table;
    }

    ChunkTableStatistics(std::string const& name) : _scanTableName{name} {}

    void addTaskFinished(double duration);
private:
    std::mutex _mtx;
    std::string const _scanTableName;

    std::uint64_t _tasksCompleted{0}; ///< Number of Tasks that have completed on this chunk/table.
    std::uint16_t _tasksBooted{0}; ///< Number of Tasks that have been booted for taking too long.

    double _avgCompletionTime{0.0}; ///< weighted average of completion time
    double _weightAvg{99.0}; ///< weight of previous average
    double _weightDur{1.0}; ///< weight of new measurement
    double _weightSum{_weightAvg + _weightDur}; ///< denominator
};



class ChunkTaskStatistics {
public:
    using Ptr = std::shared_ptr<ChunkTaskStatistics>;

    ChunkTaskStatistics(int chunkId) : _chunkId{chunkId} {}

    ChunkTableStatistics::Ptr add(std::string const& scanTableName, double duration);
    ChunkTableStatistics::Ptr getStats(std::string const& scanTableName) const;
private:
    int const _chunkId;
    mutable std::mutex _tStatsMtx; ///< protects _tableStats;
    /// Map of chunk scan table statistics indexed by slowest scan table name in query.
    std::map<std::string, ChunkTableStatistics::Ptr> _tableStats;
};



class Queries {
public:
    using Ptr = std::shared_ptr<Queries>;

    QueryStatistics::Ptr getStats(QueryId const& qId) const;

    void addTask(wbase::Task::Ptr const& task);
    void queuedTask(wbase::Task::Ptr const& task);
    void startedTask(wbase::Task::Ptr const& task);
    void finishedTask(wbase::Task::Ptr const& task);

private:
    void _finishedTaskForChunk(wbase::Task::Ptr const& task, double duration);

    mutable std::mutex _qStatsMtx; ///< protects _queryStats;
    std::map<QueryId, QueryStatistics::Ptr> _queryStats; ///< Map of Query stats indexed by QueryId.

    mutable std::mutex _chunkMtx;
    std::map<int, ChunkTaskStatistics::Ptr> _chunkStats;///< Map of Chunk stats indexed by chunk id.
};


}}} // namespace lsst::qserv::wpublish
#endif // LSST_QSERV_WSCHED_QUERYSTATISTICS_H
