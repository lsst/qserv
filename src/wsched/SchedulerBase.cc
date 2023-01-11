// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
#include "wsched/SchedulerBase.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wsched/BlendScheduler.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ScanScheduler");
}

using namespace std;

namespace lsst::qserv::wsched {

/// Set priority to use when starting next chunk.
void SchedulerBase::setPriority(int priority) { _priority = priority; }

/// Return to default priority for next chunk.
void SchedulerBase::setPriorityDefault() { _priority = _priorityDefault; }

int SchedulerBase::_incrCountForUserQuery(QueryId queryId, int sz) {
    std::lock_guard<std::mutex> lock(_countsMutex);
    _totalTaskCount += sz;
    return _userQueryCounts[queryId] += sz;
}

int SchedulerBase::_decrCountForUserQuery(QueryId queryId) {
    std::lock_guard<std::mutex> lock(_countsMutex);
    // Decrement the count for this user query and remove the entry if count is 0.
    int count = 0;
    --_totalTaskCount;
    auto iter = _userQueryCounts.find(queryId);
    if (iter != _userQueryCounts.end()) {
        count = --(iter->second);
        if (count <= 0) {
            _userQueryCounts.erase(iter);
            LOGS(_log, LOG_LVL_DEBUG, queryId << " uqCount=0, erased");
        }
    }
    return count;
}

int SchedulerBase::getUserQueriesInQ() {
    std::lock_guard<std::mutex> lock(_countsMutex);
    return _userQueryCounts.size();
}

void SchedulerBase::_incrChunkTaskCount(int chunkId) {
    std::lock_guard<std::mutex> lock(_countsMutex);
    ++_chunkTasks[chunkId];
}

void SchedulerBase::_decrChunkTaskCount(int chunkId) {
    // Decrement the count for this user query and remove the entry if count is 0.
    std::lock_guard<std::mutex> lock(_countsMutex);
    auto iter = _chunkTasks.find(chunkId);
    if (iter != _chunkTasks.end()) {
        --(iter->second);
        if (iter->second <= 0) {
            _chunkTasks.erase(iter);
        }
    }
}

int SchedulerBase::getActiveChunkCount() {
    std::lock_guard<std::mutex> lock(_countsMutex);
    return _chunkTasks.size();
}

std::string SchedulerBase::chunkStatusStr() {
    std::ostringstream os;
    std::lock_guard<std::mutex> lock(_countsMutex);
    os << getName() << " q=" << getTotalTaskCount() << " ActChunks=" << _chunkTasks.size() << " ";
    for (auto const& entry : _chunkTasks) {
        int chunkId = entry.first;
        int count = entry.second;
        os << "(" << chunkId << ":" << count << ")";
    }
    return os.str();
}

nlohmann::json SchedulerBase::statusToJson() {
    nlohmann::json status;
    status["name"] = getName();
    status["priority"] = getPriority();

    status["num_tasks_in_queue"] = getSize();
    status["num_tasks_in_flight"] = getInFlight();

    nlohmann::json queryIdToCount = nlohmann::json::array();
    nlohmann::json chunkToNumTasks = nlohmann::json::array();
    {
        std::lock_guard<std::mutex> lock(_countsMutex);
        for (auto&& entry : _userQueryCounts) {
            queryIdToCount.push_back({entry.first, entry.second});
        }
        for (auto&& entry : _chunkTasks) {
            chunkToNumTasks.push_back({entry.first, entry.second});
        }
    }
    status["query_id_to_count"] = queryIdToCount;
    status["chunk_to_num_tasks"] = chunkToNumTasks;
    return status;
}

nlohmann::json SchedulerBase::getJson() const {
    nlohmann::json js = {{"SchedulerName", getName()},          histTimeOfTransmittingTasks->getJson(),
                         histTimeOfRunningTasks->getJson(),     _histQueuedTasks->getJson(),
                         _histRunningTasks->getJson(),          _histTransmittingTasks->getJson(),
                         _histRecentlyCompletedTasks->getJson()};
    return js;
}

void SchedulerBase::setMaxActiveChunks(int maxActive) {
    if (maxActive < 1) maxActive = 1;
    _maxActiveChunks = maxActive;
}

bool SchedulerBase::chunkAlreadyActive(int chunkId) {
    std::lock_guard<std::mutex> lock(_countsMutex);
    auto iter = _chunkTasks.find(chunkId);
    return iter != _chunkTasks.end();  // return true if chunkId was found.
}

void SchedulerBase::recordPerformanceData() {
    _histQueuedTasks->addEntry(getTotalTaskCount());
    _histRunningTasks->addEntry(_inFlight);
    _histTransmittingTasks->addEntry(_transmitCount);
    _histRecentlyCompletedTasks->addEntry(_recentlyCompleted);
    _recentlyCompleted = 0;  // reset to 0 every time it is recorded.
}

}  // namespace lsst::qserv::wsched
