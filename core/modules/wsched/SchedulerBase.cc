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

namespace lsst {
namespace qserv {
namespace wsched {

/// Set priority to use when starting next chunk.
void SchedulerBase::setPriority(int priority) {
    _priority = priority;
}


/// Return to default priority for next chunk.
void SchedulerBase::setPriorityDefault() {
    _priority = _priorityDefault;
}


int SchedulerBase::_incrCountForUserQuery(QueryId queryId) {
    std::lock_guard<std::mutex> lock(_countsMutex);
    return ++_userQueryCounts[queryId];
}


int SchedulerBase::_decrCountForUserQuery(QueryId queryId) {
    std::lock_guard<std::mutex> lock(_countsMutex);
    // Decrement the count for this user query and remove the entry if count is 0.
    int count = 0;
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
    os << getName() << " ActiveChunks=" << _chunkTasks.size() << " ";
    for (auto const& entry:_chunkTasks) {
        int chunkId = entry.first;
        int count = entry.second;
        os << "(" << chunkId << ":" << count << ")";
    }
    return os.str();
}

nlohmann::json SchedulerBase::statusToJson() {
    std::lock_guard<std::mutex> lock(_countsMutex);
    nlohmann::json status;
    status["name"] = getName();
    status["priority"] = getPriority();
    
    status["num_tasks_in_queue"] = getSize();
    status["num_tasks_in_flight"] = getInFlight();
    nlohmann::json queryIdToCount = nlohmann::json::array();
    for (auto&& entry: _userQueryCounts) {
        queryIdToCount.push_back({entry.first, entry.second});
    }
    status["query_id_to_count"] = queryIdToCount;
    nlohmann::json chunkToNumTasks = nlohmann::json::array();
    for (auto&& entry: _chunkTasks) {
        chunkToNumTasks.push_back({entry.first, entry.second});
    }
    status["chunk_to_num_tasks"] = chunkToNumTasks;
    return status;
}

void SchedulerBase::setMaxActiveChunks(int maxActive) {
    if (maxActive < 1) maxActive = 1;
    _maxActiveChunks = maxActive;
}


bool SchedulerBase::chunkAlreadyActive(int chunkId) {
    std::lock_guard<std::mutex> lock(_countsMutex);
    auto iter = _chunkTasks.find(chunkId);
    return iter != _chunkTasks.end(); // return true if chunkId was found.
}

}}} // namespace lsst::qserv::wsched
