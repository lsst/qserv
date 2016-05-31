// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#include "wsched/ChunkTaskList.h"

// System headers
#include "global/Bug.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ChunkTaskList");
}

namespace lsst {
namespace qserv {
namespace wsched {


void ChunkTaskList::queTask(wbase::Task::Ptr const& task) {
    std::lock_guard<std::mutex> lg(_mapMx);
    int chunkId = task->getChunkId();
    auto iter = _chunkMap.find(chunkId);
    if (iter == _chunkMap.end()) {
        iter = _insertChunkTask(chunkId);
    }
    ++_taskCount;
    iter->second->queTask(task);
}


/// Insert a new ChunkTask object into the map.
// Precondition: _listMapMx must be locked.
// @return an iterator from the map pointing to the new object.
ChunkTaskList::ChunkMap::iterator ChunkTaskList::_insertChunkTask(int chunkId) {
    std::pair<int, ChunkTasks::Ptr> ele(chunkId, std::make_shared<ChunkTasks>(chunkId, _memMan));
    auto res = _chunkMap.insert(ele); // insert should fail if the key already exists.
    if (!res.second) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkTaskList::_insertChunkTask entry already existed " << chunkId);
    }
    return res.first;
}

/// @return true if this object is ready to provide a Task from its queue.
bool ChunkTaskList::ready(bool useFlexibleLock) {
    std::lock_guard<std::mutex> lock(_mapMx);
    return _ready(useFlexibleLock);
}

/// Precondition: _queueMutex must be locked
//  If true is returned, _readyChunk will point to a chunk with a Task that is ready to run.
//  @return true if this object is ready to provide a Task from its queue. _readyChunk != nullptr.
bool ChunkTaskList::_ready(bool useFlexibleLock) {
    LOGS(_log, LOG_LVL_DEBUG, "&&& _ready");
    if (empty()) {
        return false;
    }

    LOGS(_log, LOG_LVL_DEBUG, "&&& _ready a");
    // If the _activeChunk is invalid, start at the beginning.
    if (_activeChunk == _chunkMap.end()) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready a1");
        _activeChunk = _chunkMap.begin();
        _activeChunk->second->setActive(); // Flag tasks on active so new Tasks added wont be run.
    }

    LOGS(_log, LOG_LVL_DEBUG, "&&& _ready b");
    // Check the active chunk for valid Tasks
    if (_activeChunk->second->ready(useFlexibleLock) == ChunkTasks::ReadyState::READY) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready b1");
        _readyChunk = _activeChunk->second;
        return true;
    }

    LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c");
    // Should the active chunk be advanced?
    if (_activeChunk->second->readyToAdvance()) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c1");
        auto newActive = _activeChunk;
        ++newActive;
        if (newActive == _chunkMap.end()) {
            LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c1a");
            newActive = _chunkMap.begin();
        }

        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c2");
        // Clean up the old _active chunk before moving on.
        _activeChunk->second->setActive(false); // This should move pending Tasks to _activeTasks
        // _inFlightTasks must be empty as readyToAdvance was true.
        if (_activeChunk->second->empty()) {
            LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c2a");
            if (newActive == _activeChunk) {
                LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c2a1");
                newActive = _chunkMap.end();
            }
            _chunkMap.erase(_activeChunk);
        }

        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c3");
        _activeChunk = newActive;
        if (newActive == _chunkMap.end()) {
            LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c3a");
            return false;
        }
        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready c4");
        newActive->second->movePendingToActive();
        newActive->second->setActive();
    }

    LOGS(_log, LOG_LVL_DEBUG, "&&& _ready d");
    // Advance through chunks until READY or NO_RESOURCES found, or until entire list scanned.
    auto iter = _activeChunk;
    ChunkTasks::ReadyState chunkState = iter->second->ready(useFlexibleLock);
    while (chunkState != ChunkTasks::ReadyState::READY
           && chunkState != ChunkTasks::ReadyState::NO_RESOURCES) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready d1");
        ++iter;
        if (iter == _chunkMap.end()) {
            iter = _chunkMap.begin();
        }
        if (iter == _activeChunk) {
            LOGS(_log, LOG_LVL_DEBUG, "&&& _ready d1a");
            return false;
        }
        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready d2");
        chunkState = iter->second->ready(useFlexibleLock);
    }
    LOGS(_log, LOG_LVL_DEBUG, "&&& _ready e");
    if (chunkState == ChunkTasks::ReadyState::NO_RESOURCES) {
        // Advancing past a chunk where there aren't enough resources could cause many
        // scheduling issues.
        LOGS(_log, LOG_LVL_DEBUG, "&&& _ready e1");
        return false;
    }
    LOGS(_log, LOG_LVL_DEBUG, "&&& _ready f");
    _readyChunk = iter->second;
    return true;
}


wbase::Task::Ptr ChunkTaskList::getTask(bool useFlexibleLock) {
    std::lock_guard<std::mutex> lock(_mapMx);
    if (_readyChunk == nullptr) {
        // Attempt to set _readyChunk.
        _ready(useFlexibleLock);
    }
    // If a Task was ready, _readyChunk will not be nullptr.
    if (_readyChunk != nullptr) {
        wbase::Task::Ptr task = _readyChunk->getTask(useFlexibleLock);
        _readyChunk = nullptr;
        --_taskCount;
        return task;
    }
    return nullptr;
}


/// @return true if _activeChunk will point to a different chunk when getTask is called.
// This function is normally used by other classes to determine if it is a reasonable time
// to change priority.
bool ChunkTaskList::nextTaskDifferentChunkId() {
    if (_activeChunk == _chunkMap.end()) {
        return true;
    }
    return _activeChunk->second->readyToAdvance();
}


void ChunkTaskList::taskComplete(wbase::Task::Ptr const& task) {
    auto iter = _chunkMap.find(task->getChunkId());
    if (iter != _chunkMap.end()) {
        iter->second->taskComplete(task);
    }
}


bool ChunkTaskList::setResourceStarved(bool starved) {
    bool ret = _resourceStarved;
    _resourceStarved = starved;
    return ret;
}


void ChunkTasks::SlowTableHeap::push(wbase::Task::Ptr const& task) {
    _tasks.push_back(task);
    std::push_heap(_tasks.begin(), _tasks.end(), compareFunc);
}


wbase::Task::Ptr ChunkTasks::SlowTableHeap::pop() {
    if (_tasks.empty()) {
        return nullptr;
    }
    auto task = _tasks.front();
    std::pop_heap(_tasks.begin(), _tasks.end(), compareFunc);
    _tasks.pop_back();
    return task;
}


/// Queue new Tasks to be run.
/// This relies on ChunkTasks owner for thread safety.
void ChunkTasks::queTask(wbase::Task::Ptr const& a) {
    time(&a->entryTime);
    /// Compute entry time to reduce spurious valgrind errors
    ::ctime_r(&a->entryTime, a->timestr);

    const char* state = "";
    // If this is the active chunk, put new Tasks on the pending list, as
    // we could easily get stuck on this chunk as new Tasks come in.
    if (_active) {
        _pendingTasks.push_back(a);
        state = "PENDING";
    } else {
        _activeTasks.push(a);
        state = "ACTIVE";
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "ChunkTasks enqueue "
         << a->getIdStr()
         << " chunkId=" << _chunkId
         << " state=" << state
         << " active.sz=" << _activeTasks._tasks.size()
         << " pend.sz=" << _pendingTasks.size());
    if (_activeTasks.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: (empty)");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: " << _activeTasks.top()->getIdStr());
    }
}


void ChunkTasks::setActive(bool active) {
    if (_active != active) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks " << _chunkId << " active changed to " << active);
        if (_active && !active) {
            movePendingToActive();
        }
    }
    _active = active;
}


/// Move all pending Tasks to the active heap.
void ChunkTasks::movePendingToActive() {
    LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks " << _chunkId << " moving pending to active &&&"); // delete &&&
    for (auto const& t:_pendingTasks) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks " << _chunkId << " pending->active " << t->getIdStr());
        _activeTasks.push(t);
    }
    _pendingTasks.clear();
}


bool ChunkTasks::empty() const {
    return _activeTasks.empty() && _pendingTasks.empty();
}


/// This is ready to advance when _activeTasks is empty and no Tasks in flight.
bool ChunkTasks::readyToAdvance() {
    return _activeTasks.empty() && _inFlightTasks.empty();
}


/// @Return true if a Task is ready to be run.
// ChunkTasks does not have its own mutex and depends on its owner for thread safety.
// If a Task is ready to be run, _readyTask will not be nullptr.
ChunkTasks::ReadyState ChunkTasks::ready(bool useFlexibleLock) {
    auto logMemManRes =
        [this](bool starved, std::string const& msg, std::vector<memman::TableInfo> const& tblVect) {
            setResourceStarved(starved);
            if (starved) {
                std::string str;
                for (auto const& tblInfo:tblVect) {
                    str += tblInfo.tableName + " ";
                }
                LOGS(_log, LOG_LVL_DEBUG, "ready memMan " << msg << " - " << str);
            }
        };
    LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy a _readyTask=" << _readyTask.get());
    if (_readyTask != nullptr) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy a1");
        return ChunkTasks::ReadyState::READY;
    }
    LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy b");
    if (_activeTasks.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy b1");
        return ChunkTasks::ReadyState::NOT_READY;
    }
    // Calling this function doesn't get expensive until it gets here. Luckily,
    // after this point it will return READY or NO_RESOURCES, and ChunkTaskList::_ready
    // will not examine any further chunks upon seeing those results.
    LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy c");
    auto task = _activeTasks.top();
    if (!task->hasMemHandle()) {
        LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy c1");
        memman::TableInfo::LockType lckOptTbl = memman::TableInfo::LockType::MUSTLOCK;
        memman::TableInfo::LockType lckOptIdx = memman::TableInfo::LockType::NOLOCK;
        if (useFlexibleLock) lckOptTbl = memman::TableInfo::LockType::FLEXIBLE;
        auto scanInfo = task-> getScanInfo();
        auto chunkId = task->getChunkId();
        if (chunkId != _chunkId) {
            // This would slow things down badly, but the system would survive.
            LOGS(_log, LOG_LVL_ERROR, "ChunkTasks " << _chunkId << " got task for chunk " << chunkId
                    << " " << task->getIdStr());
        }
        std::vector<memman::TableInfo> tblVect;
        for (auto const& tbl : scanInfo.infoTables) {
            memman::TableInfo ti(tbl.db + "/" + tbl.table, lckOptTbl, lckOptIdx);
            tblVect.push_back(ti);
        }
        // If tblVect is empty, we should get the empty handle
        memman::MemMan::Handle handle = _memMan->lock(tblVect, chunkId);
        LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy d");
        if (handle == 0) {
            switch (errno) {
            case ENOMEM:
                LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy d1");
                logMemManRes(true, "ENOMEM", tblVect);
                return ChunkTasks::ReadyState::NO_RESOURCES;
            case ENOENT:
                LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy d2");
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock errno=ENOENT chunk not found " << task->getIdStr());
                // Not sure if this is the best course of action, but it should just need one
                // logic path. The query should fail from the missing tables
                // and the czar must be able to handle that with appropriate retries.
                handle = memman::MemMan::HandleType::ISEMPTY;
                break;
            default:
                LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy d3");
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock file system error " << task->getIdStr());
                // Any error reading the file system is probably fatal for the worker.
                throw std::bad_exception();
                return ChunkTasks::ReadyState::NO_RESOURCES;
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy e");
        task->setMemHandle(handle);
        logMemManRes(false, task->getIdStr() + " got handle", tblVect);
    }

    // There is a Task to run at this point, pull it off the heap to avoid confusion.
    LOGS(_log, LOG_LVL_DEBUG, "&&& c_rdy f");
    auto popped = _activeTasks.pop();
    _readyTask = task;
    if (popped != task) {
        // This would be deadly.
        throw Bug(std::string("ChunkTasks::ready popped and task don't match! ") +
                "task=" + task->getIdStr() + " popped=" + popped->getIdStr());
    }
    return ChunkTasks::ReadyState::READY;
}


int ChunkTaskList::getActiveChunkId() {
    if (_activeChunk == _chunkMap.end()) {
        return -1;
    }
    return _activeChunk->second->getChunkId();
}


/// @return old value of _resourceStarved.
bool ChunkTasks::setResourceStarved(bool starved){
    auto val = _resourceStarved;
    _resourceStarved = starved;
    return val;
}



/// @return a Task that is ready to run, if available. Otherwise return nullptr.
// ChunkTasks relies on its owner for thread safety.
wbase::Task::Ptr ChunkTasks::getTask(bool useFlexibleLock) {
    LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks::getTask start &&&"); // &&& delete
    if (ready(useFlexibleLock) != ReadyState::READY) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks " << _chunkId << " denying task");
        return nullptr;
    }
    // Return and clear _readyTask so it isn't called more than once.
    auto task = _readyTask;
    _readyTask = nullptr;
    if (task->getChunkId() == _chunkId) {
        _inFlightTasks.insert(task.get());
    }
    return task;
}


void ChunkTasks::taskComplete(wbase::Task::Ptr const& task) {
    _inFlightTasks.erase(task.get());
}


}}} // namespace lsst::qserv::wsched
