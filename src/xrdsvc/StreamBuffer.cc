// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "xrdsvc/StreamBuffer.h"

// Third-party headers
#include "boost/utility.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wbase/Task.h"
#include "wcontrol/WorkerStats.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.StreamBuffer");
}

using namespace std;

namespace lsst::qserv::xrdsvc {

// Factory function, because this should be able to delete itself when Recycle() is called.
StreamBuffer::Ptr StreamBuffer::createWithMove(std::string &input, std::shared_ptr<wbase::Task> const &task) {
    Ptr ptr(new StreamBuffer(input, task));
    ptr->_selfKeepAlive = ptr;
    return ptr;
}

StreamBuffer::StreamBuffer(std::string &input, wbase::Task::Ptr const &task) : _task(task) {
    _dataStr = std::move(input);
    // TODO: try to make 'data' a const char* in xrootd code.
    // 'data' is not being changed after being passed, so hopefully not an issue.
    //_dataStr will not be used again, but this is ugly.
    data = (char *)(_dataStr.data());
    next = 0;

    auto now = CLOCK::now();
    _createdTime = now;
    _startTime = now;
    _endTime = now;

    _wStats = wcontrol::WorkerStats::get();
    if (_wStats != nullptr) {
        _wStats->startQueryRespConcurrentQueued(_createdTime);
    }
}

void StreamBuffer::startTimer() {
    auto now = CLOCK::now();
    _startTime = now;
    _endTime = now;

    if (_wStats != nullptr) {
        _wStats->endQueryRespConcurrentQueued(_createdTime, _startTime);  // add time to queued time
    }
}

/// xrdssi calls this to recycle the buffer when finished.
void StreamBuffer::Recycle() {
    {
        std::lock_guard<std::mutex> lg(_mtx);
        _doneWithThis = true;
    }
    _cv.notify_all();

    _endTime = CLOCK::now();
    if (_wStats != nullptr) {
        _wStats->endQueryRespConcurrentXrootd(_startTime, _endTime);
    }

    if (_task != nullptr) {
        auto taskSched = _task->getTaskScheduler();
        if (taskSched != nullptr) {
            std::chrono::duration<double> secs = _endTime - _startTime;
            taskSched->histTimeOfTransmittingTasks->addEntry(secs.count());
            LOGS(_log, LOG_LVL_TRACE, "Recycle " << taskSched->histTimeOfTransmittingTasks->getJson());
        } else {
            LOGS(_log, LOG_LVL_WARN, "Recycle transmit taskSched == nullptr");
        }
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Recycle transmit _task == nullptr");
    }
    // Effectively reset _selfKeepAlive, and if nobody else was
    // referencing this, this object will delete itself when
    // this function is done.
    // std::move is used instead of reset() as reset() could
    // result in _keepalive deleting itself while still in use.
    Ptr keepAlive = std::move(_selfKeepAlive);
}

void StreamBuffer::cancel() {
    // Recycle may still need to be called by XrdSsi or there will be a memory
    // leak. XrdSsi calling Recycle is beyond what can be controlled here, but
    // better a possible leak than corrupted memory or a permanently wedged
    // thread in a limited pool.
    // In any case, this code having an effect should be extremely rare.
    // FUTURE: It would be nice to eliminate this possible memory leak.
    //       Possible fix, atomic<bool> _recycleCalled, create thread
    //       to check if _recycleCalled == true. If true or 24 hours pass
    //       use `Ptr keepAlive = std::move(_selfKeepAlive);` to kill the object.
    {
        std::lock_guard<std::mutex> lg(_mtx);
        _doneWithThis = true;
        _cancelled = true;
    }
    _cv.notify_all();
}

// Wait until recycle is called.
bool StreamBuffer::waitForDoneWithThis() {
    std::unique_lock<std::mutex> uLock(_mtx);
    _cv.wait(uLock, [this]() { return _doneWithThis || _cancelled; });
    return !_cancelled;
}

}  // namespace lsst::qserv::xrdsvc
